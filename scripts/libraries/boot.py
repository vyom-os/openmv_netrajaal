"""Capture a burst, upload over A7670C, and listen on LoRa via radio_owner_new.

Incoming radio frames are handled only for:
  X -> reply Y
  Y -> record the neighbour
  B, I, E -> image transfer
"""
import time
import utime
import machine
import gc
import struct
import random
import hashlib
import csi
import ubinascii
import enc
import uasyncio as asyncio
from machine import UART, LED, Timer
from config import (
    uid,
    get_machine_id,
    ENCRYPTION_ENABLED,
    uses_hybrid_encryption,
)
from detect import turn_OFF_IR_emitter
from db_store import DbStore
from simcom_a7670 import A7670Driver, UART_ID, BAUDRATE
from radio_owner_new import RadioOwner
from message_codec import decode_x_message, encode_x_message
from clock_utils import get_epoch_sec, get_uptime_sec, set_device_epoch_sec, format_epochms_str
from utils import int_to_nbytes, rssi_encode, encode_snr, get_free_memory, get_uptime_seconds, get_rand, pack_image_meta_header, reboot_device
import logger

UPLOAD_URL = "https://api.vyomiq.io/watchmen-detect/"
BURST_COUNT = 3
BURST_SLEEP_S = 2.5
IS_CC = False

my_addr = None
db_store = None
encnode = None

csi0 = csi.CSI(delays=False)
led = LED("LED_GREEN")
blink = Timer(-1, period=250, callback=lambda t: led.toggle())

MSG_TYPE_BYTE_LEN = 1
NODE_ID_BYTE_LEN = 1
MSG_ID_BYTE_LEN = 3
MSG_UID_LEN = MSG_TYPE_BYTE_LEN + 3 * NODE_ID_BYTE_LEN + MSG_ID_BYTE_LEN
CRC_CHECKSUM_LEN = 4
HEADER_LEN = MSG_UID_LEN + CRC_CHECKSUM_LEN
HEADER_JOINED_LEN = HEADER_LEN + 1

IMG_ID_LEN = 3
IMG_ID_BYTES = 2
CHUNK_ID_BYTES = 2
RADIO_RSSI_BYTES = 1
RADIO_SNR_BYTES = 1
CHUNK_DATA_SIZE = 200
PACKET_PAYLOAD_LIMIT = CHUNK_DATA_SIZE + 15
PACKET_BODY_LIMIT = PACKET_PAYLOAD_LIMIT + 5

ACK_SLEEP = 1
I_BULK_COUNT = 100
MAX_MISS_RETRIES = 30
TRANSMODE_LOCK_TIMEOUT = 600
TRANSMODE_INACTIVITY_LIMIT = 60
WAIT_MESSAGE = b"20"

DATA_BUFFER_SIZE = 120 * 1024
MAX_CHUNK_COUNT = (DATA_BUFFER_SIZE + CHUNK_DATA_SIZE - 1) // CHUNK_DATA_SIZE
CHUNK_BLOCK_SIZE = 1 + CHUNK_DATA_SIZE
CHUNK_STORAGE_SIZE = MAX_CHUNK_COUNT * CHUNK_BLOCK_SIZE

HANDLED_TYPES = ("X", "Y", "B", "I", "E", "D", "A", "W")

radio_owner = None

seen_neighbours = []
ack_msgs_recd = []

trans_in_progress = False
trans_paired_device = None
trans_data_id = None
trans_msg_typ = None
trans_chunks_count = 0
trans_chunk_md5 = None
trans_prev_data_id = None
trans_last_actvity_uptime = None
trans_start_uptime_sec = None
img_chunk_rssi_snr = []
img_last_logged_received = 0

IMAGE_RECOMPILE_BUFFER = None
CHUNK_STORAGE_BUFFER = None


def start_radio():
    global radio_owner
    radio_owner = RadioOwner(logger)
    for msg_typ in ("X", "Y", "B", "E", "D", "A", "W", "I"):
        radio_owner.on_receive(msg_typ, process_message)
    asyncio.create_task(radio_owner.run())
    logger.info(f"[RADIO] listening for {HANDLED_TYPES}")


def encode_node_id(node_id):
    if not isinstance(node_id, int):
        raise TypeError(f"node id must be int, got {type(node_id)}")
    if not 0 <= node_id <= 255:
        raise ValueError(f"node id {node_id} out of range (0-255)")
    return bytes((node_id,))


def encode_dest(dest):
    if dest in (0, 65535):
        return b"*"
    return encode_node_id(dest)


def get_msg_header(msg_typ, creator, dest, msgbytes):
    msg_uid = (
        msg_typ.encode()
        + encode_node_id(creator)
        + encode_node_id(my_addr)
        + encode_dest(dest)
        + get_rand(3).encode()
    )
    crc = ubinascii.crc32(msgbytes) & 0xFFFFFFFF
    return msg_uid, struct.pack(">I", crc)


def _fmt_rssi_snr(rssi, snr):
    rssi_s = "n/a" if rssi is None else "{:.1f} dBm".format(rssi)
    snr_s = "n/a" if snr is None else "{:.1f} dB".format(snr)
    return f"RSSI {rssi_s} (strength), SNR {snr_s} (clarity)"


def _fmt_rssi_snr_compact(rssi, snr):
    rssi_s = "?" if rssi is None else "{:.1f}".format(rssi)
    snr_s = "?" if snr is None else "{:.1f}".format(snr)
    return f"{rssi_s}/{snr_s}"


def _rf_ack_bytes(rssi, snr):
    try:
        radio_rssi = rssi_encode(0 if rssi is None else int(rssi))
    except Exception as e:
        logger.error(f"[LORA] rssi_encode failed: {e}\n{logger.exc_str(e)}")
        radio_rssi = 0
    try:
        radio_snr = encode_snr(0 if snr is None else int(snr))
    except Exception as e:
        logger.error(f"[LORA] encode_snr failed: {e}\n{logger.exc_str(e)}")
        radio_snr = encode_snr(0)
    return (
        int_to_nbytes(radio_rssi, RADIO_RSSI_BYTES)
        + int_to_nbytes(radio_snr, RADIO_SNR_BYTES)
    )


def parse_header(databytes):
    # Input: databytes: bytes; Output: tuple(success, msg_uid, msg_typ, creator, sender, receiver, msg)
    if databytes is None:
        logger.warning("[LORA] Weird that databytes is none")
        return (False, None, None, None, None, None, None)

    if len(databytes) < HEADER_JOINED_LEN:
        logger.error(f"[LORA] databytes too short {len(databytes)} < {HEADER_JOINED_LEN}")
        return (False, None, None, None, None, None, None)
    try:
        if chr(databytes[HEADER_LEN]) != ";":
            logger.error(f"[LORA] error parsing data[HEADER_LEN], recieved: {chr(databytes[HEADER_LEN])}, expected ;")
            return (False, None, None, None, None, None, None)

        msg_uid = databytes[:MSG_UID_LEN]
        msg_typ = chr(msg_uid[0])
        creator = int(msg_uid[1])
        sender = int(msg_uid[2])
        if msg_uid[3] == 42:  # ASCII code for "*" 
            receiver = -1
        else:
            receiver = int(msg_uid[3])

        msgbytes = databytes[HEADER_JOINED_LEN:]
        crc_checksum = databytes[MSG_UID_LEN:HEADER_LEN]
        if len(crc_checksum) != CRC_CHECKSUM_LEN:
            logger.error(f"[RECV] invalid header CRC_CHECKSUM length in {databytes}")
            return (False, None, None, None, None, None, None)
        calc_crc = ubinascii.crc32(msgbytes) & 0xFFFFFFFF
        recv_crc = struct.unpack(">I", crc_checksum)[0]
        if calc_crc != recv_crc:
            logger.error(f"[RECV] payload CRC_CHECKSUM mismatch, dropping packet: {databytes}")
            return (False, None, None, None, None, None, None)
        return (True, msg_uid, msg_typ, creator, sender, receiver, msgbytes)
    except Exception as e:
        logger.error(f"EXCP_ERR: [RECV] error parsing header: {databytes[:HEADER_LEN]} : {e}\n{logger.exc_str(e)}")
        return (False, None, None, None, None, None, None)


async def _send_frame(msg_typ, creator, msgbytes, dest):
    """Send one frame. Returns (ok, msg_uid)."""
    global radio_owner
    try:
        if isinstance(msgbytes, str):
            msgbytes = msgbytes.encode()
        if radio_owner is None or not radio_owner.ready():
            logger.error(f"[LORA] radio not ready, drop {msg_typ} to {dest}")
            return False, None
        if len(msgbytes) > PACKET_BODY_LIMIT:
            logger.error(
                f"[LORA] msgbytes {len(msgbytes)} exceeds body limit {PACKET_BODY_LIMIT}"
            )
            return False, None
        msg_uid, crc_checksum = get_msg_header(msg_typ, creator, dest, msgbytes)
        payload = msg_uid + crc_checksum + b";" + msgbytes
        result = await radio_owner.enqueue(payload).wait()
        if not result or not result[0]:
            err = None if not result else result[1]
            logger.error(f"[LORA] send {msg_typ} to {dest} failed: {err}, MSG_UID = {msg_uid}")
            return False, msg_uid
        if msg_typ != "I":
            logger.info(f"[⮕ SENT {msg_typ} to {dest}] {len(payload)} bytes, MSG_UID = {msg_uid}")
        return True, msg_uid
    except Exception as e:
        logger.error(f"EXCP_ERR: [LORA] send_msg {msg_typ}: {e}\n{logger.exc_str(e)}")
        return False, None


async def send_msg(msg_typ, creator, msgbytes, dest):
    """Send one frame through radio_owner. Returns True when it left the antenna."""
    ok, _msg_uid = await _send_frame(msg_typ, creator, msgbytes, dest)
    return ok


def _ack_missing_chunks(msg_uid):
    """Return (acked, missing_chunk_ids) for a stored ACK of msg_uid."""
    ack_radio_len = RADIO_RSSI_BYTES + RADIO_SNR_BYTES
    for _recd_uid, msgbytes, _t in ack_msgs_recd:
        if len(msgbytes) < MSG_UID_LEN + ack_radio_len:
            continue
        if msgbytes[:MSG_UID_LEN] != msg_uid:
            continue
        missing = []
        colon_pos = MSG_UID_LEN + ack_radio_len
        if len(msgbytes) > colon_pos and msgbytes[colon_pos:colon_pos + 1] == b":":
            payload = msgbytes[colon_pos + 1:]
            if payload:
                if len(payload) % CHUNK_ID_BYTES != 0:
                    logger.error(
                        "[ACK] missing-id payload length %s is not a multiple of %s"
                        % (len(payload), CHUNK_ID_BYTES)
                    )
                    return False, []
                for i in range(0, len(payload), CHUNK_ID_BYTES):
                    missing.append(int.from_bytes(payload[i:i + CHUNK_ID_BYTES], "big"))
        return True, missing
    return False, []


async def send_and_wait_ack(msg_typ, creator, msgbytes, dest, retry_count=3):
    """Send a frame and wait for its ACK. Returns (ok, missing_chunk_ids)."""
    for retry_i in range(retry_count):
        ok, msg_uid = await _send_frame(msg_typ, creator, msgbytes, dest)
        if not ok or msg_uid is None:
            continue
        for i in range(3):
            acked, missing = _ack_missing_chunks(msg_uid)
            if acked:
                return True, missing
            await asyncio.sleep(ACK_SLEEP * min(i + 1, 3))
        logger.warning(
            "[ACK] no ack for %s MSG_UID=%s, retry %s/%s"
            % (msg_typ, msg_uid, retry_i + 1, retry_count)
        )
    return False, []


async def network_response_generate(target):
    await asyncio.sleep(random.uniform(0, 10)) # to not overload the requester node
    try:
        if running_as_cc():
            # If we are the CC, respond with just our address (empty path means we are the destination)
            new_spath_msg = str(my_addr)
            logger.debug(f"[NET] sending network response as CC to :{target}, spth: {new_spath_msg}")
            asyncio.create_task(send_msg("Y", my_addr, new_spath_msg.encode(), target))
            return

        curr_spath = get_curr_spath()
        if curr_spath is None or len(curr_spath) == 0:
            logger.warning(f"[NET] No path to CC available, sending empty spath to {target}")
            new_spath_msg = ""
        else:
            new_spath = [my_addr] + curr_spath
            new_spath_msg = ",".join([str(x) for x in new_spath])
            logger.debug(f"[NET] sending network reponse to :{target}, spth: {new_spath_msg}")
        asyncio.create_task(send_msg("Y", my_addr, new_spath_msg.encode(), target))
    except Exception as e:
        logger.error(f"EXCP_ERR: [NET] error in network response generation: {e}\n{logger.exc_str(e)}")


async def network_response_consume(msg, sender, rssi, snr):
    global seen_neighbours
    epoch_ms = utime.time_ns() // 1_000_000
    neighbour = next((x for x in seen_neighbours if x["node"] == sender), None)
    if neighbour is None:
        neighbour = {"node": sender, "at": epoch_ms, "rssi_list": [], "snr_list": [], "avg_rssi": 0}
        seen_neighbours.append(neighbour)
    if rssi is not None:
        neighbour["rssi_list"] = (neighbour["rssi_list"] + [rssi])[-10:]
    if snr is not None:
        neighbour["snr_list"] = (neighbour["snr_list"] + [snr])[-10:]
    neighbour["at"] = epoch_ms
    if neighbour["rssi_list"]:
        neighbour["avg_rssi"] = sum(neighbour["rssi_list"]) / len(neighbour["rssi_list"])
    try:
        text = msg.decode() if isinstance(msg, (bytes, bytearray)) else str(msg)
    except Exception as e:
        logger.error(f"[NET] Y msg decode failed: {e}\n{logger.exc_str(e)}")
        text = repr(msg)
    logger.info(
        f"[NET] Y from {sender}, avg_rssi={neighbour['avg_rssi']}, path={text} "
        f"({_fmt_rssi_snr(rssi, snr)})"
    )


def init_transfer_buffers():
    global IMAGE_RECOMPILE_BUFFER, CHUNK_STORAGE_BUFFER
    try:
        IMAGE_RECOMPILE_BUFFER = bytearray(DATA_BUFFER_SIZE)
        CHUNK_STORAGE_BUFFER = bytearray(CHUNK_STORAGE_SIZE)
        logger.info(
            f"[MEM] image buffers: recompile {len(IMAGE_RECOMPILE_BUFFER) // 1024}KB, "
            f"chunks {MAX_CHUNK_COUNT} x {CHUNK_BLOCK_SIZE}B"
        )
        return True
    except Exception as e:
        IMAGE_RECOMPILE_BUFFER = None
        CHUNK_STORAGE_BUFFER = None
        logger.error(f"[MEM] image buffer alloc failed: {e}\n{logger.exc_str(e)}")
        return False


def _chunk_block_offset(chunk_id):
    return chunk_id * CHUNK_BLOCK_SIZE


def _chunk_payload_len(chunk_id):
    return CHUNK_STORAGE_BUFFER[_chunk_block_offset(chunk_id)]


def reset_trans_chunk_storage(chunk_count=None):
    if CHUNK_STORAGE_BUFFER is None:
        return
    limit = chunk_count if chunk_count is not None else MAX_CHUNK_COUNT
    if limit > MAX_CHUNK_COUNT:
        limit = MAX_CHUNK_COUNT
    for chunk_id in range(limit):
        CHUNK_STORAGE_BUFFER[_chunk_block_offset(chunk_id)] = 0


def get_transmode_lock(device_id, filedata_id, msg_typ, chunk_count, md5):
    global trans_in_progress, trans_paired_device
    global trans_data_id, trans_msg_typ, trans_chunks_count, trans_chunk_md5
    global img_chunk_rssi_snr, img_last_logged_received, trans_start_uptime_sec
    if CHUNK_STORAGE_BUFFER is None or IMAGE_RECOMPILE_BUFFER is None:
        logger.error("[IMG] transfer buffers not allocated")
        return False
    if trans_in_progress:
        return False
    if chunk_count > MAX_CHUNK_COUNT:
        logger.error(
            f"[MEM] chunk_count {chunk_count} exceeds MAX_CHUNK_COUNT {MAX_CHUNK_COUNT} "
            f"for filedata_id={filedata_id}"
        )
        return False
    trans_in_progress = True
    trans_paired_device = device_id
    trans_data_id = filedata_id
    trans_msg_typ = msg_typ
    trans_start_uptime_sec = get_uptime_sec()
    trans_chunks_count = chunk_count
    trans_chunk_md5 = md5
    img_chunk_rssi_snr = []
    img_last_logged_received = 0
    reset_trans_chunk_storage(chunk_count)
    logger.info(
        f"[IMG] ●●●●●●●●●●❯❯ TRANS MODE started, device:{device_id}, msg_typ:{trans_msg_typ}, filedata_id:{filedata_id} ❮❮●●●●●●●●●●"
    )
    return True


async def keep_transmode_lock(device_id, filedata_id):
    global trans_last_actvity_uptime
    start_uptime = get_uptime_seconds()
    if trans_last_actvity_uptime is None:
        trans_last_actvity_uptime = start_uptime
    try:
        while True:
            await asyncio.sleep(5)
            if not (trans_in_progress and trans_paired_device == device_id and trans_data_id == filedata_id):
                logger.debug(
                    f"[IMG] ○○○○○○○○○○❯❯ TRANS MODE already ended, device:{device_id}, msg_typ:{trans_msg_typ}, filedata_id:{filedata_id} ❮❮○○○○○○○○○○"
                )
                break
            now_uptime = get_uptime_seconds()
            timeout_expired = (now_uptime - start_uptime) >= TRANSMODE_LOCK_TIMEOUT
            inactivity_expired = (now_uptime - trans_last_actvity_uptime) >= TRANSMODE_INACTIVITY_LIMIT
            if timeout_expired or inactivity_expired:
                reason = []
                if timeout_expired:
                    reason.append("MAX_TIMEOUT")
                if inactivity_expired:
                    reason.append("INACTIVITY_TIMEOUT")
                reason_str = "&".join(reason)
                logger.warning(
                    f"[IMG] ●●●●●●●●●●❯❯ TRANS MODE ended, device:{device_id}, msg_typ:{trans_msg_typ}, filedata_id:{filedata_id}, by {reason_str} ❮❮●●●●●●●●●●"
                )
                break
    except Exception as e:
        logger.error(
            f"EXCP_ERR: [IMG] TRANS MODE loop error, device:{device_id}, msg_typ:{trans_msg_typ}, filedata_id:{filedata_id}, error={e}\n{logger.exc_str(e)}"
        )
    finally:
        if trans_in_progress and trans_paired_device == device_id and trans_data_id == filedata_id:
            delete_transmode_lock(device_id, filedata_id)


def check_transmode_lock(device_id, filedata_id):
    global trans_last_actvity_uptime
    if trans_in_progress and trans_paired_device == device_id and trans_data_id == filedata_id:
        trans_last_actvity_uptime = get_uptime_seconds()
        return True
    reasons = []
    if not trans_in_progress:
        reasons.append("trans_in_progress=False")
    if trans_paired_device != device_id:
        reasons.append("paired %s != %s" % (trans_paired_device, device_id))
    if trans_data_id != filedata_id:
        reasons.append("filedata_id %s != %s" % (trans_data_id, filedata_id))
    logger.warning(
        "[IMG] transfer lock miss, want device:%s filedata_id:%s, %s"
        % (device_id, filedata_id, ", ".join(reasons))
    )
    return False


def delete_transmode_lock(device_id, filedata_id, trans_success=False):
    global trans_in_progress, trans_paired_device
    global trans_data_id, trans_msg_typ, trans_chunks_count, trans_chunk_md5, trans_prev_data_id
    global trans_last_actvity_uptime, img_chunk_rssi_snr, img_last_logged_received
    if trans_in_progress and trans_paired_device == device_id and trans_data_id == filedata_id:
        logger.info(
            f"[IMG] ●●●●●●●●●●❯❯ TRANS MODE ended for device:{device_id}, msg_typ:{trans_msg_typ}, filedata_id:{filedata_id}, by logic ❮❮●●●●●●●●●●"
        )
        chunks_to_clear = trans_chunks_count
        trans_in_progress = False
        trans_paired_device = None
        trans_data_id = None
        trans_msg_typ = None
        trans_chunks_count = 0
        trans_chunk_md5 = None
        trans_last_actvity_uptime = None
        img_chunk_rssi_snr = []
        img_last_logged_received = 0
        if trans_success:
            trans_prev_data_id = filedata_id
        if chunks_to_clear:
            reset_trans_chunk_storage(chunks_to_clear)
        gc.collect()
    else:
        logger.debug(
            f"[IMG] ○○○○○○○○○○❯❯ TRANS MODE already ended, for device {device_id} and filedata_id {filedata_id} ❮❮○○○○○○○○○○"
        )


def begin_chunk(msgbytes):
    msg_data = msgbytes.decode()
    parts = msg_data.split(":")
    if len(parts) != 4:
        logger.error(f"[CHUNK] begin message unparsable {msg_data}")
        return (None, None, None, None)
    filedata_id = parts[0]
    file_typ = parts[1]
    numchunks = int(parts[2])
    md5 = parts[3]
    logger.info(f"[IMG RX] Received B packet for filedata_id={filedata_id}, expected_chunks={numchunks}")
    return (filedata_id, file_typ, numchunks, md5)


def get_missing_chunks(filedata_id):
    if filedata_id != trans_data_id or trans_chunks_count == 0:
        return []
    missing = []
    for chunk_id in range(trans_chunks_count):
        if _chunk_payload_len(chunk_id) == 0:
            missing.append(chunk_id)
    return missing


def add_chunk(msgbytes, rssi=None, snr=None):
    global img_chunk_rssi_snr, img_last_logged_received
    if CHUNK_STORAGE_BUFFER is None:
        return
    if len(msgbytes) < IMG_ID_BYTES + CHUNK_ID_BYTES + 1:
        logger.error(f"[CHUNK] not enough bytes {len(msgbytes)}")
        return
    try:
        filedata_id = msgbytes[0:IMG_ID_BYTES + 1].decode()
        chunk_id = int.from_bytes(msgbytes[IMG_ID_BYTES + 1:IMG_ID_BYTES + CHUNK_ID_BYTES + 1], "big")
        chunk_data = msgbytes[IMG_ID_BYTES + CHUNK_ID_BYTES + 1:]
        if filedata_id != trans_data_id:
            logger.error(f"[CHUNK] no storage for filedata_id={filedata_id}, chunk_index={chunk_id}")
            return
        if trans_chunks_count == 0 or chunk_id >= trans_chunks_count:
            logger.error(f"[CHUNK] chunk_id {chunk_id} out of range (expected 0-{trans_chunks_count - 1})")
            return
        chunk_len = len(chunk_data)
        if chunk_len > CHUNK_DATA_SIZE:
            logger.error(f"[CHUNK] chunk_id {chunk_id} payload {chunk_len}B exceeds {CHUNK_DATA_SIZE}B")
            return
        block = _chunk_block_offset(chunk_id)
        CHUNK_STORAGE_BUFFER[block + 1:block + 1 + chunk_len] = chunk_data
        CHUNK_STORAGE_BUFFER[block] = chunk_len
        img_chunk_rssi_snr.append((rssi, snr))
        missing = get_missing_chunks(filedata_id)
        received = trans_chunks_count - len(missing)
        if received != img_last_logged_received and (received % 10 == 0 or received == trans_chunks_count):
            rf_list = ", ".join(_fmt_rssi_snr_compact(r, s) for r, s in img_chunk_rssi_snr)
            logger.info(
                f"[IMG] Received {received}/{trans_chunks_count} chunks (missing={len(missing)}) "
                f"| RSSI/SNR: {rf_list}"
            )
            img_chunk_rssi_snr = []
            img_last_logged_received = received
    except Exception as e:
        logger.error(f"EXCP_ERR: [CHUNK] Error adding chunk: {e}\n{logger.exc_str(e)}")


def get_data_for_chunk_id(chunkiter):
    if trans_chunks_count == 0 or chunkiter >= trans_chunks_count:
        return None
    chunk_len = _chunk_payload_len(chunkiter)
    if chunk_len == 0:
        return None
    block = _chunk_block_offset(chunkiter)
    return memoryview(CHUNK_STORAGE_BUFFER)[block + 1:block + 1 + chunk_len]


def recompile_msg(filedata_id):
    global IMAGE_RECOMPILE_BUFFER
    if trans_chunks_count == 0 or filedata_id != trans_data_id:
        logger.warning(f"[CHUNK] recompile_msg: no chunks for filedata_id={filedata_id}")
        return None
    if get_missing_chunks(filedata_id):
        return None
    if IMAGE_RECOMPILE_BUFFER is None:
        logger.error("[CHUNK] recompile buffer not allocated")
        return None
    try:
        total_size = 0
        chunk_sizes = []
        for chunk_id in range(trans_chunks_count):
            chunk_data = get_data_for_chunk_id(chunk_id)
            if chunk_data is None:
                logger.error(f"[CHUNK] recompile_msg: missing chunk {chunk_id}")
                return None
            chunk_sizes.append(len(chunk_data))
            total_size += len(chunk_data)
        if total_size > len(IMAGE_RECOMPILE_BUFFER):
            logger.error(f"[CHUNK] Image size {total_size} exceeds buffer {len(IMAGE_RECOMPILE_BUFFER)}")
            return None
        offset = 0
        for chunk_id in range(trans_chunks_count):
            chunk_data = get_data_for_chunk_id(chunk_id)
            chunk_size = chunk_sizes[chunk_id]
            IMAGE_RECOMPILE_BUFFER[offset:offset + chunk_size] = chunk_data
            offset += chunk_size
        return memoryview(IMAGE_RECOMPILE_BUFFER)[:total_size]
    except Exception as e:
        logger.error(f"EXCP_ERR: [CHUNK] recompile_msg {filedata_id}: {e}\n{logger.exc_str(e)}")
        return None


def end_chunk(msg):
    parts = msg.split(":")
    if len(parts) != 2:
        logger.error(f"[CHUNK] end message unparsable {msg}")
        return (False, b"", None, None, None)
    filedata_id = parts[0]
    epoch_ms = int(parts[1])
    if filedata_id != trans_prev_data_id and filedata_id != trans_data_id:
        logger.warning(f"[CHUNK] end_chunk: filedata_id={filedata_id} is not the current transfer")
        return (False, b"", filedata_id, None, epoch_ms)
    missing_chunks = get_missing_chunks(filedata_id)
    if missing_chunks:
        logger.info(
            f"[CHUNK] missing {len(missing_chunks)}/{trans_chunks_count} chunks: first 20 = {missing_chunks[:20]}"
        )
        max_missing_bytes = (
            PACKET_PAYLOAD_LIMIT - HEADER_JOINED_LEN - MSG_UID_LEN - RADIO_RSSI_BYTES - RADIO_SNR_BYTES - 1
        )
        missing_bytes = bytearray()
        for idx, chunk_id in enumerate(missing_chunks):
            chunk_bytes = chunk_id.to_bytes(CHUNK_ID_BYTES, "big")
            if len(missing_bytes) + len(chunk_bytes) <= max_missing_bytes:
                missing_bytes.extend(chunk_bytes)
            else:
                logger.warning(
                    f"[CHUNK] missing list truncated at {idx}/{len(missing_chunks)} (payload limit)"
                )
                break
        return (False, bytes(missing_bytes), filedata_id, None, epoch_ms)
    recompiled = recompile_msg(filedata_id)
    if recompiled:
        return (True, b"", filedata_id, recompiled, epoch_ms)
    if filedata_id == trans_prev_data_id:
        logger.info(f"[CHUNK] filedata_id={filedata_id} already completed")
        return (True, b"", filedata_id, None, epoch_ms)
    logger.error(f"[CHUNK] Failed to recompile {filedata_id}")
    return (False, b"", filedata_id, None, epoch_ms)


async def process_message(databytes, rssi=None, snr=None):
    """radio_owner RX callback. Only X, Y, B, I, E, and A are acted on."""
    global trans_chunk_md5, ack_msgs_recd

    success, msg_uid, msg_typ, creator, sender, receiver, msgbytes = parse_header(databytes)
    if not success:
        logger.error(f"[LORA] failure parsing incoming databytes : {databytes}")
        return False

    if msg_typ not in HANDLED_TYPES:
        logger.info(f"[LORA] ignoring msg_typ={msg_typ} from {sender}")
        return False

    if receiver != -1 and my_addr != receiver:
        logger.debug(f"[LORA] skipping message for dst:{receiver}, not me (my_addr:{my_addr}), msg_uid:{msg_uid}")
        return False

    recv_log = "⬇ BCAST" if receiver == -1 else "⬇ RECV"
    rf_log = _fmt_rssi_snr(rssi, snr)
    if msg_typ != "I":
        logger.info(
            f"[{recv_log} from {sender}, {rf_log}] {len(databytes)} bytes, "
            f"msg_typ={msg_typ}, MSG_UID = {msg_uid}"
        )

    ackmessage = msg_uid + _rf_ack_bytes(rssi, snr)

    if msg_typ == "X":
        device_id, device_time_sec = decode_x_message(msgbytes)
        curr_time_sec = get_epoch_sec()
        if device_time_sec and curr_time_sec < device_time_sec:
            if set_device_epoch_sec(device_time_sec):
                logger.info(
                    f"[NET] RTC set from node {device_id}: "
                    f"{format_epochms_str(curr_time_sec * 1000)} -> {format_epochms_str(device_time_sec * 1000)}"
                )
            else:
                logger.error(f"[NET] Failed to set RTC from node {device_id} time {device_time_sec}")
        elif device_time_sec is None:
            logger.warning(f"[NET] X from {sender} has no time, ignored for RTC")
        asyncio.create_task(network_response_generate(sender))

    elif msg_typ == "Y":
        asyncio.create_task(network_response_consume(msgbytes, sender, rssi, snr))

    elif msg_typ == "A":
        ack_msgs_recd.append((msg_uid, msgbytes, utime.ticks_ms()))
        if len(ack_msgs_recd) > 30:
            del ack_msgs_recd[:-30]
        return True

    elif msg_typ == "B":
        try:
            filedata_id, file_typ, numchunks, md5 = begin_chunk(msgbytes)
            if filedata_id is None or numchunks is None:
                logger.error("[CHUNK] Invalid B packet, cannot get filedata_id/numchunks")
                return False
            if check_transmode_lock(sender, filedata_id):
                logger.debug("[CHUNK] Duplicate B packet for same transfer, sending ACK")
                asyncio.create_task(send_msg("A", my_addr, ackmessage, sender))
            elif get_transmode_lock(sender, filedata_id, file_typ, numchunks, md5):
                asyncio.create_task(keep_transmode_lock(sender, filedata_id))
                asyncio.create_task(send_msg("A", my_addr, ackmessage, sender))
            else:
                logger.warning("[CHUNK] TRANS MODE busy or no storage, sending W")
                asyncio.create_task(send_msg("W", my_addr, WAIT_MESSAGE, sender))
                return False
        except Exception as e:
            logger.error(f"EXCP_ERR: [CHUNK] B packet: {e} : {msgbytes}\n{logger.exc_str(e)}")
            return False

    elif msg_typ == "I":
        try:
            if len(msgbytes) > IMG_ID_BYTES:
                filedata_id = msgbytes[0:IMG_ID_BYTES + 1].decode()
                if check_transmode_lock(sender, filedata_id):
                    add_chunk(msgbytes, rssi, snr)
                else:
                    logger.warning(
                        f"[IMG RX] No transmode lock for filedata_id {filedata_id}, skipping chunk"
                    )
            else:
                logger.warning(
                    f"[IMG RX] Chunk I too short ({len(msgbytes)} bytes), cannot extract filedata_id"
                )
        except Exception as e:
            logger.error(f"EXCP_ERR: [IMG RX] Error processing chunk I: {e}\n{logger.exc_str(e)}")

    elif msg_typ == "E":
        logger.info(f"[IMG RX] Free memory before End chunk: {get_free_memory()}KB")
        try:
            transfer_completed, missing_bytes, filedata_id, recompiled_msgbytes, epoch_ms = end_chunk(msgbytes.decode())
        except Exception as e:
            logger.error(f"EXCP_ERR: [IMG RX] Error in end_chunk: {e}\n{logger.exc_str(e)}")
            return False

        if transfer_completed:
            if recompiled_msgbytes:
                try:
                    time_take = get_uptime_sec() - (trans_start_uptime_sec or 0)
                    computed_md5 = ubinascii.hexlify(hashlib.md5(recompiled_msgbytes).digest()).decode()
                    # Only validate when sender sent a non-empty md5 (legacy or chunk-forwarded entries may have no md5)
                    if trans_chunk_md5 and trans_chunk_md5 != computed_md5:
                        logger.error(
                            f"[IMG RX] ✘✘✘✘✘✘ Invalid md5 for the file got transferred, expected={trans_chunk_md5}, computed={computed_md5}, time_take={time_take}s ✘✘✘✘✘"
                        )
                        delete_transmode_lock(sender, filedata_id, False)
                        del recompiled_msgbytes
                        gc.collect()
                        return False
                    else:
                        logger.info(
                            f"[IMG RX] ✔✔✔✔✔✔✔✔ [VALID MD5 FILE] for the file got transferred, time_take={time_take}s ✔✔✔✔✔✔"
                        )
                        ack_done = ackmessage + b":"

                        async def send_ack_multiple(payload):
                            for i in range(2):
                                await send_msg("A", creator, payload, sender)
                                if i == 0:
                                    await asyncio.sleep(ACK_SLEEP)

                        asyncio.create_task(send_ack_multiple(ack_done))
                        delete_transmode_lock(sender, filedata_id, True)
                        if db_store is None:
                            logger.error("[IMG RX] db_store missing, image not queued")
                        else:
                            store_succ, store_err = db_store.store_image(
                                epoch_ms, creator, 0, recompiled_msgbytes
                            )
                            if store_succ:
                                logger.info(
                                    "[IMG RX] queued image creator=%s epoch=%s bytes=%s queued=%s"
                                    % (creator, epoch_ms, len(recompiled_msgbytes), db_store.get_img_queued_count())
                                )
                            else:
                                logger.error("[IMG RX] store_image failed: %s" % store_err)
                        del recompiled_msgbytes
                        gc.collect()
                except Exception as e:
                    logger.error(f"EXCP_ERR: [IMG RX] md5 check failed: {e}\n{logger.exc_str(e)}")
                    del recompiled_msgbytes
                    gc.collect()
                    return False
            else:
                logger.warning("[CHUNK] image not recompiled, already completed earlier")
                asyncio.create_task(send_msg("A", creator, ackmessage + b":", sender))
        else:
            if not missing_bytes:
                delete_transmode_lock(sender, filedata_id, False)
            else:
                asyncio.create_task(send_msg("A", my_addr, ackmessage + b":" + missing_bytes, sender))
    return True


def uid_str():
    try:
        return uid.decode()
    except Exception as e:
        logger.error(f"[MAIN] uid decode failed: {e}\n{logger.exc_str(e)}")
        return str(uid)


def resolve_address():
    """Map this board's UID to an address in config.UID_TO_ADDR."""
    text = uid_str()
    logger.info("[MAIN] UID: %s" % text)
    if my_addr is None:
        logger.error("[MAIN] UID not in config.py, exiting")
        logger.info(text)
        return None
    logger.info("[MAIN] address: %s" % my_addr)
    return my_addr


def setup_camera():
    csi0.reset()
    csi0.pixformat(csi.RGB565)
    csi0.framesize(csi.HD)
    csi0.auto_exposure(False, exposure_us=30000)
    csi0.auto_gain(False, gain_db=20)
    csi0.auto_whitebal(True)


def grab_frames(count=BURST_COUNT):
    """Snapshot JPEGs only. No encrypt/store so first frame stays low-latency."""
    frames = []
    try:
        for i in range(count):
            print("capture")
            img = csi0.snapshot()
            if img is None:
                logger.error("[MAIN] snapshot returned None")
            else:
                epoch_ms = utime.time_ns() // 1_000_000
                # img.save(f"capture{i}.jpg", quality=70)
                frames.append((epoch_ms, bytes(img.compress(quality=70))))
                del img
            if i < count - 1:
                time.sleep(BURST_SLEEP_S)
    except Exception as e:
        logger.error(f"[MAIN] capture failed: {e}\n{logger.exc_str(e)}")
    finally:
        turn_OFF_IR_emitter()
        csi0.sleep(True)
    return frames


def store_frames(machine_id, frames):
    """Add meta header, encrypt, and queue frames captured by grab_frames()."""
    for i, (epoch_ms, jpeg_bytes) in enumerate(frames):
        image_id = get_rand(3)
        payload = pack_image_meta_header(0, 0, epoch_ms, image_id, "F") + jpeg_bytes
        if uses_hybrid_encryption("P"):
            payload = enc.encrypt_hybrid(payload, encnode.get_pub_key())
        logger.info("[MAIN] jpeg_bytes %s bytes image_id=%s" % (len(payload), image_id))
        store_succ, err = db_store.store_image(epoch_ms, machine_id, 0, payload)
        if store_succ:
            logger.info("[MAIN] stored %d/%d queued=%d" % (
                i + 1, len(frames), db_store.get_img_queued_count()))
        else:
            logger.error("[MAIN] store_image failed: %s" % err)
        del payload
    return db_store.get_img_queued_count()


def capture_burst(machine_id, count=BURST_COUNT):
    """Grab frames with 2.5s between them and store each in db_store."""
    return store_frames(machine_id, grab_frames(count))


async def upload_queue(modem):
    """Send images already stored in db_store, one pass."""
    total = db_store.get_img_queued_count()
    n = 0
    while n < total:
        n += 1
        epoch_ms, creator, retry, enc_msgbytes, md5 = db_store.get_next_image_to_send()
        if epoch_ms is None:
            logger.info("[MAIN] queue empty")
            break
        data = ubinascii.b2a_base64(enc_msgbytes).rstrip().decode()
        logger.debug("creator- %s" % creator)
        payload = {
            "machine_id": creator,
            "msg_typ": "F",
            "data": data,
            "epoch_ms": epoch_ms,
            "enc": ENCRYPTION_ENABLED,
        }
        logger.info("[MAIN] upload %d/%d (%s b64 bytes) md5=%s" % (n, total, len(data), md5))
        success, http_code, response = await modem.upload_data(payload, UPLOAD_URL)
        del payload
        if success:
            db_store.update_img_sent_count(1)
            logger.info("[MAIN] upload OK HTTP %s" % http_code)
        else:
            err = str(response) if response else ""
            if len(err) > 160:
                err = err[:160] + "..."
            logger.error("[MAIN] upload FAIL HTTP %s | %s" % (http_code, err))
            store_succ, store_err = db_store.store_image(
                epoch_ms, creator, retry + 1, enc_msgbytes, False
            )
            if not store_succ:
                logger.error("[MAIN] requeue failed: %s" % store_err)
        del enc_msgbytes


def capture_motion(machine_id):
    setup_camera()
    capture_burst(machine_id)


async def make_upload(machine_id):
    modem = None
    try:
        if db_store is None or db_store.get_img_queued_count() == 0:
            logger.info("[MAIN] no images queued")
            return
        logger.info(f"[MAIN] opening UART{UART_ID} @ {BAUDRATE} (TX=P4, RX=P5)")
        uart = UART(UART_ID, BAUDRATE, timeout=2000, rxbuf=4096)
        time.sleep_ms(200)
        modem = A7670Driver(uart=uart)

        logger.info("[MAIN] waking GSM...")
        await modem.wake()
        if not await modem.connect(settle_sec=0):
            logger.info("[MAIN] GSM connect failed")
            return

        await upload_queue(modem)
    finally:
        if modem is not None:
            logger.info("[MAIN] modem sleep")
            await modem.sleep()


def make_chunks(msgbytes):
    chunks = []
    while len(msgbytes) > CHUNK_DATA_SIZE:
        chunks.append(msgbytes[:CHUNK_DATA_SIZE])
        msgbytes = msgbytes[CHUNK_DATA_SIZE:]
    if msgbytes:
        chunks.append(msgbytes)
    return chunks


async def _flush_i_batch(batch):
    """Send one bulk of I frames. Returns False when the radio is not ready."""
    if not batch:
        return True
    if radio_owner is None or not radio_owner.ready():
        logger.error("[LORA] radio not ready, drop I bulk of %s" % len(batch))
        return False
    n = len(batch)
    result = await radio_owner.enqueue(batch).wait()
    rows = result if isinstance(result, list) else []
    ok_n = sum(1 for ok, _err in rows if ok)
    logger.info("⮕ I bulk queued {} ok {} failed {}".format(n, ok_n, n - ok_n))
    return True


async def _send_image_chunks(creator, dest, filedata_id, chunks, chunk_ids):
    """Send the listed chunk indexes to dest, 100 I frames per enqueue."""
    total = len(chunk_ids)
    batch = []
    for idx, chunk_id in enumerate(chunk_ids):
        if not check_transmode_lock(dest, filedata_id):
            logger.error("[MAIN] radio image aborted, transfer lock lost")
            return False
        if chunk_id < 0 or chunk_id >= len(chunks):
            logger.warning("[MAIN] radio image skip invalid chunk %s" % chunk_id)
            continue
        if idx % I_BULK_COUNT == 0:
            logger.info(
                "[MAIN] ⮕ sending chunks to %s, (%s-%s)/%s"
                % (dest, idx, min(idx + I_BULK_COUNT, total), total)
            )
        chunkbytes = filedata_id.encode() + chunk_id.to_bytes(CHUNK_ID_BYTES, "big") + chunks[chunk_id]
        if len(chunkbytes) > PACKET_BODY_LIMIT:
            logger.error(
                "[LORA] I chunk %s body %s exceeds %s" % (chunk_id, len(chunkbytes), PACKET_BODY_LIMIT)
            )
            continue
        msg_uid, crc_checksum = get_msg_header("I", creator, dest, chunkbytes)
        batch.append(msg_uid + crc_checksum + b";" + chunkbytes)
        if len(batch) >= I_BULK_COUNT:
            if not await _flush_i_batch(batch):
                return False
            batch = []
    return await _flush_i_batch(batch)


async def send_img_via_radio():
    """Send one queued image as B/I/E frames to the nearest neighbour."""
    dest = next_device_in_spath()
    if dest is None:
        logger.warning("[MAIN] no neighbour to send image to")
        return False

    epoch_ms, creator, retry, img_bytes, md5 = db_store.get_next_image_to_send()
    if epoch_ms is None:
        logger.info("[MAIN] no image to send via radio")
        return False

    filedata_id = get_rand(IMG_ID_LEN)
    chunks = make_chunks(img_bytes)
    logger.info(
        "[MAIN] radio image to %s, filedata_id=%s, %s bytes, %s chunks"
        % (dest, filedata_id, len(img_bytes), len(chunks))
    )
    ok = False
    try:
        if not get_transmode_lock(dest, filedata_id, "P", 0, md5 or ""):
            logger.warning("[MAIN] radio image skipped, transfer already in progress")
            return False
        asyncio.create_task(keep_transmode_lock(dest, filedata_id))
        begin = "%s:P:%s:%s" % (filedata_id, len(chunks), md5 or "")
        begin_ok, _missing = await send_and_wait_ack("B", creator, begin, dest)
        if not begin_ok:
            logger.error("[MAIN] radio image begin failed")
            return False
        if not await _send_image_chunks(
            creator, dest, filedata_id, chunks, range(len(chunks))
        ):
            return False
        end = "%s:%s" % (filedata_id, epoch_ms)
        for retry_i in range(MAX_MISS_RETRIES):
            end_ok, missing_chunks = await send_and_wait_ack(
                "E", creator, end, dest, retry_count=5
            )
            if not end_ok:
                logger.error("[MAIN] radio image end failed")
                return False
            if (
                not missing_chunks
                or (len(missing_chunks) == 1 and missing_chunks[0] == -1)
            ):
                ok = True
                db_store.update_img_sent_count(1)
                time_take = get_uptime_sec() - (trans_start_uptime_sec or 0)
                logger.info(
                    "[MAIN] ✔✔✔✔✔✔ radio image sent epoch=%s to %s in %ss"
                    % (epoch_ms, dest, time_take)
                )
                return True
            logger.info(
                "[CHUNK] receiver still missing %s chunks after retry %s: %s"
                % (len(missing_chunks), retry_i, missing_chunks)
            )
            if not await _send_image_chunks(creator, dest, filedata_id, chunks, missing_chunks):
                return False
        logger.error("[MAIN] radio image failed, max retries reached-%s" % MAX_MISS_RETRIES)
        return False
    except Exception as e:
        logger.error(f"[MAIN] radio image send failed: {e}\n{logger.exc_str(e)}")
        return False
    finally:
        if trans_in_progress and trans_data_id == filedata_id:
            delete_transmode_lock(dest, filedata_id, ok)
        if not ok:
            store_succ, store_err = db_store.store_image(
                epoch_ms, creator, retry + 1, img_bytes, False
            )
            if not store_succ:
                logger.error("[MAIN] radio image requeue failed: %s" % store_err)
        del img_bytes

def running_as_cc():
    return IS_CC

def get_curr_spath():
    """ Returns the shortest path. If several share that length, the one with the highest avg_rssi. """
    global network_paths
    if running_as_cc():
        return [my_addr]
    else:
        return None

def next_device_in_spath():
    """Returns the node with the greatest avg_rssi, or None if no neighbours."""
    if not seen_neighbours:
        return None
    return max(seen_neighbours, key=lambda n: n["avg_rssi"])["node"]

async def neighbour_search_function():
    """Broadcast X ten times, then wait so every neighbour can reply with Y."""
    search_count = 3
    for i in range(search_count):
        scanmsg = encode_x_message(my_addr, get_epoch_sec())
        if scanmsg:
            await send_msg("X", my_addr, scanmsg, 65535)
            logger.debug("[NET] neighbour search X %d/%d" % (i + 1, search_count))
        else:
            logger.error("[NET] failed to encode X scan message")
        await asyncio.sleep(2)
    await asyncio.sleep(11)
    logger.info("[NET] neighbour search done, seen %d" % len(seen_neighbours))




# Set CASE to run one path:
# 1: PIR wakeup, capture_motion, make_upload
# 2: radio wakeup, receive an image, make_upload
# 3: PIR wakeup, capture_motion, send_img_via_radio three times
CASE = 1

async def main():
    global my_addr
    global db_store
    global encnode
    global IS_CC

    frames = None
    if CASE == 1:
        IS_CC = True
        logger.info("[MAIN] case 1: PIR wakeup, capture, upload")
        setup_camera()
        frames = grab_frames()

    my_addr = get_machine_id()
    if my_addr is None:
        logger.error("[MAIN] machine id not found")
        return
    encnode = enc.EncNode(my_addr)

    process_id = get_rand(3)
    try:
        db_store = DbStore(process_id, my_addr)
        logger.info("[MAIN] DbStore ready process=%s" % process_id)
    except Exception as e:
        logger.error(f"[MAIN] DbStore init failed: {e}\n{logger.exc_str(e)}")
        return

    if CASE == 1:
        store_frames(my_addr, frames)
        await make_upload(my_addr)
        logger.info("꩜꩜꩜꩜꩜꩜ main loop completed * ꩜꩜꩜꩜꩜꩜")
        logger.info("[MAIN] case 1 done, entering deep sleep")
        return
    elif CASE == 2:
        IS_CC = True
        logger.info("[MAIN] case 2: radio wakeup, receive image, upload")
        init_transfer_buffers()
        start_radio()

        wait_time = 400
        start_uptime = get_uptime_sec()
        logger.info(f"[MAIN] radio stays up for {wait_time} seconds or while a transfer is in progress")
        while get_uptime_sec() - start_uptime < wait_time or trans_in_progress:
            await make_upload(my_addr)
            await asyncio.sleep(5)
        await asyncio.sleep(5)  # let the image combine complete
        logger.info("[MAIN] making upload")
        await make_upload(my_addr)
        logger.info("[MAIN] task completed, exiting...")
    elif CASE == 3:
        logger.info("[MAIN] case 3: PIR wakeup, capture, send images via radio")
        init_transfer_buffers()
        start_radio()

        logger.info("[MAIN] keep capturing and sending images via radio")
        capture_motion(my_addr)
        await neighbour_search_function()
        while db_store.get_img_queued_count() > 0:
            logger.info("[MAIN] send_img_via_radio, remaining %d" % db_store.get_img_queued_count())
            success = await send_img_via_radio()
            if not success:
                logger.error("[MAIN] send_img_via_radio failed")
                await asyncio.sleep(10)

        remaing_images = db_store.get_img_queued_count()
        if remaing_images > 0:
            logger.info("[MAIN] remaining %d images in queue" % remaing_images)
        else:
            logger.info("[MAIN] no remaining images in queue")
        logger.info("[MAIN] task completed, exiting...")
    else:
        logger.error("[MAIN] unknown CASE %s" % CASE)

    logger.info("꩜꩜꩜꩜꩜꩜ main loop completed * ꩜꩜꩜꩜꩜꩜")
    reboot_device()

try:
    asyncio.run(main())
except Exception as e:
    logger.fatal(f"[MAIN] FATAL: {e}\n{logger.exc_str(e)}")
finally:
    blink.deinit()
    led.off()
    logger.info("[MAIN] deepsleeping the machine...")
    machine.deepsleep()
