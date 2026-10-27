"""
radio_owner.py — sole owner of the SX1262.

Public API:
  run()                 init, listen, enqueue received frames
  reset(reason="")      hardware reset and re-init (30s cooldown)
  send(data)            transmit and wait until TX_DONE or timeout; None or error str
  get_recv_msg_count()  frames waiting
  get_recv_msg()        pop the oldest frame: (data, status, rssi, snr)

Protocol stats (ACK success, header CRC) stay on this object so the health
monitor and heartbeat can read them. The app updates:
  radio_sent_succ_count, radio_sent_fail_count,
  radio_recd_succ_count, radio_recd_err_count, radio_recd_hasherr_count
"""

import machine
import utime
import uasyncio as asyncio
from supervisor import supervised
from sx1262 import SX1262
from _sx126x import (
    ERR_NONE,
    ERR_CRC_MISMATCH,
    SX126X_IRQ_CRC_ERR,
    SX126X_IRQ_HEADER_ERR,
    SX126X_IRQ_RX_DONE,
    SX126X_IRQ_TIMEOUT,
    SX126X_IRQ_TX_DONE,
    SX126X_IRQ_ALL,
)


# SF7 / 125 kHz. Same modem settings on every device.
LORA_FREQ = 868.0
LORA_BW = 125
LORA_SF = 7
LORA_CR = 6
LORA_POWER = 22
LORA_PREAMBLE = 10
LORA_SYNC_WORD = 0x2B

RX_QUEUE_MAX = 10
LORA_RESET_INTERVAL_MS = 30 * 1000
LORA_CONTINUOUS_SENT_ERROR_LIMIT = 5
TX_DONE_FAIL_LIMIT = 10
TX_DONE_WAIT_MS = 2000
RADIO_HEALTH_INTERVAL_SEC = 120


def _fmt_rssi_snr(rssi, snr):
    rssi_s = "n/a" if rssi is None else "{:.1f} dBm".format(rssi)
    snr_s = "n/a" if snr is None else "{:.1f} dB".format(snr)
    return "RSSI {} (strength), SNR {} (clarity)".format(rssi_s, snr_s)


class RadioOwner:

    def __init__(self, logger=None, my_addr=None):
        self.logger = logger
        self.my_addr = my_addr
        self.loranode = None
        self.ready = False

        self._init_in_progress = False
        self._init_count = 0
        self._last_recover_ms = 0
        self._health_started = False

        self._phy_fail_count = 0
        self._tx_done_fail = 0
        self._tx_done = False

        self.radio_sent_succ_count = 0
        self.radio_sent_fail_count = 0
        self.radio_recd_succ_count = 0
        self.radio_recd_err_count = 0
        self.radio_recd_crcerr_count = 0
        self.radio_recd_hasherr_count = 0
        self.radio_recd_skip_count = 0
        self._succ_prev = 0
        self._fail_prev = 0

        self._rx_queue = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self):
        """Init the SX1262 and keep enqueueing received frames.

        If a frame arrives while 10 are already queued, the oldest is dropped.
        Also starts the health monitor when an asyncio loop is running.
        """
        self._init()
        if self._health_started:
            return
        self._health_started = True
        try:
            asyncio.create_task(supervised(
                "lora_health_monitor", self._health_monitor, critical=True
            ))
        except Exception as e:
            self._health_started = False
            self.logger.warning("[LORA] health monitor not started: {}".format(e))

    def reset(self, reason="reset"):
        """Hardware-reset and re-init. Cooldown 30s. Returns True on success."""
        return self._recover(reason)

    def send(self, data):
        """Transmit one frame. Wait until it leaves the antenna or the wait expires.

        Returns None on success, or an error string.
        """
        if data is None or len(data) > 254:
            return "[LORA] msg too large : {}".format(0 if data is None else len(data))
        if not self.ready:
            if self.loranode is None and not self._init_in_progress:
                self._init()
            if not self.ready:
                return "[LORA] radio not ready"

        self._tx_done = False
        try:
            _nbytes, status = self.loranode.send(data)
        except Exception as e:
            return "[LORA] Send exception: {}".format(e)

        if status != ERR_NONE:
            self._phy_fail_count += 1
            if self._phy_fail_count >= LORA_CONTINUOUS_SENT_ERROR_LIMIT:
                self._request_reset(
                    "PHY send failed {} times in a row, status: {}".format(
                        self._phy_fail_count, status
                    )
                )
                self._phy_fail_count = 0
            return "[LORA] Send failed with status: {}".format(status)

        self._phy_fail_count = 0
        deadline = utime.ticks_add(utime.ticks_ms(), TX_DONE_WAIT_MS)
        while not self._tx_done:
            if utime.ticks_diff(utime.ticks_ms(), deadline) >= 0:
                self._tx_done_fail += 1
                if self._tx_done_fail >= TX_DONE_FAIL_LIMIT:
                    self._request_reset(
                        "TX_DONE failed {} times in a row".format(self._tx_done_fail)
                    )
                    self._tx_done_fail = 0
                return "[LORA] TX_DONE timeout"
            utime.sleep_ms(5)
        self._tx_done_fail = 0
        return None

    def get_recv_msg_count(self):
        irq = machine.disable_irq()
        try:
            return len(self._rx_queue)
        finally:
            machine.enable_irq(irq)

    def get_img_queued_count(self):
        """Same count as get_recv_msg_count. Name used by the standalone test."""
        return self.get_recv_msg_count()

    def get_recv_msg(self):
        """Pop the oldest queued frame. Returns (data, status, rssi, snr) or None."""
        irq = machine.disable_irq()
        try:
            if not self._rx_queue:
                return None
            return self._rx_queue.pop(0)
        finally:
            machine.enable_irq(irq)

    # ------------------------------------------------------------------
    # Init / reset
    # ------------------------------------------------------------------

    def _init(self):
        if self._init_in_progress:
            self.logger.info("[LORA] Initialization already in progress, skipping duplicate call")
            return False
        self._init_in_progress = True
        try:
            self._init_count += 1
            self.logger.info(
                "[LORA] Initializing LoRa SX1262 module... my lora addr = {}".format(self.my_addr)
            )
            if self.loranode is not None:
                try:
                    self.loranode.clearDio1Action()
                except Exception as e:
                    self.logger.error(
                        "EXCP_ERR: [LORA] Error clearing DIO1 action: {}\n{}".format(
                            e, self.logger.exc_str(e)
                        )
                    )
            else:
                self.loranode = SX1262(
                    spi_bus=1,
                    clk='P2',
                    mosi='P0',
                    miso='P1',
                    cs='P3',
                    irq='P13',
                    rst='P6',
                    gpio='P7',
                    txen='P9',
                    rxen='P10',
                    spi_baudrate=2000000,
                    spi_polarity=0,
                    spi_phase=0,
                )

            status = self.loranode.begin(
                freq=LORA_FREQ,
                bw=LORA_BW,
                sf=LORA_SF,
                cr=LORA_CR,
                syncWord=LORA_SYNC_WORD,
                power=LORA_POWER,
                currentLimit=140.0,
                preambleLength=LORA_PREAMBLE,
                implicit=False,
                crcOn=True,
                tcxoVoltage=1.6,
                useRegulatorLDO=False,
                blocking=False,
            )
            if status != ERR_NONE:
                self.ready = False
                self.logger.error("[LORA] Failed to initialize SX1262, status: {}".format(status))
                try:
                    self.loranode.clearDio1Action()
                except Exception as e:
                    self.logger.error(
                        "EXCP_ERR: [LORA] Error clearing DIO1 action: {}\n{}".format(
                            e, self.logger.exc_str(e)
                        )
                    )
                return False

            self.loranode.setBlockingCallback(blocking=False, callback=self._on_lora_event)
            self.ready = True
            self.logger.info("[LORA] LoRa SX1262 initialized successfully")
            return True
        except Exception as e:
            self.ready = False
            self.logger.error(
                "EXCP_ERR: [LORA] Exception during initialization: {}\n{}".format(
                    e, self.logger.exc_str(e)
                )
            )
            if self.loranode is not None:
                try:
                    self.loranode.clearDio1Action()
                except Exception as e2:
                    self.logger.error(
                        "EXCP_ERR: [LORA] Error clearing DIO1 action: {}\n{}".format(
                            e2, self.logger.exc_str(e2)
                        )
                    )
            return False
        finally:
            self._init_in_progress = False

    def _hardware_reset(self, verify=True):
        """Pulse NRESET (P6). Returns True on ERR_NONE."""
        if self.loranode is None:
            self.logger.error("[LORA] Cannot hardware-reset: loranode is None (init first)")
            return False
        state = self.loranode.reset(verify=verify)
        if state != ERR_NONE:
            self.logger.error("[LORA] Hardware reset (NRESET/P6) failed, status: {}".format(state))
            return False
        self.ready = False
        self.logger.info("[LORA] Hardware reset (NRESET/P6) complete — re-init required")
        return True

    def _recover(self, reason):
        try:
            if self._init_in_progress:
                return False
            now = utime.ticks_ms()
            if self._last_recover_ms and utime.ticks_diff(now, self._last_recover_ms) < LORA_RESET_INTERVAL_MS:
                self.logger.warning(
                    "[LORA] Reset already attempted within last {} seconds, skipping reset...".format(
                        LORA_RESET_INTERVAL_MS // 1000
                    )
                )
                return False
            self._last_recover_ms = now
            self.logger.fatal("[LORA] recovering radio: {}".format(reason))
            self._hardware_reset()
            succ = self._init()
            if succ:
                self._tx_done_fail = 0
                self._snapshot_health_window()
            return succ
        except Exception as e:
            self.logger.error(
                "EXCP_ERR: [LORA] Error recovering LoRa: {}\n{}".format(e, self.logger.exc_str(e))
            )
            return False

    def _request_reset(self, reason):
        try:
            asyncio.create_task(self._recover_later(reason))
        except Exception as e:
            self.logger.error("[LORA] could not schedule reset ({}): {}".format(reason, e))
            self.reset(reason)

    async def _recover_later(self, reason):
        self.reset(reason)

    def _snapshot_health_window(self):
        self._succ_prev = self.radio_sent_succ_count + self.radio_recd_succ_count
        self._fail_prev = (
            self.radio_sent_fail_count
            + self.radio_recd_err_count
            + self.radio_recd_hasherr_count
        )

    # ------------------------------------------------------------------
    # IRQ
    # ------------------------------------------------------------------

    def _enqueue_rx(self, msg, status, rssi, snr):
        dropped = False
        irq = machine.disable_irq()
        try:
            if len(self._rx_queue) >= RX_QUEUE_MAX:
                self._rx_queue.pop(0)
                self.radio_recd_skip_count += 1
                dropped = True
            self._rx_queue.append((msg, status, rssi, snr))
        finally:
            machine.enable_irq(irq)
        if dropped:
            self.logger.warning("[LORA] RX queue full ({}), dropped oldest packet".format(RX_QUEUE_MAX))

    def _on_lora_event(self, events):
        """DIO1 callback. Enqueue a good frame; re-arm RX on faults."""
        if events & (SX126X_IRQ_CRC_ERR | SX126X_IRQ_HEADER_ERR):
            self.radio_recd_crcerr_count += 1
            try:
                self.loranode.clearIrqStatus(
                    SX126X_IRQ_CRC_ERR | SX126X_IRQ_HEADER_ERR | SX126X_IRQ_RX_DONE
                )
                self.loranode.startReceive()
            except Exception as e:
                self.logger.error(
                    "EXCP_ERR: [LORA] Error clearing IRQ status: {}\n{}".format(
                        e, self.logger.exc_str(e)
                    )
                )
            return

        if events & SX126X_IRQ_RX_DONE:
            try:
                msg, status = self.loranode.recv(len=0)
                rssi = self.loranode.getLastRSSI()
                snr = self.loranode.getLastSNR()
                if status == ERR_NONE:
                    if msg and len(msg) > 0:
                        self._enqueue_rx(msg, status, rssi, snr)
                elif status == ERR_CRC_MISMATCH:
                    self.radio_recd_crcerr_count += 1
                    self.logger.fatal(
                        "[LORA] CRC error on received packet, dropped this packet ({})".format(
                            _fmt_rssi_snr(rssi, snr)
                        )
                    )
                else:
                    self.radio_recd_err_count += 1
                    self.logger.fatal("[LORA] Receive error status: {}, dropped this packet".format(status))
            except Exception as e:
                self.radio_recd_err_count += 1
                self.logger.error(
                    "EXCP_ERR: [LORA] Error reading packet in interrupt callback: {}\n{}".format(
                        e, self.logger.exc_str(e)
                    )
                )
                try:
                    self.loranode.clearIrqStatus(SX126X_IRQ_ALL)
                    self.loranode.startReceive()
                except Exception as e2:
                    self.logger.error(
                        "EXCP_ERR: [LORA] Error clearing interrupt status: {}\n{}".format(
                            e2, self.logger.exc_str(e2)
                        )
                    )
            return

        if events & SX126X_IRQ_TIMEOUT:
            self.logger.warning("[LORA] RX/TX timeout IRQ, restarting receive")
            try:
                self.loranode.clearIrqStatus(SX126X_IRQ_TIMEOUT)
                self.loranode.startReceive()
            except Exception as e:
                self.logger.error(
                    "EXCP_ERR: [LORA] Error handling timeout in interrupt callback: {}\n{}".format(
                        e, self.logger.exc_str(e)
                    )
                )
                try:
                    self.loranode.clearIrqStatus(SX126X_IRQ_ALL)
                    self.loranode.startReceive()
                except Exception as e2:
                    self.logger.error(
                        "EXCP_ERR: [LORA] Error clearing interrupt status: {}\n{}".format(
                            e2, self.logger.exc_str(e2)
                        )
                    )
            return

        if events & SX126X_IRQ_TX_DONE:
            self._tx_done = True
            self._tx_done_fail = 0
            return

        self.logger.error("[LORA] Unknown interrupt event: {}, resetting status, receive mode...".format(events))
        try:
            self.loranode.clearIrqStatus(SX126X_IRQ_ALL)
            self.loranode.startReceive()
        except Exception as e:
            self.logger.error(
                "EXCP_ERR: [LORA] Error clearing interrupt status: {}\n{}".format(
                    e, self.logger.exc_str(e)
                )
            )

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def _health_monitor(self):
        while True:
            try:
                if self._init_in_progress:
                    await asyncio.sleep(RADIO_HEALTH_INTERVAL_SEC)
                    continue

                if self.loranode is None:
                    self.logger.info("[LORA] LoRa not initialized, initializing...")
                    succ = self._init()
                    if not succ:
                        self.logger.error("[LORA] Failed to initialize LoRa, retrying in 60 seconds")
                    else:
                        self._snapshot_health_window()
                        self.logger.info("[LORA] LoRa initialized successfully")
                    await asyncio.sleep(RADIO_HEALTH_INTERVAL_SEC)
                    continue

                succ_count = self.radio_sent_succ_count + self.radio_recd_succ_count
                fail_count = (
                    self.radio_sent_fail_count
                    + self.radio_recd_err_count
                    + self.radio_recd_hasherr_count
                )
                succ_diff = succ_count - self._succ_prev
                fail_diff = fail_count - self._fail_prev
                total_msg = max(succ_diff + fail_diff, 1)
                error_percentage = int(max(fail_diff, 0) * 100 / total_msg)

                if total_msg >= 20 and error_percentage >= 50:
                    await self._recover_and_log("error pct: {}%".format(error_percentage))
                elif total_msg >= 10 and error_percentage >= 80:
                    await self._recover_and_log("error pct: {}%".format(error_percentage))
                elif total_msg >= 10:
                    self.logger.info("Radio is working fine")
                else:
                    self.logger.debug("less radio steps available, will re-analyse later.")
                await asyncio.sleep(RADIO_HEALTH_INTERVAL_SEC)
            except Exception as e:
                self.logger.error(
                    "EXCP_ERR: [LORA] Error in health monitor: {}\n{}".format(
                        e, self.logger.exc_str(e)
                    )
                )
                self.reset("health monitor exception: {}".format(e))
                await asyncio.sleep(RADIO_HEALTH_INTERVAL_SEC)

    async def _recover_and_log(self, reason):
        succ = self.reset(reason)
        if not succ:
            self.logger.error("[LORA] Failed to initialize LoRa, retrying in 60 seconds")
        else:
            self.logger.info("[LORA] LoRa initialized successfully")






# -----------------------------------▼▼▼▼▼-----------------------------------
# ------------------------------- TESTING CODE ------------------------------
# ---------------------------------------------------------------------------
def _recv_log_line(databytes, rssi, snr):
    """Same receive line as main_prod.process_message: ⬇ RECV or ⬇ BCAST."""
    n = 0 if databytes is None else len(databytes)
    recv_log = "⬇ RECV"
    sender = "?"
    msg_uid = "?"
    if databytes is not None and n >= 4:
        msg_uid = databytes[:7] if n >= 7 else databytes
        sender = int(databytes[2])
        if databytes[3] == 42:  # "*"
            recv_log = "⬇ BCAST"
    masked = 1 if n == 0 else min(10, max(1, (n + 20) // 21))
    rf_log = ", {}".format(_fmt_rssi_snr(rssi, snr))
    return "[{} from {}{}] [{}] {} bytes, MSG_UID = {}".format(
        recv_log, sender, rf_log, "*" * masked, n, msg_uid
    )


def process_message(databytes, rssi=None, snr=None):
    """Dummy handler for the standalone receive test."""
    pass


if __name__ == "__main__":
    import logger
    from config import get_machine_id
    import struct
    import ubinascii
    from clock_utils import get_epoch_sec
    from message_codec import encode_x_message
    from utils import get_rand
    
    my_addr = get_machine_id()
    if my_addr is None:
        logger.error("No machine id found for this device, exiting...")
        exit(1)
    
    radio_owner = RadioOwner(logger=logger, my_addr=my_addr)
    radio_owner.run()
    
    # ---- SENDING CODE -----
    SEND_MS = 60 * 1000
    SEND_EVERY_MS = 6 * 1000
    dest = 65535
    logger.info("sending X every 6 seconds for 1 minute")
    send_deadline = utime.ticks_add(utime.ticks_ms(), SEND_MS)
    sent = 0
    while utime.ticks_diff(send_deadline, utime.ticks_ms()) > 0:
        msgbytes = encode_x_message(my_addr, get_epoch_sec())
        if msgbytes is None:
            logger.error("failed to encode X message")
        else:
            msg_uid = b"X" + bytes((my_addr, my_addr)) + b"*" + get_rand(3).encode()
            crc = struct.pack(">I", ubinascii.crc32(msgbytes) & 0xFFFFFFFF)
            payload = msg_uid + crc + b";" + msgbytes
            err = radio_owner.send(payload)
            if err:
                logger.error("[LORA] send X failed: {}, MSG_UID = {}".format(err, msg_uid))
            else:
                sent += 1
                masked = min(10, max(1, (len(payload) + 20) // 21))
                logger.info("[⮕ SENT to {}] [{}] {} bytes, MSG_UID = {}".format(
                    dest, "*" * masked, len(payload), msg_uid
                ))
        utime.sleep_ms(SEND_EVERY_MS)
    logger.info("✔✔✔ send test done, sent {} X frames".format(sent))

    
    # ---- LISTING CODE -----
    LISTEN_MS = 4 * 60 * 1000  # listen for 4 minutes
    logger.info("listening for 4 minutes")
    deadline = utime.ticks_add(utime.ticks_ms(), LISTEN_MS)
    got = 0
    while utime.ticks_diff(deadline, utime.ticks_ms()) > 0:
        if radio_owner.get_recv_msg_count() > 0:
            data, rx_status, rx_rssi, rx_snr = radio_owner.get_recv_msg()
            got += 1
            logger.info(_recv_log_line(data, rx_rssi, rx_snr))
            process_message(data, rx_rssi, rx_snr)
        else:
            utime.sleep_ms(100)
    logger.info("✔✔✔ receive test done, received {} frames".format(got))
