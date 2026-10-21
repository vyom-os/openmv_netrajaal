import logger
from machine import LED, Pin, UART
import machine
import uasyncio as asyncio
import utime
import sensor
import random
import ubinascii
import gc
import enc
from config import uid, get_my_addr, get_version_str, VERSION
from utils import pack_image_meta_header, get_free_memory
from internet_driver import (
    InternetDriver,
    UART_ID,
    BAUDRATE,
    UART_HEALTH_INTERVAL_SEC,
    SIM_UNDETECTED_INTERVAL_SEC,
    FAILED_RE_CONFIGURE_INTERVAL_SEC,
    FAILED_INTERNET_RETRY_INTERVAL_SEC_CC,
)
import power_mgmt
from watchdog import start_watchdog, check_watchdog_reset, feed_watchdog
from supervisor import supervised

# Minimum time from the start of one capture to the start of the next.
# Upload finishes before the next capture so only one image is in RAM.
CAPTURE_INTERVAL_SEC = 6
UPLOAD_ATTEMPTS = 3
UPLOAD_RETRY_PAUSE_SEC = 5
ENCRYPTION_ENABLED = True
URL = "https://api.vyomiq.io/watchmen-detect/"

# Same direct-upload size window previously used for command-center uploads.
HIGH_TARGET_SIZE = 99 * 1024
LOW_TARGET_SIZE = 80 * 1024
HIGH_COMP_QUALITY = 100
MIN_COMP_QUALITY = 55

led = LED("LED_BLUE")
led.off()
ir_emitter = Pin("P14", Pin.OUT)
ir_emitter.off()

encnode = None
my_addr = None
internet_module = None
tracx_uart = None
tracx_uart_lock = asyncio.Lock()
sensor_initialized = False
img_capture_count = 0
img_file_counter = 0
global_comp_quality = 55


def get_rand(length=3):
    rstr = ""
    for _ in range(length):
        rstr += chr(65 + random.randint(0, 25))
    return rstr


def get_epoch_ms():
    return utime.time_ns() // 1_000_000


def turn_on_ir():
    ir_emitter.on()
    utime.sleep_ms(100)


def turn_off_ir():
    ir_emitter.off()


def init_camera_sensor():
    global sensor_initialized
    try:
        if sensor_initialized:
            return True
        sensor.reset()
        sensor.set_pixformat(sensor.GRAYSCALE)
        sensor.set_framesize(sensor.HD)
        sensor.skip_frames(time=2000)
        sensor_initialized = True
        logger.info("[INIT] Camera/sensor initialized")
        return True
    except Exception as e:
        sensor_initialized = False
        logger.error(f"EXCP_ERR: [INIT] Failed to initialize camera/sensor: {e}\n{logger.exc_str(e)}")
        return False


def encrypt_image(plain):
    try:
        if not ENCRYPTION_ENABLED:
            return plain
        msgbytes = enc.encrypt_hybrid(plain, encnode.get_pub_key())
        logger.debug(f"P : Len msg = {len(plain)}, len msgbytes = {len(msgbytes)}")
        return msgbytes
    except Exception as e:
        logger.error(f"EXCP_ERR: Error in encrypt_image: {e}\n{logger.exc_str(e)}")
        return None


def internet_ready():
    return bool(
        internet_module
        and internet_module.module_ready
        and internet_module.internet_established()
        and internet_module.cc_enabled()
    )


async def init_device():
    global encnode, my_addr
    logger.info(f"UID: {uid}")
    my_addr = get_my_addr()
    if my_addr is None:
        logger.error(f"error in main.py: Unknown device UID for {uid}, rebooting...")
        return False
    logger.info(f"MY_ADDR: {my_addr}")
    logger.info(f"FIRM_VER: {get_version_str(VERSION)}")

    try:
        encnode = enc.EncNode(my_addr)
    except Exception as e:
        logger.error(f"EXCP_ERR: [INIT] Encryption init failed: {e}\n{logger.exc_str(e)}")
        return False

    rtc = machine.RTC()
    rtc.datetime((2024, 1, 1, 0, 0, 0, 0, 0))
    gc.enable()
    logger.info(f"[INIT] Free memory: {get_free_memory()}KB")

    if not init_camera_sensor():
        logger.error("[INIT] Failed to initialize camera/sensor, will retry on the next capture")
    else:
        power_mgmt.camera_wake()
    return True


async def init_tracx_internet():
    global internet_module, tracx_uart
    logger.info("\n[CELL] === Initializing Internet Module ===")
    if tracx_uart is None:
        logger.info(f"[CELL] Creating shared UART (id={UART_ID}, baud={BAUDRATE})...")
        tracx_uart = UART(UART_ID, BAUDRATE, timeout=2000)
        await asyncio.sleep(1)

    async with tracx_uart_lock:
        internet_module = InternetDriver(uart=tracx_uart)
        internet_module.set_cc_enabled(True)
        await internet_module.establish_internet()
        if internet_module.internet_established():
            logger.info("[CELL] Internet established, device will upload captures")
            return True
        logger.fatal("[CELL] Internet configuration failed; will retry periodically")
        return False


async def re_init_tracx_internet(reset=False):
    global internet_module
    gc.collect()
    async with tracx_uart_lock:
        if reset:
            internet_module.reset_module()
        internet_module.set_cc_enabled(True)
        await internet_module.establish_internet(retry_count=2)
        if internet_module and internet_module.internet_established():
            logger.info("[CELL] Internet re-established")
            return True
        logger.error("[CELL] Internet re-configuration failed; will retry periodically")
        return False


async def keep_checking_internet():
    """Re-init only when the modem or link is actually down. A healthy link stays up."""
    while True:
        try:
            if internet_module is None:
                await asyncio.sleep(5)
                continue

            if internet_ready():
                await asyncio.sleep(UART_HEALTH_INTERVAL_SEC)
                async with tracx_uart_lock:
                    healthy = await internet_module.check_module_health()
                if healthy and internet_ready():
                    continue
                logger.error("[CELL] Health check failed, resetting module and re-initializing")
                await re_init_tracx_internet(reset=True)
                await asyncio.sleep(UART_HEALTH_INTERVAL_SEC)
                continue

            if not internet_module.module_ready:
                logger.error("[CELL] Internet module not ready, resetting and re-initializing")
                await re_init_tracx_internet(reset=True)
                await asyncio.sleep(UART_HEALTH_INTERVAL_SEC)
                continue

            if not internet_module.has_sim:
                logger.error("[CELL] SIM not detected, re-initializing")
                await re_init_tracx_internet(reset=False)
                await asyncio.sleep(SIM_UNDETECTED_INTERVAL_SEC)
                continue

            if not internet_module.configured:
                logger.error("[CELL] Module not configured, re-initializing")
                await re_init_tracx_internet(reset=False)
                await asyncio.sleep(FAILED_RE_CONFIGURE_INTERVAL_SEC)
                continue

            logger.error("[CELL] Internet not connected, re-initializing")
            await re_init_tracx_internet(reset=False)
            await asyncio.sleep(FAILED_INTERNET_RETRY_INTERVAL_SEC_CC)
        except Exception as e:
            logger.error(f"EXCP_ERR: [CELL] keep_checking_internet: {e}\n{logger.exc_str(e)}")
            await asyncio.sleep(30)


async def upload_payload_to_server(payload, msg_typ, creator):
    if not internet_ready():
        logger.error(f"upload skipped, internet not ready (msg_typ:{msg_typ} creator:{creator})")
        return False
    try:
        await asyncio.sleep_ms(100)
        signal_strength = internet_module.get_last_signal_strength()
        logger.info(
            f"[CELL] Uploading data, signal strength: {signal_strength if signal_strength is not None else 'unknown'}%"
        )
        async with tracx_uart_lock:
            result, _, response_message = await internet_module.upload_data(payload, URL)
        if result:
            logger.info(f"msg_typ:{msg_typ} from node {creator} sent to cloud successfully")
            return True
        logger.error(
            f"msg_typ:{msg_typ} from node {creator} failed to send to cloud via cellular, error:{response_message}"
        )
        return False
    except Exception as e:
        logger.error(
            f"EXCP_ERR: msg_typ:{msg_typ} from node {creator} error sending to cloud via cellular: {e}\n{logger.exc_str(e)}"
        )
        return False


def capture_image():
    """Capture one JPEG. Recompress this frame until it fits the upload window."""
    global img_capture_count, img_file_counter, global_comp_quality
    img_snapshot = None
    jpeg = None
    try:
        power_mgmt.camera_wake()
        turn_on_ir()
        if not init_camera_sensor():
            logger.error("[IMG] Failed to initialize camera/sensor, will retry later")
            return None, None
        img_snapshot = sensor.snapshot()
        if img_snapshot is None:
            raise Exception("snapshot returned None (camera not ready)")
        sensor.flush()

        quality = global_comp_quality
        for _ in range(8):
            compressed = img_snapshot.compress(quality=quality)
            jpeg = bytes(compressed)
            compressed = None
            size = len(jpeg)
            logger.info(f"Compressed image size: {size}, compress_quality-{quality}")
            if size <= HIGH_TARGET_SIZE or quality <= MIN_COMP_QUALITY:
                break
            jpeg = None
            gc.collect()
            quality = max(MIN_COMP_QUALITY, quality - 5)

        global_comp_quality = quality
        if jpeg is not None and len(jpeg) < LOW_TARGET_SIZE:
            global_comp_quality = min(HIGH_COMP_QUALITY, quality + 5)
        elif jpeg is not None and len(jpeg) > HIGH_TARGET_SIZE:
            global_comp_quality = max(MIN_COMP_QUALITY, quality - 5)

        img_capture_count += 1
        img_file_counter += 1
        event_epoch_ms = get_epoch_ms() + img_file_counter
        return jpeg, event_epoch_ms
    except Exception as e:
        logger.error(f"EXCP_ERR: [IMG] Failed to capture image: {e}\n{logger.exc_str(e)}")
        return None, None
    finally:
        img_snapshot = None
        turn_off_ir()


async def upload_b64_image(data_b64_str, epoch_ms):
    """POST the same payload the command center used for an encrypted image."""
    server_payload = {
        "machine_id": my_addr,
        "msg_typ": "F",
        "data": data_b64_str,
        "epoch_ms": epoch_ms,
        "enc": ENCRYPTION_ENABLED,
    }
    logger.info(f"[IMG] Uploading encrypted image, file: {my_addr}_{epoch_ms}")
    return await upload_payload_to_server(server_payload, "F", my_addr)


async def capture_and_upload_once():
    """Capture, hybrid-encrypt, upload. Each buffer is freed before the next copy."""
    jpeg = None
    plain = None
    enc_msgbytes = None
    data_b64_str = None
    led.on()
    try:
        jpeg, event_epoch_ms = capture_image()
        if jpeg is None or event_epoch_ms is None:
            return False

        image_id = get_rand(3)
        plain = pack_image_meta_header(0, 0, event_epoch_ms, image_id, "F") + jpeg
        jpeg = None
        gc.collect()

        enc_msgbytes = encrypt_image(plain)
        plain = None
        gc.collect()
        if enc_msgbytes is None:
            logger.error(f"[IMG] Encryption failed, free memory {get_free_memory()}KB")
            return False

        logger.info(
            f"[IMG] Captured {my_addr}_{event_epoch_ms} ({len(enc_msgbytes)} bytes encrypted), free {get_free_memory()}KB"
        )
        data_b64_str = ubinascii.b2a_base64(enc_msgbytes).rstrip().decode()
        enc_msgbytes = None
        gc.collect()
        led.off()

        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            sent = await upload_b64_image(data_b64_str, event_epoch_ms)
            if sent:
                logger.info(f"[IMG] Uploaded {my_addr}_{event_epoch_ms}")
                return True
            logger.warning(f"[IMG] Upload attempt {attempt}/{UPLOAD_ATTEMPTS} failed for {my_addr}_{event_epoch_ms}")
            if attempt < UPLOAD_ATTEMPTS:
                await asyncio.sleep(UPLOAD_RETRY_PAUSE_SEC)
        return False
    except Exception as e:
        logger.error(f"EXCP_ERR: [IMG] capture/upload error: {e}\n{logger.exc_str(e)}")
        return False
    finally:
        led.off()
        jpeg = None
        plain = None
        enc_msgbytes = None
        data_b64_str = None
        gc.collect()


async def capture_loop():
    logger.info(f"[IMG] Automatic capture every {CAPTURE_INTERVAL_SEC}s, then encrypt and upload")
    waiting_logged = False
    while True:
        cycle_start = utime.ticks_ms()
        if not internet_ready():
            if not waiting_logged:
                logger.info("[IMG] Internet not ready, waiting to capture")
                waiting_logged = True
            await asyncio.sleep(UPLOAD_RETRY_PAUSE_SEC)
            continue
        waiting_logged = False

        await capture_and_upload_once()

        elapsed_ms = utime.ticks_diff(utime.ticks_ms(), cycle_start)
        delay_ms = CAPTURE_INTERVAL_SEC * 1000 - elapsed_ms
        if delay_ms > 0:
            await asyncio.sleep_ms(delay_ms)
        else:
            logger.info(f"[IMG] Capture cycle took {elapsed_ms} ms, starting the next frame")


async def main():
    logger.info("Entering MAIN loop... [CAPTURE + UPLOAD]")
    check_watchdog_reset()
    if not await init_device():
        await asyncio.sleep(10)
        machine.reset()

    start_watchdog()
    await init_tracx_internet()
    asyncio.create_task(supervised("keep_checking_internet", keep_checking_internet, critical=True))
    asyncio.create_task(supervised("capture_loop", capture_loop, critical=True))

    while True:
        await asyncio.sleep(3600)
        logger.info(
            f"[IMG] capture+upload still running, captured={img_capture_count}, free={get_free_memory()}KB"
        )


try:
    asyncio.run(main())
except KeyboardInterrupt as e:
    logger.error(f"EXCP_ERR: stopped by user via keyboard interrupt\n{logger.exc_str(e)}")
except Exception as e:
    logger.fatal(f"EXCP_ERR: Uncaught error in main.py: {str(e)}\n{logger.exc_str(e)}")
finally:
    try:
        print("SHUTTING DOWN, restarting the device in 5 min...")
        try:
            logger.info("SHUTTING DOWN, restarting the device in 5 min...")
        except Exception:
            pass
        for _ in range(30):
            feed_watchdog()
            utime.sleep(10)
        machine.reset()
    except Exception as e:
        logger.fatal(f"EXCP_ERR: error in restarting the device in main.py: {str(e)}\n{logger.exc_str(e)}")