import logger
from machine import LED, Pin, UART
import machine
import network
import socket
import uasyncio as asyncio
import utime
import sensor
import random
import ubinascii
import gc
import enc
from config import uid, get_my_addr, get_version_str, VERSION
from utils import pack_image_meta_header, get_free_memory
from internet_driver import InternetDriver, UART_ID, BAUDRATE
import power_mgmt
from watchdog import start_watchdog, check_watchdog_reset, feed_watchdog
from supervisor import supervised

try:
    import ssl
except ImportError:
    import ussl as ssl

# One frame is queued ahead of the uploader. The next capture starts as soon
# as that slot is free, and it does not begin during an open socket send.
MAX_QUEUED_IMAGES = 1
UPLOAD_RETRY_PAUSE_MS = 400
ENCRYPTION_ENABLED = True
UPLOAD_HOST = "api.vyomiq.io"
UPLOAD_PATH = "/watchmen-detect/"
UPLOAD_PORT = 443

WIFI_SSID = "Airtel_AirtelBNS"
WIFI_PASSWORD = "silkbroad1243"
WIFI_CONNECT_TIMEOUT_SEC = 20

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
wifi_nic = None
wifi_lock = asyncio.Lock()
upload_in_progress = False
upload_queue = []
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


_ir_on = False


def turn_on_ir():
    global _ir_on
    if _ir_on:
        return
    ir_emitter.on()
    _ir_on = True
    utime.sleep_ms(100)


def turn_off_ir():
    global _ir_on
    ir_emitter.off()
    _ir_on = False


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


def wifi_ready():
    try:
        return bool(wifi_nic is not None and wifi_nic.isconnected())
    except Exception:
        return False


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


async def sleep_gsm_module():
    """Hardware-reset the EC200, then turn the radio off and allow UART sleep."""
    global internet_module, tracx_uart
    logger.info("[CELL] Putting GSM module to sleep")
    try:
        if tracx_uart is None:
            logger.info(f"[CELL] Creating UART (id={UART_ID}, baud={BAUDRATE})")
            tracx_uart = UART(UART_ID, BAUDRATE, timeout=2000)
            await asyncio.sleep(1)
        internet_module = InternetDriver(uart=tracx_uart)
        internet_module.set_cc_enabled(False)

        ready = False
        for attempt in range(1, 9):
            await asyncio.sleep(2)
            ok, resp = await internet_module._send_command("AT", timeout=2)
            if ok:
                ready = True
                logger.info(f"[CELL] GSM answered AT on attempt {attempt}")
                break
            logger.info(f"[CELL] GSM not ready yet ({attempt}/8): {resp}")
        if not ready:
            logger.error("[CELL] GSM did not answer AT; sleep command may not apply")

        ok, resp = await internet_module._send_command("AT+CFUN=4", timeout=15)
        if ok:
            logger.info("[CELL] Cellular antenna off (CFUN=4)")
        else:
            logger.error(f"[CELL] CFUN=4 failed: {resp}")
        ok, resp = await internet_module._send_command("AT+QSCLK=1", timeout=2)
        if ok:
            logger.info("[CELL] EC200 UART sleep enabled (QSCLK=1)")
        else:
            logger.error(f"[CELL] QSCLK=1 failed: {resp}")
        return True
    except Exception as e:
        logger.error(f"EXCP_ERR: [CELL] Failed to sleep GSM module: {e}\n{logger.exc_str(e)}")
        return False


async def connect_wifi():
    """Join the fiber router. Safe to call again while already connected."""
    global wifi_nic
    async with wifi_lock:
        try:
            if wifi_nic is None:
                wifi_nic = network.WLAN(network.WLAN.IF_STA)
            if wifi_nic.isconnected():
                return True

            try:
                wifi_nic.active(True)
            except Exception as e:
                logger.error(f"[WIFI] active(True) failed: {e}")
                return False
            await asyncio.sleep(1)

            if wifi_nic.isconnected():
                logger.info(f"[WIFI] Already connected, IP {wifi_nic.ifconfig()[0]}")
                return True

            logger.info(f"[WIFI] Connecting to {WIFI_SSID}")
            try:
                wifi_nic.disconnect()
            except Exception:
                pass
            wifi_nic.connect(WIFI_SSID, WIFI_PASSWORD)
            deadline = utime.ticks_add(utime.ticks_ms(), WIFI_CONNECT_TIMEOUT_SEC * 1000)
            while utime.ticks_diff(deadline, utime.ticks_ms()) > 0:
                if wifi_nic.isconnected():
                    logger.info(f"[WIFI] Connected, IP {wifi_nic.ifconfig()[0]}")
                    return True
                await asyncio.sleep_ms(500)
            logger.error(f"[WIFI] Connection timeout, status={wifi_nic.status()}")
            return False
        except Exception as e:
            logger.error(f"EXCP_ERR: [WIFI] connect failed: {e}\n{logger.exc_str(e)}")
            return False


async def keep_wifi_connected():
    while True:
        try:
            await asyncio.sleep(15)
            if upload_in_progress:
                continue
            if not wifi_ready():
                logger.warning("[WIFI] Link down, reconnecting")
                await connect_wifi()
        except Exception as e:
            logger.error(f"EXCP_ERR: [WIFI] keep_wifi_connected: {e}\n{logger.exc_str(e)}")
            await asyncio.sleep(5)


def _ipv4_addr(host, port):
    infos = socket.getaddrinfo(host, port)
    if not infos:
        raise OSError("DNS returned no address")
    for info in infos:
        if info[0] == socket.AF_INET:
            return info[-1]
    return infos[0][-1]


async def _write_all(sock, data):
    if isinstance(data, str):
        data = data.encode()
    sent = 0
    stalled = 0
    length = len(data)
    while sent < length:
        chunk = data[sent:sent + 512]
        n = sock.write(chunk)
        if n is None:
            stalled += 1
            if stalled > 100:
                raise OSError("socket write stalled")
            await asyncio.sleep_ms(10)
            continue
        if n <= 0:
            raise OSError("socket write returned 0")
        stalled = 0
        sent += n


async def _write_text(sock, text):
    step = 1024
    for i in range(0, len(text), step):
        await _write_all(sock, text[i:i + step])


def _open_tls(addr):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(8)
    kwargs = {}
    if hasattr(ssl, "CERT_NONE"):
        kwargs["cert_reqs"] = ssl.CERT_NONE
    try:
        sock.connect(addr)
        try:
            tls = ssl.wrap_socket(sock, server_hostname=UPLOAD_HOST, **kwargs)
        except TypeError:
            try:
                tls = ssl.wrap_socket(sock, **kwargs)
            except TypeError:
                tls = ssl.wrap_socket(sock)
        try:
            tls.settimeout(8)
        except Exception:
            pass
        return tls
    except Exception:
        try:
            sock.close()
        except Exception:
            pass
        raise


async def upload_b64_image(data_b64_str, epoch_ms):
    """HTTPS POST the same JSON fields previously sent over cellular."""
    global upload_in_progress
    if not wifi_ready():
        logger.error("[WIFI] upload skipped, WiFi not connected")
        return False

    enc_flag = "true" if ENCRYPTION_ENABLED else "false"
    prefix = '{"machine_id":%d,"msg_typ":"F","data":"' % my_addr
    suffix = '","epoch_ms":%d,"enc":%s}' % (epoch_ms, enc_flag)
    body_len = len(prefix) + len(data_b64_str) + len(suffix)
    header = (
        "POST %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n"
    ) % (UPLOAD_PATH, UPLOAD_HOST, body_len)

    tls = None
    upload_in_progress = True
    try:
        logger.info(f"[WIFI] Uploading {my_addr}_{epoch_ms} ({body_len} bytes) to {UPLOAD_HOST}")
        addr = _ipv4_addr(UPLOAD_HOST, UPLOAD_PORT)
        tls = _open_tls(addr)
        await _write_all(tls, header)
        await _write_text(tls, prefix)
        await _write_text(tls, data_b64_str)
        await _write_text(tls, suffix)

        response = b""
        deadline = utime.ticks_add(utime.ticks_ms(), 20000)
        while b"\r\n" not in response and utime.ticks_diff(deadline, utime.ticks_ms()) > 0 and len(response) < 512:
            try:
                chunk = tls.read(256)
            except OSError:
                chunk = None
            if chunk:
                response += chunk
            else:
                await asyncio.sleep_ms(50)

        line = response.split(b"\r\n", 1)[0] if response else b""
        parts = line.split(b" ")
        try:
            status = int(parts[1]) if len(parts) > 1 else 0
        except Exception:
            status = 0
        if status == 200:
            logger.info(f"[WIFI] {my_addr}_{epoch_ms} sent to cloud, HTTP {status}")
            return True
        logger.error(f"[WIFI] {my_addr}_{epoch_ms} upload failed, HTTP {status}, head={line}")
        return False
    except Exception as e:
        logger.error(f"EXCP_ERR: [WIFI] upload error for {my_addr}_{epoch_ms}: {e}\n{logger.exc_str(e)}")
        return False
    finally:
        upload_in_progress = False
        if tls is not None:
            try:
                tls.close()
            except Exception:
                pass


def capture_image():
    """Capture one JPEG. Recompress this frame until it fits the upload window."""
    global img_capture_count, img_file_counter, global_comp_quality
    img_snapshot = None
    jpeg = None
    try:
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


def take_encrypted_frame():
    """Capture and hybrid-encrypt one frame. Returns (base64, epoch_ms) or (None, None)."""
    jpeg = None
    plain = None
    enc_msgbytes = None
    try:
        jpeg, event_epoch_ms = capture_image()
        if jpeg is None or event_epoch_ms is None:
            return None, None

        image_id = get_rand(3)
        plain = pack_image_meta_header(0, 0, event_epoch_ms, image_id, "F") + jpeg
        jpeg = None
        gc.collect()

        enc_msgbytes = encrypt_image(plain)
        plain = None
        gc.collect()
        if enc_msgbytes is None:
            logger.error(f"[IMG] Encryption failed, free memory {get_free_memory()}KB")
            return None, None

        raw_b64 = ubinascii.b2a_base64(enc_msgbytes)
        if not isinstance(raw_b64, str):
            raw_b64 = raw_b64.decode()
        data_b64_str = raw_b64.replace("\n", "").replace("\r", "")
        logger.info(
            f"[IMG] Captured {my_addr}_{event_epoch_ms} ({len(enc_msgbytes)} bytes encrypted), free {get_free_memory()}KB"
        )
        enc_msgbytes = None
        return data_b64_str, event_epoch_ms
    except Exception as e:
        logger.error(f"EXCP_ERR: [IMG] capture error: {e}\n{logger.exc_str(e)}")
        return None, None
    finally:
        jpeg = None
        plain = None
        enc_msgbytes = None
        gc.collect()


async def capture_loop():
    """Keep taking frames. Upload runs in its own task."""
    logger.info("[IMG] Continuous capture started")
    power_mgmt.camera_wake()
    turn_on_ir()
    while True:
        # Let the open POST finish. Starting another encrypt here drops the socket.
        while upload_in_progress or len(upload_queue) >= MAX_QUEUED_IMAGES:
            await asyncio.sleep_ms(20)
        led.on()
        data_b64_str, epoch_ms = take_encrypted_frame()
        led.off()
        if data_b64_str is None:
            await asyncio.sleep_ms(200)
            continue
        upload_queue.append((epoch_ms, data_b64_str))


async def upload_loop():
    """Send queued frames over Wi-Fi. A frame stays queued until HTTP 200."""
    logger.info("[WIFI] Upload task started")
    while True:
        if not upload_queue:
            await asyncio.sleep_ms(20)
            continue

        epoch_ms, data_b64_str = upload_queue[0]
        if not wifi_ready():
            await connect_wifi()
            if not wifi_ready():
                await asyncio.sleep_ms(UPLOAD_RETRY_PAUSE_MS)
                continue

        sent = await upload_b64_image(data_b64_str, epoch_ms)
        if sent and upload_queue and upload_queue[0][0] == epoch_ms:
            upload_queue.pop(0)
            logger.info(f"[IMG] Upload success {my_addr}_{epoch_ms}, still queued={len(upload_queue)}")
        else:
            logger.warning(f"[IMG] Upload not accepted for {my_addr}_{epoch_ms}, retrying")
            await asyncio.sleep_ms(UPLOAD_RETRY_PAUSE_MS)


async def main():
    logger.info("Entering MAIN loop... [CAPTURE + WIFI UPLOAD]")
    check_watchdog_reset()
    if not await init_device():
        await asyncio.sleep(10)
        machine.reset()

    start_watchdog()
    await sleep_gsm_module()
    await connect_wifi()
    asyncio.create_task(supervised("keep_wifi_connected", keep_wifi_connected, critical=False))
    asyncio.create_task(supervised("upload_loop", upload_loop, critical=True))
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
