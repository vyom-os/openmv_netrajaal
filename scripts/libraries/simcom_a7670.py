"""SIMCom A7670C modem driver — AT connect + HTTP(S) POST only."""
import time
import json
import uasyncio as asyncio

try:
    from machine import UART, Pin
    HAS_MACHINE_UART = True
except ImportError:
    HAS_MACHINE_UART = False
    Pin = None
    UART = None

# OpenMV Cam RT1062: only external HW UART is UART1 on P4(TX)/P5(RX)
# https://docs.openmv.io/openmvcam/quickref/openmv-rt1062.html
UART_ID = 1
UART_TX_PIN = "P4"
UART_RX_PIN = "P5"
BAUDRATE = 115200
SIM_APN = "iot.com"


class _UARTSerialAdapter:
    def __init__(self, uart):
        self._uart = uart

    @property
    def in_waiting(self):
        n = self._uart.any()
        return int(n) if n else 0

    def read(self, n):
        if not n:
            return b""
        data = self._uart.read(n)
        return data if data else b""

    def write(self, data):
        return self._uart.write(data)


class A7670Driver:
    """A7670C AT client: connect (SIM/PDP/HTTP) + upload_data (POST)."""

    def __init__(self, uart=None, uart_id=UART_ID, baudrate=BAUDRATE, apn=SIM_APN):
        self.apn = apn
        self.default_timeout = 10
        self.connected = False
        self.http_ready = False

        if uart is not None:
            if hasattr(uart, "any") and not hasattr(uart, "in_waiting"):
                self.uart = _UARTSerialAdapter(uart)
            else:
                self.uart = uart
        elif HAS_MACHINE_UART:
            # RT1062: UART(1) → P4 TX / P5 RX (pins are fixed; cannot remap)
            uart = UART(
                uart_id,
                baudrate,
                timeout=2000,
                rxbuf=4096,
            )
            time.sleep_ms(500)
            self.uart = _UARTSerialAdapter(uart)
        else:
            raise Exception("UART not available")

        print("[A7670] uart ready")

    async def _send_command(self, command, wait_for="OK", timeout=None):
        if timeout is None:
            timeout = self.default_timeout
        try:
            # drain stale bytes
            n = getattr(self.uart, "in_waiting", 0) or 0
            if n:
                self.uart.read(n)
        except Exception:
            pass
        try:
            self.uart.write((command + "\r\n").encode())
        except Exception as e:
            print("[A7670] write failed: %s" % e)
            return False, ""
        return await self._read_response(wait_for, int(timeout * 1000))

    async def _read_response(self, wait_for, timeout_ms, error_str="ERROR"):
        deadline = time.ticks_ms() + timeout_ms
        response = ""
        while time.ticks_diff(deadline, time.ticks_ms()) > 0:
            try:
                available = getattr(self.uart, "in_waiting", 0) or 0
            except Exception:
                available = 0
            if available:
                chunk = self.uart.read(available)
                if chunk:
                    try:
                        response += chunk.decode("utf-8")
                    except Exception:
                        try:
                            response += chunk.decode("latin-1")
                        except Exception:
                            pass
                if wait_for and wait_for in response:
                    return True, response
                if error_str and error_str in response:
                    return False, response
            await asyncio.sleep(0.02)
        return False, response

    async def _write_exact(self, data):
        """Send every byte. A short UART write truncates the JPEG body."""
        if isinstance(data, str):
            data = data.encode()
        mv = memoryview(data)
        total = len(mv)
        sent = 0
        chunk = 256
        while sent < total:
            n = chunk if (total - sent) > chunk else (total - sent)
            piece = bytes(mv[sent:sent + n])
            wrote = self.uart.write(piece)
            if wrote is None or wrote == len(piece):
                sent += n
            elif wrote > 0:
                sent += wrote
            else:
                await asyncio.sleep(0.02)
                continue
            await asyncio.sleep(0.03)
        await asyncio.sleep(0.05)
        return sent

    def _reg_stat(self, resp):
        """Registration status from +CREG/+CGREG/+CEREG. 1=home, 5=roam."""
        if not resp:
            return None
        for line in resp.replace("\r", "").split("\n"):
            line = line.strip()
            if not (
                line.startswith("+CEREG:")
                or line.startswith("+CGREG:")
                or line.startswith("+CREG:")
            ):
                continue
            try:
                fields = line.split(":", 1)[1].strip().split(",")
                if len(fields) >= 2:
                    return int(fields[1].strip())
                return int(fields[0].strip())
            except Exception:
                continue
        return None

    async def _wait_registered(self, timeout_sec):
        """Poll until home/roam. Prints status so a timeout is diagnosable."""
        deadline = time.ticks_ms() + timeout_sec * 1000
        while time.ticks_diff(deadline, time.ticks_ms()) > 0:
            bits = []
            registered = False
            for cmd in ("AT+CEREG?", "AT+CGREG?", "AT+CREG?"):
                _, resp = await self._send_command(cmd, timeout=4)
                stat = self._reg_stat(resp)
                bits.append("%s:%s" % (cmd[3:cmd.find("?")], stat))
                if stat in (1, 5):
                    registered = True
            _, csq = await self._send_command("AT+CSQ", timeout=3)
            csq_txt = ""
            if csq and "+CSQ:" in csq:
                csq_txt = csq.replace("\r", "").split("+CSQ:")[1].split("\n")[0].strip()
            print("[A7670] %s  CSQ %s" % (" ".join(bits), csq_txt or "?"))
            if registered:
                return True
            await asyncio.sleep(3)
        return False

    async def wake(self):
        """Leave UART sleep. The first AT while CSCLK is on is often dropped."""
        await self._send_command("AT", timeout=1)
        await asyncio.sleep(0.2)
        ok, _ = await self._send_command("AT+CSCLK=0", timeout=3)
        if not ok:
            await self._send_command("AT", timeout=2)
            ok, _ = await self._send_command("AT+CSCLK=0", timeout=3)
        print("[A7670] wake %s" % ("ok" if ok else "no AT"))
        return ok

    async def sleep(self):
        """Radio off, then UART sleep. Call this before the host deep-sleeps."""
        self.connected = False
        self.http_ready = False
        await self._send_command("AT", timeout=1)
        await asyncio.sleep(0.2)
        await self._send_command("AT+HTTPTERM", timeout=3)
        await self._send_command("AT+CGACT=0,1", timeout=10)
        await self._send_command("AT+CFUN=4", timeout=15)
        ok, resp = await self._send_command("AT+CSCLK=2", timeout=3)
        print("[A7670] sleep %s" % ("ok" if ok else resp))
        return ok

    async def connect(self, settle_sec=5, reg_timeout_sec=90):
        """AT → SIM → register → PDP → HTTPINIT."""
        print("[A7670] connecting (settle %ss)..." % settle_sec)
        await asyncio.sleep(settle_sec)

        for _ in range(8):
            ok, _ = await self._send_command("AT", timeout=3)
            if ok:
                break
            await asyncio.sleep(1)
        else:
            print("[A7670] no AT response")
            return False

        await self._send_command("ATE0", timeout=2)
        await self._send_command("AT+CMEE=2", timeout=2)

        ok, resp = await self._send_command("AT+CPIN?", timeout=5)
        if not ok or "READY" not in resp.upper():
            print("[A7670] SIM not ready: %s" % resp)
            return False
        print("[A7670] SIM ready")

        # Full radio. CNMP=2 is automatic (GSM+LTE). CNMP=38 is LTE-only and
        # restarts the search, which was timing out before a tower was found.
        await self._send_command("AT+CFUN=1", timeout=15)
        await self._send_command("AT+CNMP=2", timeout=10)
        await asyncio.sleep(2)

        if not await self._wait_registered(reg_timeout_sec):
            print("[A7670] still searching, AT+COPS=0 ...")
            await self._send_command("AT+COPS=0", timeout=60)
            if not await self._wait_registered(60):
                print("[A7670] network registration timeout")
                return False
        print("[A7670] network registered")

        ok, resp = await self._send_command("AT+CSQ", timeout=3)
        print(
            "[A7670] CSQ: %s"
            % (resp or "").strip().replace("\r", " ").replace("\n", " ")
        )

        if not await self._activate_pdp():
            return False
        if not await self._http_init():
            return False

        self.connected = True
        print("[A7670] connected")
        return True

    async def _activate_pdp(self):
        await self._send_command("AT+CGACT=0,1", timeout=10)
        await asyncio.sleep(1)

        ok, resp = await self._send_command(
            'AT+CGDCONT=1,"IP","%s"' % self.apn, timeout=5
        )
        if not ok:
            print("[A7670] CGDCONT failed: %s" % resp)
            return False

        ok, resp = await self._send_command("AT+CGACT=1,1", timeout=40)
        if not ok:
            print("[A7670] CGACT failed, trying CNACT: %s" % resp)
            ok, resp = await self._send_command("AT+CNACT=0,1", timeout=40)
            if not ok:
                print("[A7670] PDP activate failed: %s" % resp)
                return False

        await asyncio.sleep(2)
        ok, resp = await self._send_command("AT+CGPADDR=1", timeout=5)
        if ok:
            print(
                "[A7670] PDP up: %s"
                % (resp or "").strip().replace("\r", " ").replace("\n", " ")
            )
        else:
            _, resp2 = await self._send_command("AT+CNACT?", timeout=5)
            print(
                "[A7670] CNACT?: %s"
                % (resp2 or "").strip().replace("\r", " ").replace("\n", " ")
            )
        return True

    async def _http_init(self):
        await self._send_command("AT+HTTPTERM", timeout=3)
        await asyncio.sleep(0.3)

        # TLS 1.2, no cert check, ignore clock. Context id 1.
        await self._send_command('AT+CSSLCFG="sslversion",1,3', timeout=3)
        await self._send_command('AT+CSSLCFG="authmode",1,0', timeout=3)
        await self._send_command('AT+CSSLCFG="ignorertctime",1,1', timeout=3)

        ok, resp = await self._send_command("AT+HTTPINIT", timeout=10)
        if not ok:
            print("[A7670] HTTPINIT failed: %s" % resp)
            return False

        # Required for https:// URLs on A7670; ignore if this firmware lacks it.
        await self._send_command("AT+HTTPSSL=1", timeout=3)
        await self._send_command('AT+HTTPPARA="CID",1', timeout=3)
        await self._send_command('AT+HTTPPARA="SSLCFG",1', timeout=3)
        await self._send_command('AT+HTTPPARA="REDIR",1', timeout=3)
        self.http_ready = True
        print("[A7670] HTTP ready")
        return True

    async def upload_data(self, data, url, input_timeout_ms=60000):
        """POST JSON (dict or str) to url. Returns (success, http_code, response)."""
        if not self.connected or not self.http_ready:
            return False, 0, "not connected"

        try:
            if isinstance(data, dict):
                data = json.dumps(data)
            body = data.encode() if isinstance(data, str) else data
            data_len = len(body)

            host = url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
            await self._send_command('AT+CSSLCFG="sni",1,"%s"' % host, timeout=3)

            ok, resp = await self._send_command(
                'AT+HTTPPARA="URL","%s"' % url, timeout=8
            )
            if not ok:
                return False, 0, "URL failed: %s" % resp

            await self._send_command(
                'AT+HTTPPARA="CONTENT","application/json"', timeout=3
            )

            ok, resp = await self._send_command(
                "AT+HTTPDATA=%d,%d" % (data_len, input_timeout_ms),
                wait_for="DOWNLOAD",
                timeout=10,
            )
            if not ok:
                return False, 0, "HTTPDATA failed: %s" % resp

            sent = await self._write_exact(body)
            print("[A7670] body sent %s/%s bytes" % (sent, data_len))
            if sent != data_len:
                return False, 0, "body short write %s/%s" % (sent, data_len)
            ok, resp = await self._read_response("OK", timeout_ms=20000)
            if not ok:
                return False, 0, "body write failed: %s" % resp

            # OK comes back immediately. +HTTPACTION URC follows after TLS+POST.
            ok, resp = await self._send_command(
                "AT+HTTPACTION=1", wait_for="OK", timeout=15
            )
            if not ok:
                return False, 0, "HTTPACTION failed: %s" % resp
            if "+HTTPACTION:" not in (resp or ""):
                print("[A7670] POST sent, waiting for +HTTPACTION...")
                ok, urc = await self._read_response("+HTTPACTION:", timeout_ms=120000)
                resp = (resp or "") + (urc or "")
            if "+HTTPACTION:" not in (resp or ""):
                return False, 0, "HTTPACTION failed: %s" % resp

            http_code = 0
            body_len = 0
            try:
                line = resp.split("+HTTPACTION:")[1].split("\n")[0].strip()
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    http_code = int(parts[1])
                    body_len = int(parts[2])
                elif len(parts) >= 2:
                    http_code = int(parts[1])
            except Exception:
                pass

            read_resp = ""
            if body_len > 0:
                _, read_resp = await self._send_command(
                    "AT+HTTPREAD=0,%d" % body_len, timeout=20
                )

            if http_code == 200:
                return True, http_code, read_resp
            return False, http_code, read_resp or resp

        except Exception as e:
            print("[A7670] upload exception: %s" % e)
            return False, 0, "upload exception: %s" % e
