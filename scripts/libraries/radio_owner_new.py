"""
radio_owner.py — sole owner of the SX1262.

Rules
  * Nothing outside this class may call the radio driver.
  * The DIO1 handler only sets a flag and a timestamp. No SPI in interrupt context.
  * The owner task wakes on DIO1, on a queued send, or at the next deadline.
  * Every callback (TX and RX) is an async function and runs as its own task.
    Nothing waits on a callback.

Client API (see README_radio_owner.md)
  t = radio_owner.enqueue(data, urgent=False, on_frame=None, on_done=None, timeout_ms=None)
  res = await radio_owner.send(data, ...)            # enqueue + wait
  t.done(), t.result(), await t.wait(timeout_ms), t.cancel(), t.add_done_callback(fn)
  radio_owner.on_receive("A", async_fn)              # or "*" for any other type
  radio_owner.request_reset(reason), radio_owner.ready(), radio_owner.get_stats()

Driver prerequisites (sx126x.py / sx1262.py) — see README
  * startReceive(): clearIrqStatus first, always reaches setRx
  * startReceiveCommon(): HEADER_VALID in the irq mask AND the DIO1 mask
  * getDeviceErrors() using | not &
  * getRssiInst(), chipMode(), selfTestCAD()
"""

import uasyncio as asyncio
import utime
from sx1262 import SX1262
from _sx126x import (
    ERR_NONE,
    SX126X_RX_TIMEOUT_INF,
    SX126X_CMD_GET_RSSI_INST,
    SX126X_CMD_GET_DEVICE_ERRORS,
    SX126X_IRQ_RX_DONE,
    SX126X_IRQ_TX_DONE,
    SX126X_IRQ_TIMEOUT,
    SX126X_IRQ_CRC_ERR,
    SX126X_IRQ_HEADER_ERR,
)

try:
    from _sx126x import SX126X_IRQ_HEADER_VALID
except ImportError:
    SX126X_IRQ_HEADER_VALID = 0x0010


# ----------------------------------------------------------------------
# Tunables
# ----------------------------------------------------------------------
MODE_RX = 5                      # chip mode reported by GetStatus while receiving
HEALTH_INTERVAL_MS = 60000       # idle self-test period
RESET_COOLDOWN_MS = 30000        # minimum time between radio resets
IDLE_RSSI_LIMIT = -60.0          # an idle channel this strong means a stuck reading

BURST_HOLD_MIN = 10              # lists longer than this hold RX between frames
BURST_LISTEN_EVERY_MS = 10000    # during a burst, open a listening window this often
BURST_LISTEN_MS = 1000           # ...for this long
BURST_FRAME_GAP_MS = 20          # gap between burst frames so the receiver keeps up

URGENT_TIMEOUT_MS = 3000         # default expiry for urgent single frames (ACK / W)
SINGLE_TIMEOUT_MS = 15000        # default expiry for other single frames
CALLER_GRACE_MS = 2000           # a waiter force-expires after deadline + this

CB_LIMIT = 32                    # max callback tasks running at once


# ThreadSafeFlag needs MicroPython >= 1.15; fall back to polling if absent.
if hasattr(asyncio, "ThreadSafeFlag"):
    _Flag = asyncio.ThreadSafeFlag
else:
    class _Flag:
        def __init__(self):
            self._set = False

        def set(self):
            self._set = True

        async def wait(self):
            while not self._set:
                await asyncio.sleep_ms(5)
            self._set = False


# ======================================================================
# TxTicket
# ======================================================================
class TxTicket:
    """
    Handle for a queued send. The client may wait on it, poll it, cancel it,
    register a completion callback, or simply drop it. Dropping it does not
    cancel the send.
    """
    __slots__ = ("frames", "single", "results", "_event", "on_frame", "idx",
                 "cancelled", "deadline", "_owner", "_callbacks")

    def __init__(self, owner, frames, single, on_frame, deadline):
        self._owner = owner
        self.frames = frames
        self.single = single
        self.results = []
        self._event = asyncio.Event()
        self.on_frame = on_frame
        self.idx = 0
        self.cancelled = False
        self.deadline = deadline
        self._callbacks = None

    def done(self):
        return self._event.is_set()

    def result(self):
        """(ok, err) for a single frame, list of them for a list, None while pending."""
        if not self._event.is_set():
            return None
        return self.results[0] if self.single else self.results

    def add_done_callback(self, fn):
        """
        fn is an async function fn(ticket). It starts as its own task once the
        send finishes, whatever the outcome (immediately if already finished).
        """
        if self._event.is_set():
            self._owner._spawn("done", fn, (self,), self)
        else:
            if self._callbacks is None:
                self._callbacks = []
            self._callbacks.append(fn)
        return self

    def cancel(self):
        """Drop frames not yet on air. A frame already on air finishes. False if finished."""
        if self._event.is_set():
            return False
        self.cancelled = True
        self._owner.flag.set()
        return True

    async def wait(self, timeout_ms=None):
        """
        Wait for the send to finish or expire and return result().
        timeout_ms limits only this wait: if it runs out, returns None and the
        send carries on. With no timeout_ms, returns by deadline + CALLER_GRACE_MS
        at the latest, even if the owner has stalled.
        """
        if self._event.is_set():
            return self.result()
        backstop = max(0, utime.ticks_diff(self.deadline, utime.ticks_ms())) + CALLER_GRACE_MS
        limit = backstop if timeout_ms is None else min(timeout_ms, backstop)
        try:
            await asyncio.wait_for_ms(self._event.wait(), limit)
        except asyncio.TimeoutError:
            if limit < backstop:
                return None                                  # client stopped waiting
            self._owner._expire(self, "expired (owner stalled)")
        return self.result()

    def complete(self, reason):
        """Owner-internal: finish the ticket, fill unsent frames with reason, fire callbacks."""
        if self._event.is_set():
            return
        while len(self.results) < len(self.frames):
            self.results.append((False, reason))
        self._event.set()
        cbs, self._callbacks = self._callbacks, None
        if cbs:
            for fn in cbs:
                self._owner._spawn("done", fn, (self,), self)


# ======================================================================
# RadioOwner
# ======================================================================
class RadioOwner:

    # SF7 / 125 kHz. Same modem settings on every device.
    LORA_FREQ = 868.0
    LORA_BW = 250
    LORA_SF = 7
    LORA_CR = 6
    LORA_POWER = 22
    LORA_PREAMBLE = 10
    LORA_SYNC_WORD = 0x2B

    def __init__(self, logger, rx_sink=None, on_fatal=None, rx_limit=64, tx_limit=32):
        """
        logger   : your logger module (info / warning / error / fatal)
        rx_sink  : optional list for packets with no registered handler
        on_fatal : optional async fn() called if the radio cannot be initialised
        rx_limit : max entries in rx_sink
        tx_limit : max queued tickets per priority queue
        """
        self.logger = logger
        self.rx_sink = rx_sink
        self.on_fatal = on_fatal
        self.rx_limit = rx_limit
        self.tx_limit = tx_limit

        self.node = None
        self.flag = _Flag()

        # --- transmit state ---
        self._urgent = []                # single-frame tickets: ACK / W
        self._normal = []                # everything else, including bursts
        self._cur = None                 # normal ticket being sent
        self._inflight = None            # (ticket, start_ms, limit_ms)

        self._rx_hold = False            # standby between frames during a long burst
        self._window_until = 0           # end of the current listening window (0 = none)
        self._last_window_ms = 0
        self._next_frame_ms = 0
        self._rx_busy_ms = 0             # header seen, RX_DONE not yet

        # --- recovery / health state ---
        self._reset_reason = None
        self._last_reset_ms = utime.ticks_add(utime.ticks_ms(), -RESET_COOLDOWN_MS)
        self._failed_inits = 0
        self._tx_timeout_run = 0
        self._health_fail = 0
        self._last_health_ms = utime.ticks_ms()
        self._prev_rx = 0
        self._prev_crc_strong = 0
        self.last_valid_rx_ms = utime.ticks_ms()

        self._irq_ms = 0
        self._max_latency_ms = 0

        # --- callbacks: each runs as its own task; nobody waits on it ---
        self._rx_handlers = {}           # type byte -> async fn(data, rssi, snr)
        self._rx_default = None          # handler for types without their own
        self._active_cb = 0              # callback tasks currently running
        self._max_active_cb = 0

        self.stats = {"tx": 0, "tx_fail": 0, "tx_timeout": 0, "tx_deferred": 0,
                      "tx_rejected": 0, "tx_cancelled": 0, "tx_expired": 0,
                      "windows": 0, "rx": 0, "rx_drop": 0, "rx_unhandled": 0,
                      "rx_handler_err": 0, "crc": 0, "crc_strong": 0,
                      "cb_err": 0, "cb_drop": 0, "rearm_fail": 0, "resets": 0}

    # ------------------------------------------------------------------
    # Interrupt handler: flag and timestamp only
    # ------------------------------------------------------------------
    def _isr(self, pin):
        self._irq_ms = utime.ticks_ms()
        self.flag.set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ready(self):
        return self.node is not None and self._reset_reason is None

    def enqueue(self, data, urgent=False, on_frame=None, on_done=None, timeout_ms=None):
        """
        Queue one frame or a list of frames and return a TxTicket immediately.

        data       : bytes / bytearray / memoryview, or a list of them
        urgent     : single frames only; jumps ahead of everything, including bursts
        on_frame   : optional async fn(index, ok) -> bool, started after each frame.
                     Returning False cancels the rest of the list.
        on_done    : optional async fn(ticket), started once when the send finishes.
        timeout_ms : expiry measured from this call. None = sensible default.
        """
        single = isinstance(data, (bytes, bytearray, memoryview))
        frames = [data] if single else list(data)
        is_urgent = urgent and single
        if timeout_ms is None:
            timeout_ms = self._default_timeout_ms(frames, is_urgent)
        t = TxTicket(self, frames, single, on_frame,
                     utime.ticks_add(utime.ticks_ms(), timeout_ms))
        if on_done is not None:
            t.add_done_callback(on_done)                     # registered before any rejection

        if not frames:
            t.complete("empty")
            return t
        if not self.ready():
            t.complete("radio not ready")
            return t
        q = self._urgent if is_urgent else self._normal
        if len(q) >= self.tx_limit:
            self.stats["tx_rejected"] += len(frames)
            t.complete("tx queue full")
            return t

        q.append(t)
        self.flag.set()
        return t

    async def send(self, data, urgent=False, on_frame=None, on_done=None, timeout_ms=None):
        """Enqueue and wait until the frame(s) left the antenna or expired."""
        return await self.enqueue(data, urgent, on_frame, on_done, timeout_ms).wait()

    def on_receive(self, msg_type, fn):
        """
        Register async fn(data, rssi, snr) for received frames of this message type.
        msg_type : "A" / b"A" / 65 for one type, or "*" for any type without
                   its own handler. fn=None removes the handler.
        Each packet starts its handler as a separate task.
        """
        if msg_type == "*" or msg_type == b"*":
            self._rx_default = fn
            return
        key = self._type_key(msg_type)
        if fn is None:
            self._rx_handlers.pop(key, None)
        else:
            self._rx_handlers[key] = fn

    def request_reset(self, reason):
        """Ask the owner to reset and re-initialise the radio. Rate-limited."""
        if self._reset_reason is not None:
            return False
        if utime.ticks_diff(utime.ticks_ms(), self._last_reset_ms) < RESET_COOLDOWN_MS:
            return False
        self._reset_reason = reason
        self.flag.set()
        return True

    def get_stats(self):
        s = dict(self.stats)
        s["queue"] = len(self._urgent) + len(self._normal) + (1 if self._cur else 0)
        s["active_cb"] = self._active_cb
        s["quiet_s"] = utime.ticks_diff(utime.ticks_ms(), self.last_valid_rx_ms) // 1000
        return s

    @staticmethod
    def _type_key(msg_type):
        if isinstance(msg_type, int):
            return msg_type
        if isinstance(msg_type, str):
            return ord(msg_type[0])
        return msg_type[0]                                   # bytes / bytearray

    # ------------------------------------------------------------------
    # Owner loop
    # ------------------------------------------------------------------
    async def run(self):
        await self._do_reset("radio initializing...")
        while True:
            try:
                await asyncio.sleep_ms(0)                   # let other tasks run

                if self._reset_reason:
                    await self._do_reset(self._reset_reason)
                    continue

                n = self.node
                if n is None:
                    await asyncio.sleep(10)
                    self._reset_reason = "radio absent, reinitializing..."
                    continue

                if n.irq.value():                           # pin read, no SPI
                    self._service_irq()
                    continue

                if self._inflight:
                    if self._check_tx_timeout():
                        continue
                elif self._step():
                    continue

                try:
                    await asyncio.wait_for_ms(self.flag.wait(), self._next_deadline_ms())
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                self.logger.error(f"[RADIO] owner loop error: {e}")
                await asyncio.sleep_ms(50)

    def _step(self):
        """One decision while nothing is on air. True = did something, loop again."""
        now = utime.ticks_ms()
        self._prune(now)
        receiving = self._rx_in_progress()

        # 1. Urgent replies go first, even inside a burst, but never over an arriving packet
        if self._urgent:
            if receiving:
                self.stats["tx_deferred"] += 1
                return False
            self._start_frame(self._urgent.pop(0))
            return True

        # 2. Current ticket cancelled, expired or already finished
        c = self._cur
        if c is not None and (c.done() or c.cancelled or self._expired(c, now)):
            if c.cancelled:
                self._cancel(c)
            else:
                self._expire(c)
            self._end_cur()
            self._rearm()
            return True

        # 3. Listening window inside a burst
        if self._window_until:
            if utime.ticks_diff(now, self._window_until) < 0 or receiving:
                return False                                # keep listening
            self._window_until = 0
            self._last_window_ms = now
            if self._cur is not None and len(self._cur.frames) > BURST_HOLD_MIN:
                self._hold_rx()
            return True

        # 4. Pick up the next ticket
        if self._cur is None and self._normal:
            if receiving:
                self.stats["tx_deferred"] += 1
                return False
            self._cur = self._normal.pop(0)
            self._last_window_ms = now
            self._next_frame_ms = now
            if len(self._cur.frames) > BURST_HOLD_MIN:
                self._hold_rx()
            return True

        # 5. Continue the current ticket
        if self._cur is not None:
            if self._rx_hold and \
                    utime.ticks_diff(now, self._last_window_ms) >= BURST_LISTEN_EVERY_MS:
                self._release_rx()
                self._window_until = utime.ticks_add(now, BURST_LISTEN_MS)
                self.stats["windows"] += 1
                return True
            if utime.ticks_diff(self._next_frame_ms, now) > 0:
                return False                                # inter-frame gap
            if receiving:
                self.stats["tx_deferred"] += 1
                return False
            self._start_frame(self._cur)
            return True

        # 6. Idle: periodic health check
        if not self._rx_busy_ms and self._health_due():
            self._last_health_ms = now
            self._health_check()
            return True

        return False

    # ------------------------------------------------------------------
    # Expiry and cancellation
    # ------------------------------------------------------------------
    def _default_timeout_ms(self, frames, urgent):
        if not frames:
            return 0
        if len(frames) == 1:
            return URGENT_TIMEOUT_MS if urgent else SINGLE_TIMEOUT_MS
        per_frame = self.toa_ms(len(frames[0])) + BURST_FRAME_GAP_MS
        est = len(frames) * per_frame
        est += (est // BURST_LISTEN_EVERY_MS) * BURST_LISTEN_MS
        return est * 2 + 10000

    def _expired(self, t, now):
        return utime.ticks_diff(now, t.deadline) >= 0

    def _expire(self, t, reason="expired"):
        if not t.done():
            self.stats["tx_expired"] += len(t.frames) - len(t.results)
            t.complete(reason)

    def _cancel(self, t):
        if not t.done():
            self.stats["tx_cancelled"] += len(t.frames) - len(t.results)
            t.complete("cancelled")

    def _prune(self, now):
        """Drop queued tickets that finished, were cancelled or expired."""
        for q in (self._urgent, self._normal):
            i = 0
            while i < len(q):
                t = q[i]
                if t.done():
                    q.pop(i)
                elif t.cancelled:
                    q.pop(i)
                    self._cancel(t)
                elif self._expired(t, now):
                    q.pop(i)
                    self._expire(t)
                else:
                    i += 1

    # ------------------------------------------------------------------
    # RX hold (private)
    # ------------------------------------------------------------------
    def _hold_rx(self):
        """Stay in standby between frames. Takes effect at the next TX_DONE."""
        self._rx_hold = True

    def _release_rx(self):
        """Back to listening immediately. Call only when nothing is on air."""
        self._rx_hold = False
        self._rearm()

    def _end_cur(self):
        self._cur = None
        self._window_until = 0
        self._rx_hold = False

    # ------------------------------------------------------------------
    # Transmit
    # ------------------------------------------------------------------
    def _start_frame(self, t):
        n = self.node
        data = t.frames[t.idx]
        try:
            n.standby()                     # nothing can land in the buffer while loading
            _, st = n.send(data)
        except Exception as e:
            st = str(e)
        if st != ERR_NONE:
            self.stats["tx_fail"] += 1
            self._frame_done(t, False, f"send status {st}")
            return
        self._rx_busy_ms = 0
        limit = self.toa_ms(len(data)) * 3 // 2 + 50
        self._inflight = (t, utime.ticks_ms(), limit)

    def _frame_done(self, t, ok, err):
        """Record one frame's outcome, advance the ticket, and re-arm."""
        if t.done():                                # expired or force-expired while on air
            if t is self._cur:
                self._end_cur()
            self._rearm()
            return

        t.results.append((ok, err))
        if t.on_frame is not None:
            self._spawn("frame", t.on_frame, (t.idx, ok), t)
        t.idx += 1

        if t.cancelled or t.idx >= len(t.frames):
            if t.cancelled:
                self._cancel(t)
            else:
                t.complete("done")                  # nothing left to fill
            if t is self._cur:
                self._end_cur()
        elif t is self._cur:
            self._next_frame_ms = utime.ticks_add(utime.ticks_ms(), BURST_FRAME_GAP_MS)

        self._rearm()                               # respects _rx_hold

    def _check_tx_timeout(self):
        t, t0, limit = self._inflight
        if utime.ticks_diff(utime.ticks_ms(), t0) <= limit:
            return False
        self._inflight = None
        self.stats["tx_timeout"] += 1
        self._tx_timeout_run += 1
        self._frame_done(t, False, "TX_DONE timeout")
        if self._tx_timeout_run >= 5:
            self._tx_timeout_run = 0
            self.request_reset("5 consecutive TX_DONE timeouts, reinitializing...")
        return True

    # ------------------------------------------------------------------
    # Receive
    # ------------------------------------------------------------------
    def _rearm(self):
        """Clear IRQs and return to RX, unless holding for a burst."""
        self._rx_busy_ms = 0
        n = self.node
        try:
            n.clearIrqStatus()
            if self._rx_hold:
                return
            if n.startReceive() != ERR_NONE:
                self.stats["rearm_fail"] += 1
        except Exception:
            self.stats["rearm_fail"] += 1

    def _rx_in_progress(self):
        """True between a header interrupt and RX_DONE. No SPI."""
        if not self._rx_busy_ms:
            return False
        if utime.ticks_diff(utime.ticks_ms(), self._rx_busy_ms) < self.toa_ms(255) + 50:
            return True
        self._rx_busy_ms = 0                # RX_DONE never came; don't block forever
        return False

    def _service_irq(self):
        lat = utime.ticks_diff(utime.ticks_ms(), self._irq_ms)
        if lat > self._max_latency_ms:
            self._max_latency_ms = lat

        n = self.node
        try:
            ev = n.getIrqStatus()
        except Exception:
            ev = 0

        # Our frame finished
        if (ev & SX126X_IRQ_TX_DONE) and self._inflight:
            t = self._inflight[0]
            self._inflight = None
            self.stats["tx"] += 1
            self._tx_timeout_run = 0
            self._frame_done(t, True, None)
            return

        # Corrupt packet: skip the buffer read
        if ev & (SX126X_IRQ_CRC_ERR | SX126X_IRQ_HEADER_ERR):
            self.stats["crc"] += 1
            try:
                snr = n.getSNR()
                if snr is not None and snr > 0:     # strong signal failing CRC = local fault
                    self.stats["crc_strong"] += 1
            except Exception:
                pass
            self._rearm()
            return

        # Good packet: hand it straight to its handler
        if ev & SX126X_IRQ_RX_DONE:
            self._rx_busy_ms = 0
            try:
                data, st = n.recv(len=0)            # reads buffer and re-arms RX
                rssi, snr = n.getLastRSSI(), n.getLastSNR()
            except Exception as e:
                self.logger.error(f"[RADIO] read failed: {e}")
                self._rearm()
                return
            if st == ERR_NONE and data:
                self.stats["rx"] += 1
                self.last_valid_rx_ms = utime.ticks_ms()
                self._deliver(data, rssi, snr)
            return

        # Packet starting to arrive: hold transmits until RX_DONE
        if ev & SX126X_IRQ_HEADER_VALID:
            self._rx_busy_ms = utime.ticks_ms()
            try:
                n.clearIrqStatus(SX126X_IRQ_HEADER_VALID)   # drop DIO1 so RX_DONE gives an edge
            except Exception:
                pass
            return

        # Timeout, stale flag or unknown: clear and listen
        self._rearm()

    # ------------------------------------------------------------------
    # Callbacks: each one runs as its own task, nothing waits on it
    # ------------------------------------------------------------------
    def _deliver(self, data, rssi, snr):
        fn = self._rx_handlers.get(data[0], self._rx_default)
        if fn is None:
            if self.rx_sink is not None and len(self.rx_sink) < self.rx_limit:
                self.rx_sink.append((data, rssi, snr))
            else:
                self.stats["rx_unhandled"] += 1
            return
        if not self._spawn("rx", fn, (data, rssi, snr)):
            self.stats["rx_drop"] += 1

    def _spawn(self, kind, fn, args, ticket=None):
        """Start fn(*args) as its own task. Returns False if dropped at the cap."""
        if kind != "done" and self._active_cb >= CB_LIMIT:
            if kind == "frame":
                self.stats["cb_drop"] += 1
            return False
        self._active_cb += 1
        if self._active_cb > self._max_active_cb:
            self._max_active_cb = self._active_cb
        asyncio.create_task(self._run_cb(kind, fn, args, ticket))
        return True

    async def _run_cb(self, kind, fn, args, ticket):
        """Wrapper inside the callback's own task: error handling and cancel-on-False."""
        try:
            r = await fn(*args)
            if kind == "frame" and r is False and ticket is not None:
                ticket.cancel()
        except Exception as e:
            if kind == "rx":
                self.stats["rx_handler_err"] += 1
                try:
                    typ = chr(args[0][0])
                except Exception:
                    typ = "?"
                self.logger.error(f"[RADIO] rx handler error for type {typ}: {e}")
            else:
                self.stats["cb_err"] += 1
                self.logger.error(f"[RADIO] {kind} callback error: {e}")
        finally:
            self._active_cb -= 1

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    def _health_due(self):
        return utime.ticks_diff(utime.ticks_ms(), self._last_health_ms) > HEALTH_INTERVAL_MS

    def _health_check(self):
        n = self.node
        problems = []
        mode = None
        rssi = None
        try:
            mode = n.chipMode()
            if mode != MODE_RX:
                problems.append(f"mode {mode}")
                self._rearm()

            ok, detail = n.selfTestCAD()            # ends back in RX
            self._rx_busy_ms = 0
            if not ok:
                problems.append("cad " + detail)

            rssi = n.getRssiInst()
            if rssi >= IDLE_RSSI_LIMIT:
                problems.append(f"idle rssi {rssi}")

            errs = n.getDeviceErrors()
            if errs:
                problems.append(f"dev_err 0x{errs:04X}")
                n.clearDeviceErrors()
        except Exception as e:
            problems.append(f"exception {e}")

        new_rx = self.stats["rx"] - self._prev_rx
        new_strong = self.stats["crc_strong"] - self._prev_crc_strong
        self._prev_rx = self.stats["rx"]
        self._prev_crc_strong = self.stats["crc_strong"]
        if new_strong >= 10 and new_rx == 0:
            problems.append(f"{new_strong} strong CRC errors, no valid RX")

        self.logger.info(f"[RADIO] health mode={mode} rssi={rssi} "
                      f"max_irq_latency={self._max_latency_ms}ms "
                      f"max_callbacks={self._max_active_cb} {self.get_stats()}")
        self._max_latency_ms = 0
        self._max_active_cb = self._active_cb

        if problems:
            self._health_fail += 1
            self.logger.error(f"[RADIO] health fail #{self._health_fail}: {', '.join(problems)}")
            if self._health_fail >= 2:
                self._health_fail = 0
                self.request_reset("health: " + ", ".join(problems) + ", reinitializing...")
        else:
            self._health_fail = 0

    def _next_deadline_ms(self):
        now = utime.ticks_ms()
        if self._inflight:
            _, t0, limit = self._inflight
            return max(1, limit - utime.ticks_diff(now, t0))

        cands = [HEALTH_INTERVAL_MS - utime.ticks_diff(now, self._last_health_ms)]
        if self._window_until:
            cands.append(utime.ticks_diff(self._window_until, now))
        if self._cur is not None:
            cands.append(utime.ticks_diff(self._next_frame_ms, now))
            cands.append(utime.ticks_diff(self._cur.deadline, now))
            if self._rx_hold:
                cands.append(BURST_LISTEN_EVERY_MS - utime.ticks_diff(now, self._last_window_ms))
        for q in (self._urgent, self._normal):
            for t in q:
                cands.append(utime.ticks_diff(t.deadline, now))     # wake to expire it
        if self._rx_busy_ms and (self._urgent or self._normal or self._cur):
            cands.append(self.toa_ms(255) + 50 - utime.ticks_diff(now, self._rx_busy_ms))
        return max(1, min(cands))

    # ------------------------------------------------------------------
    # Reset / init
    # ------------------------------------------------------------------
    def toa_ms(self, nbytes):
        """Airtime in ms. SF7, 125 kHz, CR 4/6, preamble 10. No SPI."""
        t_sym_us = (1000000 << self.LORA_SF) // (self.LORA_BW * 1000)
        t_pre_us = (self.LORA_PREAMBLE * 4 + 17) * t_sym_us // 4
        num = 8 * nbytes - 4 * self.LORA_SF + 28 + 16
        den = 4 * self.LORA_SF
        n_payload = 8 if num <= 0 else 8 + ((num + den - 1) // den) * self.LORA_CR
        return max(1, (t_pre_us + n_payload * t_sym_us + 999) // 1000)

    def _init_radio(self):
        """Build a configured SX1262. Does not attach a DIO1 handler."""
        node = SX1262(
            spi_bus=1, clk="P2", mosi="P0", miso="P1", cs="P3",
            irq="P13", rst="P6", gpio="P7", txen="P9", rxen="P10",
            spi_baudrate=2000000, spi_polarity=0, spi_phase=0,
        )
        status = node.begin(
            freq=self.LORA_FREQ, bw=self.LORA_BW, sf=self.LORA_SF, cr=self.LORA_CR,
            syncWord=self.LORA_SYNC_WORD, power=self.LORA_POWER, currentLimit=140.0,
            preambleLength=self.LORA_PREAMBLE, implicit=False, crcOn=True,
            tcxoVoltage=1.6, useRegulatorLDO=False, blocking=False,
        )
        if status != ERR_NONE:
            self.logger.error("[RADIO] SX1262 begin failed, status={}".format(status))
            return None

        # startReceiveCommon() does not put HEADER_VALID on DIO1. SetDioIrqParams
        # is only valid in standby, so re-arm from standby after the driver enters RX.
        irq_mask = (
            SX126X_IRQ_RX_DONE | SX126X_IRQ_TIMEOUT | SX126X_IRQ_CRC_ERR
            | SX126X_IRQ_HEADER_ERR | SX126X_IRQ_HEADER_VALID
        )
        dio1 = SX126X_IRQ_RX_DONE | SX126X_IRQ_HEADER_VALID
        orig_start = node.startReceive
        orig_recv = node.recv

        def listen(timeout=SX126X_RX_TIMEOUT_INF):
            node.standby()
            node.setDioIrqParams(irq_mask, dio1)
            return node.setRx(timeout)

        def start_receive(timeout=SX126X_RX_TIMEOUT_INF):
            st = orig_start(timeout)
            return st if st != ERR_NONE else listen(timeout)

        def recv(len=0, timeout_en=False, timeout_ms=0):
            out = orig_recv(len=len, timeout_en=timeout_en, timeout_ms=timeout_ms)
            listen()
            return out

        def rssi_inst():
            raw = bytearray(1)
            node.SPIreadCommand([SX126X_CMD_GET_RSSI_INST], 1, memoryview(raw), 1)
            return -raw[0] / 2.0

        def device_errors():
            raw = bytearray(2)
            node.SPIreadCommand([SX126X_CMD_GET_DEVICE_ERRORS], 1, memoryview(raw), 2)
            return (raw[0] << 8) | raw[1]

        node.startReceive = start_receive
        node.recv = recv
        node.chipMode = lambda: (node.getStatus() >> 4) & 7
        node.getRssiInst = rssi_inst
        node.getDeviceErrors = device_errors
        node.selfTestCAD = lambda: (True, "ok")
        return node

    async def _do_reset(self, reason):
        self.logger.info(f"[RADIO] : {reason}")
        self.stats["resets"] += 1
        self._last_reset_ms = utime.ticks_ms()

        # Finish everything queued or in flight so waiters and callbacks fire
        pending = []
        if self._inflight:
            pending.append(self._inflight[0])
            self._inflight = None
        if self._cur is not None:
            pending.append(self._cur)
            self._cur = None
        pending.extend(self._urgent)
        pending.extend(self._normal)
        self._urgent = []
        self._normal = []
        for t in pending:
            t.complete("radio reset")

        self._rx_hold = False
        self._window_until = 0
        self._rx_busy_ms = 0
        self.node = None

        node = None
        for _ in range(3):
            try:
                node = self._init_radio()
            except Exception as e:
                self.logger.error(f"[RADIO] ✘✘✘ init failed: {e}")
                node = None
            if node is not None:
                break
            await asyncio.sleep(2)

        self._reset_reason = None

        if node is None:
            self._failed_inits += 1
            self.logger.fatal(f"[RADIO] ✘✘✘ init failed (x{self._failed_inits})")
            if self._failed_inits >= 3 and self.on_fatal:
                await self.on_fatal()
            return

        self._failed_inits = 0
        self.node = node
        node.setDio1Action(self._isr)
        self._rearm()
        self.last_valid_rx_ms = utime.ticks_ms()
        self.logger.info("[RADIO] ✔✔✔ initialized")


# -----------------------------------▼▼▼▼▼-----------------------------------
# ------------------------------- TESTING CODE ------------------------------
# ---------------------------------------------------------------------------
# Run on the device:  radio_owner_2.py
#
# enqueue() returns at once. TX_DONE (DIO1) finishes the ticket later and
# starts on_done. Received bytes go to the async handler registered for
# data[0], or to "*" when that type has no handler.

def _fmt_rssi_snr(rssi, snr):
    rssi_s = "n/a" if rssi is None else "{:.1f} dBm".format(rssi)
    snr_s = "n/a" if snr is None else "{:.1f} dB".format(snr)
    return "RSSI {} (strength), SNR {} (clarity)".format(rssi_s, snr_s)


def _recv_log_line(databytes, rssi, snr):
    """Same receive line as radio_owner.py: ⬇ RECV or ⬇ BCAST."""
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


async def _standalone_test():
    """Send 100 X frames one at a time, then five X frames in one enqueue, then listen.

    enqueue() returns a ticket at once. await ticket.wait() returns when
    TX_DONE has finished every frame, or when the wait limit is reached.
    Each received type runs its own async handler.
    """
    import logger
    from config import get_machine_id
    from clock_utils import get_epoch_sec, get_uptime_ms
    from message_codec import create_msg_uid, encode_x_message

    my_addr = get_machine_id()
    if my_addr is None:
        logger.error("No machine id found for this device, exiting...")
        return

    got = {"n": 0}
    tx_ok = {"n": 0}

    async def on_x(data, rssi, snr):
        got["n"] += 1
        logger.info("[X] {}".format(_recv_log_line(data, rssi, snr)))

    async def on_y(data, rssi, snr):
        got["n"] += 1
        logger.info("[Y] {}".format(_recv_log_line(data, rssi, snr)))

    async def on_other(data, rssi, snr):
        got["n"] += 1
        typ = "?"
        if data:
            try:
                typ = chr(data[0])
            except Exception:
                typ = str(data[0])
        logger.info("[{}] {}".format(typ, _recv_log_line(data, rssi, snr)))

    async def on_tx_done(ticket):
        res = ticket.result()
        rows = res if isinstance(res, list) else ([] if res is None else [res])
        if not rows:
            logger.error("[TX_DONE] no result")
            return
        for i, (ok, err) in enumerate(rows):
            uid = ticket.frames[i][:7] if i < len(ticket.frames) else b"?"
            if ok:
                tx_ok["n"] += 1
                logger.info("[TX_DONE] MSG_UID = {}".format(uid))
            else:
                logger.error("[TX_DONE] failed: {}, MSG_UID = {}".format(err, uid))

    radio = RadioOwner(logger)
    radio.on_receive("X", on_x)
    radio.on_receive("Y", on_y)
    radio.on_receive("*", on_other)
    asyncio.create_task(radio.run())

    ready_deadline = utime.ticks_add(utime.ticks_ms(), 20000)
    while not radio.ready():
        if utime.ticks_diff(ready_deadline, utime.ticks_ms()) <= 0:
            logger.error("[RADIO] not ready, aborting test")
            return
        await asyncio.sleep_ms(100)
    logger.info("[RADIO] test starting, addr={}".format(my_addr))

    # ---- SINGLE SEND -----
    # One enqueue() per frame. result is (ok, err).
    logger.info("---- starting single send ----")
    SINGLE_COUNT = 100
    SINGLE_WAIT_MS = 1000
    dest = 65535
    single_ok = 0
    t0 = get_uptime_ms()
    for _ in range(SINGLE_COUNT):
        msgbytes = encode_x_message(my_addr, get_epoch_sec())
        if msgbytes is None:
            logger.error("failed to encode X message")
            continue
        msg_uid, crc = create_msg_uid(my_addr, my_addr, dest, msgbytes)
        payload = msg_uid + crc + b";" + msgbytes
        masked = min(10, max(1, (len(payload) + 20) // 21))
        logger.info("[⋙ QUEUED to {}] [{}] {} bytes, MSG_UID = {}".format(
            dest, "*" * masked, len(payload), msg_uid
        ))
        logger.info("queued 1 X frame, waiting up to {} ms".format(SINGLE_WAIT_MS))
        ticket = radio.enqueue(payload, on_done=on_tx_done, timeout_ms=SINGLE_WAIT_MS)
        result = await ticket.wait(SINGLE_WAIT_MS)
        if result is None:
            logger.error("✔✔✔ single send wait ended after {} ms, TX still in progress".format(
                SINGLE_WAIT_MS
            ))
        elif result[0]:
            single_ok += 1
            logger.info("⮕ single send done, MSG_UID = {}".format(msg_uid))
        else:
            logger.error("✘ single send failed: {}, MSG_UID = {}".format(result[1], msg_uid))
    elapsed = utime.ticks_diff(get_uptime_ms(), t0)
    logger.info("⮕ {} X single sends took {} ms, ok {}".format(
        SINGLE_COUNT, elapsed, single_ok
    ))

    # ---- BULK SEND -----
    # One enqueue of a list. wait() returns when every frame has left the antenna.
    logger.info("---- starting bulk send ----")
    SEND_COUNT = 100
    dest = 65535
    frames = []
    for _ in range(SEND_COUNT):
        msgbytes = encode_x_message(my_addr, get_epoch_sec())
        if msgbytes is None:
            logger.error("failed to encode X message")
            continue
        msg_uid, crc = create_msg_uid(my_addr, my_addr, dest, msgbytes)
        frames.append(msg_uid + crc + b";" + msgbytes)

    logger.info("⋙ queued {} X frames in one enqueue".format(len(frames)))
    t0 = get_uptime_ms()
    ticket = radio.enqueue(frames)
    result = await ticket.wait()
    elapsed = utime.ticks_diff(get_uptime_ms(), t0)
    rows = result if isinstance(result, list) else ticket.results
    ok_n = sum(1 for ok, _err in rows if ok)
    fail_n = len(frames) - ok_n
    logger.info("⮕ BULK RESULT queued {} ok {} failed {} took {} ms".format(
        len(frames), ok_n, fail_n, elapsed
    ))

    # ---- LISTENING CODE -----
    LISTEN_MS = 4 * 60 * 1000
    logger.info("listening for 4 minutes")
    await asyncio.sleep_ms(LISTEN_MS)
    logger.info("✔✔✔ receive test done, received {} frames".format(got["n"]))
    logger.info("[RADIO] stats {}".format(radio.get_stats()))


if __name__ == "__main__":
    asyncio.run(_standalone_test())
