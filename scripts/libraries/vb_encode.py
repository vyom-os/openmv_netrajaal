# vb_encode.py -- MicroPython / OpenMV
#
#   send_full, payload, pct_change = encode_image(cur_jpeg, ref_jpeg)
#
# Capture stays outside. Both arguments are JPEG bytes.
#
#   cur_jpeg   JPEG bytes of the frame just captured
#   ref_jpeg   JPEG bytes the receiver currently holds, or None
#
#   returns (True,  cur_jpeg)              send the original JPEG, untouched
#           (False, <packed crop record>)  send this as a partial update
#
# Nothing is remembered between calls. Pass the receiver's JPEG again next
# time. pct_change, 0..100, is returned with the payload.
#
# Frames are width x height (1280x720). Both frames are compared as 1/4
# grayscale thumbnails. A full RGB image is needed only when crops are sent,
# and that decode goes into the sensor frame buffer so the heap never has to
# hold a second 1280x720 RGB565 (about 1.8MB).
#
# WIRE FORMAT of the returned payload:
#     full:     raw JPEG bytes
#     partial:  <B ntiles> then ntiles x ( <HHHH x,y,w,h> <I jpeg_len> <jpeg> )
# Coordinates are full-resolution, top-left origin. The receiver reconstructs
# by pasting each crop onto its copy of the reference.
#
# Thresholds here are the CONSERVATIVE set: adaptive between 45 and 60. Higher
# ceilings compress better on paper but were never validated for whether a
# subject survives, and on test data the conservative setting was cheaper
# anyway because small frequent updates beat rare expensive full refreshes.

import gc
import struct
import sys

import image

# --- geometry ----------------------------------------------------------
width = 1280
height = 720
DS = 4                    # detection runs at 1/4 linear scale
TILE = 32                 # full-resolution tile size for change decisions

# --- detection ---------------------------------------------------------
THRESH_MIN = 45           # validated floor
THRESH_MAX = 60           # validated ceiling; do not raise without retesting
NOISE_K = 7               # threshold = NOISE_K x median-ish noise floor
DEAD_NOISE = 35           # noise floor above this: reference is stale, go full
MERGE_MARGIN = 24
MIN_AREA = 256
ERODE_THRESH = 4          # keep a pixel if 5+ of its 3x3 neighbours are set.
                          # OpenMV's default for erode(1) is 8 -- all nine --
                          # which is full binary erosion and deletes any
                          # feature a few pixels wide at 1/4 scale. Measured:
                          # a thin shaft went from 49% covered at 8 to 95% at
                          # 4, costing 3 points of payload on vegetation
                          # frames, where dropping erode entirely cost 16.
ALIGN = 8
MAX_TILES = 24
STILL_FRAC = 0.002        # below this, nothing meaningful moved

# --- coding ------------------------------------------------------------
Q_TILE = 50
CROP_OVERHEAD = 290       # measured JPEG header bytes per crop
FULL_RECT_FRAC = 0.85     # crops covering ~everything are a bad full frame
MARGIN = 0.60             # send crops only if est < MARGIN x full-frame cost

EXCLUDE = []              # full-res (x, y, w, h) regions to ignore

# --- learned / persistent ----------------------------------------------
_bpp = 0.07               # bytes per changed pixel, learned as we go
_full_bytes = 40000       # last observed full-frame size


def get_state():
    """Persist this across deep sleep alongside your reference image."""
    return (_bpp, _full_bytes)


def set_state(state):
    global _bpp, _full_bytes
    _bpp, _full_bytes = state


def _val(obj, name):
    """Method on some firmware, plain value on others (blob.x is an int here)."""
    v = getattr(obj, name)
    return v() if callable(v) else v


def stat(stats, name):
    """Read a statistic across firmware versions.

    Some builds expose get_statistics().mean as a method, others as a plain
    attribute. Calling the wrong one raises TypeError rather than returning
    something wrong, so probe and adapt.
    """
    v = getattr(stats, name)
    try:
        return v()
    except (TypeError, ValueError):
        return v


def fill_rect(img, x, y, w, h, level=0):
    """Fill a rectangle, working around draw_rectangle signature drift.

    Newer firmware wants colour as a tuple even for grayscale images, and
    some builds want the rect itself as a tuple.
    """
    attempts = (
        ((x, y, w, h), {"color": (level, level, level), "fill": True}),
        (((x, y, w, h),), {"color": (level, level, level), "fill": True}),
        ((x, y, w, h), {"color": level, "fill": True}),
        (((x, y, w, h),), {"color": level, "fill": True}),
    )
    for args, kw in attempts:
        try:
            img.draw_rectangle(*args, **kw)
            return True
        except (TypeError, ValueError):
            continue
    return False


def _erode(img, size=1, threshold=ERODE_THRESH):
    """erode() with an explicit threshold, falling back if unsupported."""
    try:
        img.erode(size, threshold=threshold)
        return True
    except TypeError:
        pass
    try:
        img.erode(size, threshold)
        return True
    except TypeError:
        pass
    img.erode(size)            # default threshold: aggressive, but better
    return False               # than skipping the step entirely


def _snap_rect(x, y, w, h, W, H):
    x0 = max(0, (x // ALIGN) * ALIGN)
    y0 = max(0, (y // ALIGN) * ALIGN)
    x1 = min(W, ((x + w + ALIGN - 1) // ALIGN) * ALIGN)
    y1 = min(H, ((y + h + ALIGN - 1) // ALIGN) * ALIGN)
    return (x0, y0, max(ALIGN, x1 - x0), max(ALIGN, y1 - y0))


def _merge(rects, bpp):
    """Fuse rects when the wasted area costs less than another JPEG header."""
    rects = list(rects)
    again = True
    while again and len(rects) > 1:
        again = False
        for i in range(len(rects)):
            for j in range(i + 1, len(rects)):
                ax, ay, aw, ah = rects[i]
                bx, by, bw, bh = rects[j]
                ux, uy = min(ax, bx), min(ay, by)
                ux2, uy2 = max(ax + aw, bx + bw), max(ay + ah, by + bh)
                waste = (ux2 - ux) * (uy2 - uy) - aw * ah - bw * bh
                if waste * bpp < CROP_OVERHEAD:
                    rects[i] = (ux, uy, ux2 - ux, uy2 - uy)
                    rects.pop(j)
                    again = True
                    break
            if again:
                break
    return rects


def _jpeg_bytes(img, quality):
    gc.collect()
    j = img.compress(quality=quality)
    # memoryview slice avoids a second full-size copy of the JPEG
    return bytes(memoryview(j.bytearray())[:j.size()])


def _open_jpeg(data, w, h):
    """JPEG bytes as an image wrapper. The wrapper borrows the caller's buffer."""
    try:
        return image.Image(w, h, image.JPEG, buffer=data)
    except TypeError:
        return image.Image(w, h, image.JPEG, buffer=bytearray(data))


def _thumb(data):
    """1/4 grayscale used for the diff. Does not decode a full RGB frame."""
    jpg = _open_jpeg(data, width, height)
    try:
        try:
            g = jpg.to_grayscale(copy=True, x_scale=1.0 / DS, y_scale=1.0 / DS)
        except TypeError:
            g = jpg.to_grayscale(x_scale=1.0 / DS, y_scale=1.0 / DS)
        if g is None or g is jpg:
            raise ValueError("thumbnail")
        if _val(g, "width") != width // DS or _val(g, "height") != height // DS:
            raise ValueError("thumbnail")
        return g
    finally:
        del jpg
        gc.collect()


def _full_frame(data):
    """Decode a full JPEG straight into the camera frame buffer.

    to_rgb565(copy=True) asks the heap for 1280*720*2 bytes and fails here.
    copy_to_fb puts that same image in the buffer the sensor already owns.
    alloc_extra_fb is deprecated on this firmware and is not used.
    """
    gc.collect()
    jpg = _open_jpeg(data, width, height)
    im = jpg.to_rgb565(copy_to_fb=True)
    if im is None:
        im = jpg
    if im is not jpg:
        del jpg
    gc.collect()
    if im is None or _val(im, "width") != width or _val(im, "height") != height:
        raise MemoryError("frame buffer decode failed")
    return im


def _crop_rgb(data, w, h):
    """A crop is small enough to decode on the heap."""
    jpg = _open_jpeg(data, w, h)
    try:
        try:
            im = jpg.to_rgb565(copy=True)
        except TypeError:
            im = jpg.to_rgb565()
        if im is None or _val(im, "width") != w or _val(im, "height") != h:
            raise ValueError("crop is not %dx%d" % (w, h))
        return im
    finally:
        del jpg
        gc.collect()


def _plan(small, work):
    """Decide full / empty / crops. `small` is destroyed (it becomes the mask).

    Returns (kind, rects, changed_pixels, pct_change).
    kind is 'full', 'empty', or 'crops'. pct_change is 0..100.
    """

    # Cancel global light drift before differencing, or a passing cloud reads
    # as scene-wide motion.
    delta = int(stat(small.get_statistics(), "mean")
                - stat(work.get_statistics(), "mean"))
    if delta > 0:
        small.sub(delta)
    elif delta < 0:
        small.add(-delta)

    small.difference(work)          # small now holds |cur - ref|

    # The noise floor separates "grainy frame" from "reference is meaningless".
    noise = stat(small.get_statistics(), "median")
    if noise > DEAD_NOISE:
        return "full", None, 0, 100

    thresh = int(NOISE_K * noise)
    if thresh < THRESH_MIN:
        thresh = THRESH_MIN
    elif thresh > THRESH_MAX:
        thresh = THRESH_MAX

    small.binary([(thresh, 255)])
    _erode(small)
    small.dilate(2)
    for (ex, ey, ew, eh) in EXCLUDE:
        fill_rect(small, ex // DS, ey // DS, ew // DS, eh // DS, 0)

    rects = []
    for b in small.find_blobs([(1, 255)], merge=True, margin=MERGE_MARGIN // DS,
                              pixels_threshold=MIN_AREA // (DS * DS)):
        x, y, w, h = _val(b, "rect")
        rects.append(_snap_rect(x * DS, y * DS, w * DS, h * DS, width, height))
    rects.sort(key=lambda r: r[2] * r[3], reverse=True)

    if not rects:
        return "empty", None, 0, 0

    # Merge BEFORE truncating. The other order discards a small fragment that
    # would have fused into a neighbour, so real change vanishes silently.
    rects = _merge(rects, _bpp)[:MAX_TILES]
    changed = 0
    for r in rects:
        changed += r[2] * r[3]
    frac = float(changed) / (width * height)
    pct = int(frac * 100)

    if frac < STILL_FRAC:
        return "empty", None, 0, 0

    # A crop covering nearly everything is just a worse full frame.
    if frac > FULL_RECT_FRAC:
        return "full", None, changed, pct

    est = int(changed * _bpp) + (CROP_OVERHEAD + 12) * len(rects) + 1
    if est >= _full_bytes * MARGIN:
        return "full", None, changed, pct

    return "crops", rects, changed, pct


def _pack_crops(cur, rects, changed):
    """JPEG-compress each crop into one buffer, freeing each crop as we go."""
    global _bpp
    out = bytearray()
    out.append(len(rects))
    total = 0
    for (x, y, w, h) in rects:
        crop = cur.copy(roi=(x, y, w, h))
        jb = _jpeg_bytes(crop, Q_TILE)    # compress consumes crop
        del crop
        out.extend(struct.pack("<HHHHI", x, y, w, h, len(jb)))
        out.extend(jb)
        total += len(jb)
        del jb
        gc.collect()

    # Learn PIXEL cost only. Including headers here double-counts them in the
    # estimate above and forces premature full frames.
    pix = total - CROP_OVERHEAD * len(rects)
    if pix > 0 and changed:
        _bpp = 0.4 * (float(pix) / changed) + 0.6 * _bpp
    return False, bytes(out)


def encode_image(cur_jpeg, ref_jpeg=None):
    """Diff two JPEG byte strings. See the module docstring.

    Returns (send_full, payload, pct_change). pct_change is 0..100.
    cur_jpeg and ref_jpeg are bytes. A full result is cur_jpeg itself.
    """
    global _full_bytes
    small = None
    work = None
    cur_img = None
    try:
        if not ref_jpeg:
            _full_bytes = len(cur_jpeg)
            return True, cur_jpeg, 100

        # Known full-frame cost, so the crop-vs-full test uses this JPEG's size.
        _full_bytes = len(cur_jpeg)

        work = _thumb(ref_jpeg)
        small = _thumb(cur_jpeg)
        kind, rects, changed, pct = _plan(small, work)
        del small
        del work
        small = None
        work = None
        gc.collect()

        if kind == "empty":
            return False, struct.pack("<B", 0), pct
        if kind != "crops":
            return True, cur_jpeg, pct

        cur_img = _full_frame(cur_jpeg)
        send_full, payload = _pack_crops(cur_img, rects, changed)
        return send_full, payload, pct
    except Exception as e:
        print("encode_image failed:", e)
        try:
            sys.print_exception(e)
        except Exception:
            pass
        gc.collect()
        return True, cur_jpeg, 100
    finally:
        if small is not None:
            del small
        if work is not None:
            del work
        if cur_img is not None:
            del cur_img
            gc.collect()


def note_full_size(n):
    """Tell the estimator how big a real full frame is. Call after any send
    where send_full was True."""
    global _full_bytes
    _full_bytes = n


JPEG_MAGIC = b"\xff\xd8\xff"
Q_OUT = 40                # capture and recombine JPEG quality


def _patch_sof_height(buf, h):
    """Rewrite baseline SOF height so a decode can stop after `h` rows."""
    i = 2
    n = len(buf)
    while i + 8 < n:
        if buf[i] != 0xFF:
            i += 1
            continue
        while i < n and buf[i] == 0xFF:
            i += 1
        if i >= n:
            return False
        marker = buf[i]
        i += 1
        if marker == 0xD9 or marker == 0xDA:
            return False
        if 0xD0 <= marker <= 0xD7:
            continue
        if i + 1 >= n:
            return False
        seglen = (buf[i] << 8) | buf[i + 1]
        if marker == 0xC0:
            buf[i + 3] = (h >> 8) & 0xFF
            buf[i + 4] = h & 0xFF
            return True
        if seglen < 2:
            return False
        i += seglen
    return False


def _heap_rgb_rows(data, rows):
    """First `rows` of a JPEG as RGB565 on the heap. A full frame will not fit."""
    buf = bytearray(data)
    if not _patch_sof_height(buf, rows):
        raise ValueError("no SOF")
    gc.collect()
    jpg = _open_jpeg(buf, width, rows)
    try:
        try:
            im = jpg.to_rgb565(copy=True)
        except TypeError:
            im = jpg.to_rgb565()
    finally:
        del jpg
        del buf
    if im is None or _val(im, "width") != width or _val(im, "height") != rows:
        raise ValueError("strip")
    return im


def _decode_base(data):
    """RGB of the base JPEG in the sensor frame buffer.

    A 1280x720 RGB565 copy needs ~1.8MB and will not allocate here, so the
    full decode uses copy_to_fb. That leaves the JPEG bitstream in the first
    rows; those rows are then replaced by a short heap decode of the same
    top-down scan. encode_image does not use this — it only cuts crops.
    """
    im = _full_frame(data)
    rows = (len(data) + (width * 2) - 1) // (width * 2)
    rows = (rows + 15) & ~15
    if rows < 64:
        rows = 64
    if rows > 160:
        rows = 160
    strip = None
    try:
        while rows >= 16 and rows < height:
            gc.collect()
            try:
                strip = _heap_rgb_rows(data, rows)
                break
            except MemoryError:
                print("base top strip", rows, "OOM")
                rows -= 16
        if strip is not None:
            im.draw_image(strip, 0, 0)
            print("base top filled", _val(strip, "height"), "rows")
    except Exception as e:
        print("base top strip skipped:", e)
    finally:
        if strip is not None:
            del strip
            gc.collect()
    return im


def combine_base_and_delta(base_jpeg, delta, quality=Q_OUT):
    """Paste an encode_image payload back onto the base JPEG.

    Runs on the camera (no Pillow). Returns (True, jpeg_bytes) or (False, None).
    An empty update returns the original base bytes. A full-frame payload,
    which is itself a JPEG, is returned unchanged.
    """
    base = None
    try:
        if delta[:3] == JPEG_MAGIC:
            return True, bytes(delta) if not isinstance(delta, bytes) else delta
        n = delta[0]
        if n > MAX_TILES:
            raise ValueError("tile count %d" % n)
        if n == 0:
            if isinstance(base_jpeg, bytes):
                return True, base_jpeg
            return True, bytes(base_jpeg)

        items = []
        off = 1
        for i in range(n):
            if off + 12 > len(delta):
                raise ValueError("truncated header for crop %d" % i)
            x, y, w, h, ln = struct.unpack_from("<HHHHI", delta, off)
            off += 12
            if off + ln > len(delta) or x + w > width or y + h > height or w == 0 or h == 0:
                raise ValueError("bad crop %d" % i)
            items.append((x, y, w, h, off, ln))
            off += ln
        if off != len(delta):
            raise ValueError("trailing bytes")

        gc.collect()
        base = _decode_base(base_jpeg)
        for (x, y, w, h, p, ln) in items:
            gc.collect()
            crop = _crop_rgb(delta[p:p + ln], w, h)
            base.draw_image(crop, x, y)
            del crop
        gc.collect()
        return True, _jpeg_bytes(base, quality)
    except Exception as e:
        print("combine failed:", e)
        try:
            sys.print_exception(e)
        except Exception:
            pass
        gc.collect()
        return False, None
    finally:
        if base is not None:
            del base
            gc.collect()


# ---- live camera test (OpenMV) ----------------------------------------

def _snap_jpeg(quality=Q_OUT, settle=2):
    """Capture one frame as JPEG bytes.

    Two things here are not optional:

    `settle` discards frames first. The first snapshot after the sensor has
    been idle can be torn -- a band of uninitialised buffer across the top.
    If such a frame becomes the reference, every later reconstruction shows
    that band wherever no crop happens to cover it, and the detector will
    not repair it: random bytes threshold to a sparse mask that erode then
    removes.

    The return is trimmed to size(). bytes(img) hands back the whole
    allocated buffer on some builds, appending garbage and inflating the
    length -- which then corrupts _full_bytes and the crops-vs-full choice.
    """
    import sensor
    for _ in range(settle):
        sensor.snapshot()
    img = sensor.snapshot()
    try:
        jpeg = img.compress(quality=quality)
    except TypeError:
        jpeg = img.compress(quality)
    return bytes(memoryview(jpeg.bytearray())[:jpeg.size()])


def save_jpeg_bytes(jpeg_bytes, filename=None):
    """Save JPEG bytes to flash using the current epoch timestamp."""
    import time
    if filename is None:
        filename = "/flash/{}.jpg".format(int(time.time() * 1000))
    with open(filename, "wb") as f:
        f.write(jpeg_bytes)
    print("Saved:", filename)


def main():
    import sensor
    import time

    sensor.reset()
    sensor.set_pixformat(sensor.RGB565)
    sensor.set_framesize(sensor.HD)   # 1280x720
    sensor.skip_frames(time=2000)

    print("capturing base in 5 seconds...")
    time.sleep_ms(5000)
    base_jpeg_bytes = _snap_jpeg(Q_OUT)
    print("base jpeg size:", len(base_jpeg_bytes), "B")

    print("taking new image after 5 seconds...")
    time.sleep_ms(5000)
    img_jpeg_bytes = _snap_jpeg(Q_OUT)
    print("new jpeg size:", len(img_jpeg_bytes), "B")

    send_full, payload, pct_change = encode_image(img_jpeg_bytes, base_jpeg_bytes)
    print("diff pct:", pct_change, "%  send_full:", send_full)

    if send_full:
        print("difference too large - using full image")
        return

    print("using delta image, delta size:", len(payload), "B")
    print("combining image using combine_base_and_delta...")
    succ, image_bytes = combine_base_and_delta(base_jpeg_bytes, payload)
    if not succ:
        print("combine failed")
        return
    print("combined jpeg size:", len(image_bytes), "B")

    # Upload the captured frame, then the diff+combine result.
    UPLOAD_OR_SAVE = True
    if UPLOAD_OR_SAVE:
        import ubinascii
        import uasyncio as asyncio
        import enc
        from config import (
            get_machine_id,
            ENCRYPTION_ENABLED,
            uses_hybrid_encryption,
        )
        from utils import pack_image_meta_header, get_rand
        from clock_utils import get_epoch_ms
        from internet_driver import InternetDriver, EVENT_URL

        machine_id = get_machine_id()
        if machine_id is None:
            print("no machine_id, skip upload")
            return

        event_epoch_ms = get_epoch_ms()
        frames = (
            ("captured", img_jpeg_bytes, event_epoch_ms),
            ("combined", image_bytes, event_epoch_ms + 1),
        )

        internet_driver = InternetDriver()
        asyncio.run(internet_driver.establish_internet())
        if not internet_driver.has_internet:
            print("no internet connection, just saving the images to flash")
            save_jpeg_bytes(img_jpeg_bytes)
            save_jpeg_bytes(image_bytes)
            return

        pub = None
        if ENCRYPTION_ENABLED and uses_hybrid_encryption("F"):
            pub = enc.EncNode(machine_id).get_pub_key()

        for label, jpeg, epoch_ms in frames:
            print("\n...uploading", label, "image to server......")
            image_id = get_rand(3)
            file_bytes = (
                pack_image_meta_header(0, 0, epoch_ms, image_id, "B", Q_OUT)
                + jpeg
            )
            if pub is not None:
                enc_msgbytes = enc.encrypt_hybrid(file_bytes, pub)
                print(label, "encrypted size:", len(enc_msgbytes), "B")
            else:
                enc_msgbytes = file_bytes
                print(label, "encryption disabled, raw size:", len(enc_msgbytes), "B")

            data_b64 = ubinascii.b2a_base64(enc_msgbytes).rstrip().decode()
            del enc_msgbytes
            del file_bytes
            gc.collect()
            upload = {
                "machine_id": machine_id,
                "msg_typ": "F2",
                "data": data_b64,
                "epoch_ms": epoch_ms,
                "enc": ENCRYPTION_ENABLED,
            }
            ok, http_code, response = asyncio.run(
                internet_driver.upload_data(upload, EVENT_URL)
            )
            print(label, "upload:", ok, "http:", http_code, "resp:", response)
            del data_b64
            del upload
            gc.collect()


if __name__ == "__main__":
    main()
