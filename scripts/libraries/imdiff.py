"""
main.py  -  OpenMV MicroPython
Live person detection using greyscale grid comparison.

Algorithm:
  - First frame → base reference (raw greyscale means per cell)
  - Every CAPTURE_INTERVAL_MS: capture frame, compare to base
  - Detection uses median normalisation + spikiness gate + cluster check
  - If no detection for BASE_REFRESH_MS: refresh base with current frame
  - On detection: calls on_detection() — fill this in yourself

Parameters (tune from tuner.py output):
  COLS, ROWS       grid dimensions
  THRESHOLD        normalised diff to consider a cell a hit
  MIN_CLUSTER      adjacent hit cells required
  MIN_SPIKINESS    hit cell must be >= this × mean diff (0 = disabled)

Copy this file to OpenMV SD card as main.py.
No other files required.
"""

import sensor
import image
import time

# ── Parameters ────────────────────────────────────────────────────────────────

COLS               = 19
ROWS               = 11
THRESHOLD          = 4
MIN_CLUSTER        = 1
MIN_SPIKINESS      = 3

CAPTURE_INTERVAL_MS = 5000    # capture every 5 seconds
BASE_REFRESH_MS     = 600000  # refresh base every 10 minutes (if no detection)

# ── Camera setup ──────────────────────────────────────────────────────────────

sensor.reset()
sensor.set_pixformat(sensor.GRAYSCALE)
sensor.set_framesize(sensor.VGA)    # 640x480 — change to QVGA if memory is tight
sensor.skip_frames(time=2000)       # let auto-exposure settle

# ── Detection callback — FILL THIS IN ─────────────────────────────────────────

import os
import utime

# ── Grid helpers (pure Python, no numpy) ──────────────────────────────────────

def _get_step_cell(dim, n):
    step = max(1, dim // n)
    cell = step * 2
    return step, cell


def _region_mean(img, x0, y0, x1, y1):
    """Mean greyscale of a rectangle using OpenMV get_statistics."""
    w = x1 - x0
    h = y1 - y0
    if w <= 0 or h <= 0:
        return 0
    stats = img.get_statistics(roi=(x0, y0, w, h))
    return stats.mean()


def compute_means(img):
    """
    Return flat list of COLS*ROWS greyscale means using overlapping grid.
    step = dim // n,  cell = 2*step,  last cell extends to edge.
    """
    W, H     = img.width(), img.height()
    step_W, cell_W = _get_step_cell(W, COLS)
    step_H, cell_H = _get_step_cell(H, ROWS)
    means = []
    for row in range(ROWS):
        for col in range(COLS):
            x0 = col * step_W
            y0 = row * step_H
            x1 = W if col == COLS - 1 else min(W, x0 + cell_W)
            y1 = H if row == ROWS - 1 else min(H, y0 + cell_H)
            means.append(_region_mean(img, x0, y0, x1, y1))
    return means


# ── Median (no statistics module on MicroPython) ──────────────────────────────

def _median(values):
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 == 1 else (s[mid - 1] + s[mid]) // 2


# ── Largest connected cluster (4-connectivity BFS) ────────────────────────────

def _largest_cluster(hit_indices):
    if not hit_indices:
        return 0
    hit_set = set(hit_indices)
    visited = set()
    best    = 0
    for start in hit_indices:
        if start in visited:
            continue
        size  = 0
        queue = [start]
        visited.add(start)
        while queue:
            idx  = queue.pop()
            size += 1
            r, c = divmod(idx, COLS)
            for dr, dc in ((-1,0),(1,0),(0,-1),(0,1)):
                nr, nc = r + dr, c + dc
                if 0 <= nr < ROWS and 0 <= nc < COLS:
                    nidx = nr * COLS + nc
                    if nidx in hit_set and nidx not in visited:
                        visited.add(nidx)
                        queue.append(nidx)
        if size > best:
            best = size
    return best


# ── Core detection ────────────────────────────────────────────────────────────

def detect(base_means, cand_means, mn, mx):
    """
    Compare candidate means to base means.
    Returns (is_hit, hit_cells) where hit_cells = [(row, col, diff), ...]
    """
    n    = len(base_means)
    span = (mx - mn) if mx != mn else 1

    # Raw diffs
    raw_diffs = [cand_means[i] - base_means[i] for i in range(n)]

    # Median normalisation — removes global lighting shift
    offset = _median(raw_diffs)

    # Normalised diffs in score units (1-100 scale)
    norm_diffs = [int(round((d - offset) * 99 / span)) for d in raw_diffs]
    abs_diffs  = [abs(d) for d in norm_diffs]

    # Spikiness gate
    if MIN_SPIKINESS > 0:
        mean_abs = sum(abs_diffs) / n if n else 1
        mean_abs = mean_abs if mean_abs > 0 else 1e-6
    else:
        mean_abs = 1

    # Hit indices
    hit_indices = []
    for i, d in enumerate(abs_diffs):
        if d >= THRESHOLD:
            if MIN_SPIKINESS == 0 or d >= MIN_SPIKINESS * mean_abs:
                hit_indices.append(i)

    # Cluster check
    cluster = _largest_cluster(hit_indices)
    is_hit  = cluster >= MIN_CLUSTER

    hit_cells = []
    if is_hit:
        for i in hit_indices:
            r, c = divmod(i, COLS)
            hit_cells.append((r, c, norm_diffs[i]))

    return is_hit, hit_cells


# ── Main loop ─────────────────────────────────────────────────────────────────
print("Starting detector: {}x{} grid, threshold={}, spikiness={}".format(
      COLS, ROWS, THRESHOLD, MIN_SPIKINESS))

base_means      = None
base_mn         = 0
base_mx         = 0
last_capture_ms = 0
last_base_ms    = 0
frame_count     = 0

while True:
    now = time.ticks_ms()

    # Wait for capture interval
    if time.ticks_diff(now, last_capture_ms) < CAPTURE_INTERVAL_MS:
        time.sleep_ms(100)
        continue

    last_capture_ms = now
    frame_count    += 1

    # Capture frame
    start_ms = time.ticks_ms()    
    img   = sensor.snapshot()
    means = compute_means(img)

    # ── First frame or no base yet: set as base ───────────────────────────────
    if base_means is None:
        base_means  = means
        base_mn     = min(means)
        base_mx     = max(means)
        last_base_ms = now
        print("Frame {:4d} | Base initialised  mn={} mx={}".format(
              frame_count, base_mn, base_mx))
        continue

    # ── Compare to base ───────────────────────────────────────────────────────
    is_hit, hit_cells = detect(base_means, means, base_mn, base_mx)

    if is_hit:
        print("Frame {:4d} | DETECTION  {} cells: {}".format(
              frame_count, len(hit_cells),
              " ".join("R{}C{}({:+d})".format(r+1,c+1,d)
                       for r,c,d in hit_cells[:5])))
        # Do NOT refresh base after a detection
    else:
        print("Frame {:4d} | clear".format(frame_count))

        # Refresh base every BASE_REFRESH_MS if no detection
        if time.ticks_diff(now, last_base_ms) >= BASE_REFRESH_MS:
            base_means   = means
            base_mn      = min(means)
            base_mx      = max(means)
            last_base_ms = now
            print("             | Base refreshed  mn={} mx={}".format(
                  base_mn, base_mx))
    end_ms = time.ticks_ms()
    duration_ms = time.ticks_diff(end_ms, start_ms)
    print(f"Time taken: {duration_ms/1000:.4f} seconds")
