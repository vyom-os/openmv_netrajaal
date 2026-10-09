import asyncio
import binascii
import errno
import gc
import hashlib
import os
import struct
import ubinascii
import utime

import logger


class StoreUtils:

    # SD card write lock
    filesave_lock = asyncio.Lock()

    def __init__(self) -> None:
        """
        Base initialisation for filesystem helpers.

        Holds the per-instance FS stats counters that mirror the globals
        used in nv2/main.register_fs_succ.
        """
        self.file_system_err = 0
        self.file_system_success = 0
        self.file_system_consecutive_err = 0

    # ------------------------------------------------------------------
    # FS success/error tracking (equivalent of nv2/main.register_fs_succ)
    # ------------------------------------------------------------------
    def register_fs_succ(self, succ: bool) -> None:
        """
        Track filesystem success/failure counts.

        Mirrors logic from nv2/main.register_fs_succ:
          - increments success counter and resets consecutive_err on success
          - increments err and consecutive_err on failure
        """
        if succ:
            self.file_system_success += 1
            self.file_system_consecutive_err = 0
        else:
            self.file_system_err += 1
            self.file_system_consecutive_err += 1

    async def save_file_once(self, data, filepath):
        async with self.filesave_lock:
            try:
                with open(filepath, "wb") as f:
                    f.write(data)
                    os.sync()
            except Exception as e:
                logger.error(f"Could not save encrypted file {filepath} : {e}")
                return False
            logger.info(f"[FS] Saved datafile: {filepath}, datasize: {len(data)} bytes")
            return True

    async def read_file_once(self, filepath):
        async with self.filesave_lock:
            try:
                with open(filepath, "rb") as f:
                    data = f.read()
                    logger.debug(
                        f"[FS] ****************** Readable datafile, datasize: {len(data)} bytes"
                    )
                    return True, data
            except Exception as e:
                logger.error(f"[FS] -----  Failed to read file : {filepath}, e: {e}")
        return False, None

    async def save_file(self, data, filepath):
        logger.info(
            f"[FS] Saving datafile to {filepath}, datasize: {len(data)} bytes..."
        )
        try:

            for i in range(3):
                succ = await self.save_file_once(data, filepath)
                if succ:
                    await asyncio.sleep(2)
                    logger.info(f"Writtend : {filepath}")
                    readable, _ = await self.read_file_once(filepath)
                    if readable:
                        logger.info(f"Writtend and readable : {filepath}")
                        # notify DbStore-style tracker if present
                        if hasattr(self, "register_fs_succ"):
                            try:
                                self.register_fs_succ(True)
                            except Exception as e:
                                logger.error(f"[FS] register_fs_succ(True) failed: {e}")
                        return True
                    else:
                        logger.error(f"Writtend and NOT readable : {filepath}")
                else:
                    logger.error(f"Not able to write {filepath}")

            logger.error(f"Writing failed after retries for {filepath}")
            if hasattr(self, "register_fs_succ"):
                try:
                    self.register_fs_succ(False)
                except Exception as e:
                    logger.error(f"[FS] register_fs_succ(False) failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Some unknown Error saving file {filepath}: {e}")
            if hasattr(self, "register_fs_succ"):
                try:
                    self.register_fs_succ(False)
                except Exception as e2:
                    logger.error(
                        f"[FS] register_fs_succ(False) failed in exception path: {e2}"
                    )
            return False


class DbStore(StoreUtils):
    """
    Storage helper for this project, matching how `nv2/main.py` uses:
    - image directories on SD card
    - in-transit image/file buffer
    - in-memory image buffers
    """

    # Recompile buffer (same size as in nv2/main.py)
    DATA_BUFFER_SIZE = 120 * 1024  # 120KB

    # Image list buffer defaults (aligned with nv2/main.py, extended header)
    IMG_LIST_CAPACITY = 20  # Number of image slots
    IMG_MAX_SIZE = 100  # 100KB per image
    IMG_LIST_SLOT_SIZE = IMG_MAX_SIZE * 1024

    # Header layout inside each image slot:
    # [0]         : 1 byte  -> occupied flag (0 = empty, 1 = used)
    # [1:7]       : 6 bytes -> epoch_ms (48-bit big-endian, enough for 13-digit ms)
    # [7]         : 1 byte  -> creator_id (0-255)
    # [8]         : 1 byte  -> retry count (0-10)
    # [9:12]      : 3 bytes -> image size (big-endian)
    IMG_FILLED_FLAG_OFFSET = 0
    IMG_LIST_EPOCH_OFFSET = 1
    IMG_LIST_EPOCH_BYTES = 6
    IMG_LIST_CREATOR_OFFSET = 7
    IMG_LIST_CREATOR_BYTES = 1
    IMG_LIST_RETRY_OFFSET = 8
    IMG_LIST_RETRY_BYTES = 1
    IMG_LIST_SIZE_OFFSET = 9
    IMG_LIST_HEADER_SIZE = 12
    IMG_LIST_DATA_CAPACITY = IMG_LIST_SLOT_SIZE - IMG_LIST_HEADER_SIZE

    def __init__(self, process_id_str, my_addr) -> None:
        """
        Complete all 5 tasks described in the comments:
        1. Optional image directory on SD card
        2. In-transit recompile buffer
        3. Image ring buffer
        """
        # Initialise base StoreUtils (including FS counters)
        super().__init__()

        if not process_id_str:
            raise ValueError("process_id_str must be a non-empty string")

        self.process_id_str: str = process_id_str
        self.my_addr = my_addr

        self.fs_root = "/sdcard"
        self.sdcard_available = (
            self._is_sdcard_readable() and self._is_sdcard_writable()
        )

        self.process_dir = None
        self.image_dir = None
        self.logs_dir = None

        if self.sdcard_available:
            logger.info(f"[DB] ⛃⛃⛃⛁⛁⛁ SD CARD AVAILABLE & USABLE, using fs_root={self.fs_root}")
            self.process_dir = f"{self.fs_root}/{self.process_id_str}"
            self.image_dir = f"{self.process_dir}/all_images"
            self.logs_dir = f"{self.process_dir}/logs"
            self._create_dir_if_not_exists(self.process_dir)
            self._create_dir_if_not_exists(self.image_dir)
            self._create_dir_if_not_exists(self.logs_dir)
        else:
            logger.warning("[DB] ⛃⛃⛃⛁⛁⛁ SD CARD NOT AVAILABLE/USABLE, operating in memory-only mode")

        # ---- TASK 2: in-transit recompile buffer ----
        # self.image_recompile_buffer = None
        # self._init_file_recompile_buffer()

        # ---- TASK 3: image ring buffer ----
        self.image_list_buffer = None
        self._last_sent_img_creator = None
        self._init_image_list_buffer()

        # counters
        self.image_queued_count = 0
        self.img_sent_count = 0
        self.img_dropped_count = 0
        self.img_failed_count = 0

        self.LIMIT_PER_DEVICE = 6
        self.img_retry_count_to_fail = 20 # TODO need to increased

    # ------------------------------------------------------------------
    # Internal helpers (filesystem)
    # ------------------------------------------------------------------
    def _is_sdcard_readable(self) -> bool:
        """Best-effort check that SD card root is listable."""
        for attempt in range(2):
            try:
                utime.sleep_ms(300 * (attempt + 1))
                os.listdir(self.fs_root)
                logger.debug(f"[DB] SD card readable (attempt {attempt + 1})")
                return True
            except OSError:
                logger.warning(f"[DB] SD card not ready (attempt {attempt + 1}/5)")
        return False

    def _is_sdcard_writable(self) -> bool:
        """Best-effort check that we can create a tiny file."""
        test_file = f"{self.fs_root}/.dbstore_test"
        try:
            with open(test_file, "wb") as f:
                f.write(b"ok")
            os.remove(test_file)

            test_file = "/sdcard/processid"
            with open(test_file, "wb") as f:
                self.process_id_str.encode()
            return True
        except OSError:
            logger.warning("[DB] SD card not writable")
            return False

    def _create_dir_if_not_exists(self, dir_path: str) -> None:
        """Minimal variant of `create_dir_if_not_exists` in nv2/main.py."""
        try:
            parts = [p for p in dir_path.split("/") if p]
            if len(parts) < 2:
                logger.warning(f"[DB] Invalid directory path (no parent): {dir_path}")
                return
            parent = "/" + "/".join(parts[:-1])
            dir_name = parts[-1]

            if dir_name not in os.listdir(parent):
                os.mkdir(dir_path)
                logger.info(f"[DB] Created {dir_path}")
            else:
                try:
                    os.listdir(dir_path)
                except OSError:
                    logger.warning(
                        f"[DB] {dir_path} exists but is not a directory; recreating"
                    )
                    try:
                        os.remove(dir_path)
                        os.mkdir(dir_path)
                        logger.info(f"[DB] Recreated directory {dir_path}")
                    except OSError as e:
                        logger.error(
                            f"[DB] Failed to recreate directory {dir_path}: {e}"
                        )
        except Exception as e:
            logger.error(f"[DB] Error ensuring directory {dir_path}: {e}")

    # ------------------------------------------------------------------
    # Internal helpers (buffers)
    # ------------------------------------------------------------------
    # def _init_file_recompile_buffer(self) -> None:
    #     """Allocate the in-transit image/file recompilation buffer."""
    #     gc.collect()
    #     try:
    #         self.image_recompile_buffer = bytearray(self.DATA_BUFFER_SIZE)
    #         logger.info(
    #             f"[DB] Pre-allocated recompile buffer: "
    #             f"{len(self.image_recompile_buffer) // 1024}KB"
    #         )
    #     except MemoryError as e:
    #         logger.error(f"[DB] Failed to allocate recompile buffer: {e}")
    #         self.image_recompile_buffer = None
    #     except Exception as e:
    #         logger.error(f"[DB] Error allocating recompile buffer: {e}")
    #         self.image_recompile_buffer = None

    def _init_image_list_buffer(self) -> None:
        """Allocate the in-memory circular ring buffer for images."""
        gc.collect()
        try:
            self.image_list_buffer = [
                bytearray(self.IMG_LIST_SLOT_SIZE)
                for _ in range(self.IMG_LIST_CAPACITY)
            ]
            self.image_queued_count = 0
            logger.info(
                f"[DB] Pre-allocated image ring: "
                f"{self.IMG_LIST_CAPACITY} x {self.IMG_LIST_SLOT_SIZE // 1024}KB"
            )
        except MemoryError as e:
            logger.error(f"[DB] Failed to allocate image ring buffer: {e}")
            self.image_list_buffer = None
            self.image_queued_count = 0
        except Exception as e:
            logger.error(f"[DB] Error allocating image ring buffer: {e}")
            self.image_list_buffer = None
            self.image_queued_count = 0

    # ------------------------------------------------------------------
    # Public API: setters / getters / round-robin access
    # ------------------------------------------------------------------
    # Image ring operations ---------------------------------------------

    def storage_available(self, creator):
        # For self.my_addr, reserve LIMIT_PER_DEVICE image slots (e.g. if 2 stored,
        # assume more reserved for self.my_addr only).
        # For creator == self.my_addr, always True (old images drop, new ones store).
        # For others: True only if stored count < LIMIT_PER_DEVICE and queue has
        # space beyond the reservation for self.my_addr.
        try:
            if creator == self.my_addr:
                return True

            if self.image_list_buffer is None:
                return False

            creator_img_count = 0
            my_img_count = 0
            total_filled = 0

            for idx in range(self.IMG_LIST_CAPACITY):
                slot = self.image_list_buffer[idx]
                if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                    continue
                total_filled += 1
                c_id = slot[self.IMG_LIST_CREATOR_OFFSET]
                if c_id == creator:
                    creator_img_count += 1
                if c_id == self.my_addr:
                    my_img_count += 1

            # Reserve free slots for self.my_addr up to LIMIT_PER_DEVICE.
            reserved_for_me = max(0, self.LIMIT_PER_DEVICE - my_img_count)
            max_usable_for_others = self.IMG_LIST_CAPACITY - total_filled - reserved_for_me
            if creator_img_count >= self.LIMIT_PER_DEVICE:
                logger.info(
                    f"[DB] creator={creator} has {creator_img_count} images, "
                    f"exceeding limit of {self.LIMIT_PER_DEVICE}, returning False"
                )
                return False
            if max_usable_for_others <= 0:
                logger.info(
                    "[DB] no space left in queue for other devices, returning False"
                )
                return False
            return True
        except Exception as e:
            logger.error(f"[DB] storage_available failed for creator={creator}: {e}")
            return False

    def store_image_raw(self, epoch_ms: int, creator_id: int, img_snapshot) -> bool:
        """
        Store raw image bytes into the image list buffer.
        """
        try:
            if self.sdcard_available:
                raw_path = f"{self.image_dir}/{self.process_id_str}_{creator_id}_{epoch_ms}_raw.jpg"
                logger.info(
                    f"Saving raw image to {raw_path} : imbytesize = {len(img_snapshot.bytearray())}"
                )
                img_snapshot.save(raw_path)
                logger.info(
                    f"Saved raw image: {raw_path}: raw size = {len(img_snapshot.bytearray())} bytes"
                )
            else:
                logger.warning("[DB] SD card not ready, skipping raw image save...")
        except Exception as e:
            logger.error(f"Failed to save raw image: {e}")

    def store_image(
        self,
        epoch_ms: int,
        creator_id: int,
        retry: int,
        img_bytes: bytes,
        save_file: bool = True,
    ):
        """
        Store raw image bytes into the image list buffer.
        Header layout per slot:
          - 1 byte  : occupied flag (0 = empty, 1 = used)
          - 6 bytes : epoch_ms (48-bit big-endian)
          - 1 byte  : creator_id (0-255)
          - 3 bytes : image size (big-endian)

        Return:
            (store_succ: bool, error: str)
        """
        if retry >= self.img_retry_count_to_fail:
            self.update_img_failed_count(1)
            return False, "retry_limit_reached"

        if self.image_list_buffer is None:
            logger.error("[DB] image_list_buffer not INITIALIZED")
            return False, "image_buffer_not_initialized"

        if creator_id < 0 or creator_id > 255:
            logger.error(f"[DB] creator_id out of range (0-255): {creator_id}")
            return False, "creator_out_of_range"

        size = len(img_bytes)
        if size > self.IMG_LIST_DATA_CAPACITY:
            logger.error(
                f"[DB] Image size {size} exceeds image data capacity "
                f"{self.IMG_LIST_DATA_CAPACITY}, returning..."
            )
            return False, "image_too_large"

        # ------------------------------------------------------------------
        # Slot selection strategy
        # ------------------------------------------------------------------
        # 1) If there is free space (image_list_count < capacity), find first empty slot.
        # 2) Else (buffer full):
        #    - If image is from this device (creator_id == self.my_addr),
        #      replace the oldest image of this creator (min epoch_ms).
        #    - If image is from other device, reject (return False).

        slot_idx = None
        creator_img_count = 0
        my_img_count = 0
        total_filled = 0

        for idx in range(self.IMG_LIST_CAPACITY):
            slot = self.image_list_buffer[idx]
            if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                continue
            total_filled += 1
            c_id = slot[self.IMG_LIST_CREATOR_OFFSET]
            if c_id == creator_id:
                creator_img_count += 1
            if c_id == self.my_addr:
                my_img_count += 1

        if (
            creator_img_count < self.LIMIT_PER_DEVICE
            and self.image_queued_count < self.IMG_LIST_CAPACITY
        ):
            # Find first empty slot (flag == 0)
            for idx in range(self.IMG_LIST_CAPACITY):
                slot = self.image_list_buffer[idx]
                if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                    slot_idx = idx
                    break
            if slot_idx is None:
                logger.error(
                    "[DB] Error in the code, couldn't find empty space saving image"
                )
                return False, "empty_slot_not_found:unknown_error"
        else:
            if creator_id != self.my_addr:
                logger.warning(
                    f"[DB] Buffer full and creator_id {creator_id} != my_addr {self.my_addr}; "
                    "rejecting new image"
                )
                return False, "buffer_full_non_creator"
            else:  # find the index to replace the old image
                oldest_epoch = None
                oldest_idx = None
                for idx in range(self.IMG_LIST_CAPACITY):
                    slot = self.image_list_buffer[idx]
                    if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                        continue

                    c_id = slot[self.IMG_LIST_CREATOR_OFFSET]
                    if c_id != self.my_addr:
                        continue

                    epoch_bytes = slot[
                        self.IMG_LIST_EPOCH_OFFSET : self.IMG_LIST_EPOCH_OFFSET
                        + self.IMG_LIST_EPOCH_BYTES
                    ]
                    epoch_val = int.from_bytes(epoch_bytes, "big")

                    if oldest_epoch is None or epoch_val < oldest_epoch:
                        oldest_epoch = epoch_val
                        oldest_idx = idx

                if oldest_idx is None:
                    logger.warning(
                        "[DB] Buffer full but no existing image for this creator; rejecting new image"
                    )
                    return False, "buffer_full_no_replace_candidate"

                slot_idx = oldest_idx
                self.update_img_queued_count(-1)
                self.update_img_dropped_count(1)

        # ------------------------------------------------------------------
        # Write header + data into chosen slot
        # ------------------------------------------------------------------
        slot = self.image_list_buffer[slot_idx]

        # Occupied flag
        slot[self.IMG_FILLED_FLAG_OFFSET] = 1

        # Epoch (48-bit, big-endian)
        epoch_clamped = epoch_ms & ((1 << (8 * self.IMG_LIST_EPOCH_BYTES)) - 1)
        epoch_bytes = epoch_clamped.to_bytes(self.IMG_LIST_EPOCH_BYTES, "big")
        start = self.IMG_LIST_EPOCH_OFFSET
        slot[start : start + self.IMG_LIST_EPOCH_BYTES] = epoch_bytes

        # Creator id (1 byte)
        slot[self.IMG_LIST_CREATOR_OFFSET] = creator_id & 0xFF

        # Retry count (1 byte, clamped 0-10)
        retry_clamped = max(0, min(10, retry))
        slot[self.IMG_LIST_RETRY_OFFSET] = retry_clamped & 0xFF

        # Size (3 bytes, big-endian)
        size_bytes = size.to_bytes(3, "big")
        start = self.IMG_LIST_SIZE_OFFSET
        slot[start : start + 3] = size_bytes

        # Image data
        data_start = self.IMG_LIST_HEADER_SIZE
        slot[data_start : data_start + size] = img_bytes[:size]

        # Housekeeping counters
        self.image_queued_count = min(self.image_queued_count + 1, self.IMG_LIST_CAPACITY)
        logger.info(f"Image added for db_queue, new length: {self.image_queued_count}, {self.process_id_str}_{creator_id}_{epoch_ms}.enc")

        # Optionally persist the encrypted image to filesystem (fire-and-forget).
        if self.sdcard_available and save_file:
            enc_filepath = (
                f"{self.image_dir}/{self.process_id_str}_{creator_id}_{epoch_ms}.enc"
            )
            logger.info(f"[DB] Scheduling save of encrypted image to {enc_filepath}")
            try:
                # Do not block caller; schedule async write in background.
                asyncio.create_task(self.save_file(img_bytes, enc_filepath))
            except Exception as e:
                logger.error(
                    f"[DB] Failed to schedule encrypted image save to {enc_filepath}: {e}"
                )

        return True, ""

    def get_next_image_to_send(self):
        """
        Select the next image to send in round-robin fashion across creators.

        Returns:
            tuple (img_bytes, epoch_ms, creator_id) or None if no image is available.

        Side effects:
            - Removes the chosen image from the buffer (marks slot as empty).
            - Updates internal round-robin state.
        """
        if self.image_list_buffer is None or self.image_queued_count == 0:
            return None, None, None, None, None

        # Collect distinct creators from occupied slots
        creators = []
        for idx in range(self.IMG_LIST_CAPACITY):
            slot = self.image_list_buffer[idx]
            if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                continue
            c_id = slot[self.IMG_LIST_CREATOR_OFFSET]
            if c_id not in creators:
                creators.append(c_id)

        if not creators:
            return None, None, None, None, None

        creators.sort()

        # Round-robin across creators
        if (
            self._last_sent_img_creator is not None
            and self._last_sent_img_creator in creators
        ):
            last_idx = creators.index(self._last_sent_img_creator)
            next_idx = (last_idx + 1) % len(creators)
            chosen_creator = creators[next_idx]
        else:
            chosen_creator = creators[0]

        # Among slots for chosen_creator, pick oldest (min epoch_ms)
        chosen_slot_idx = None
        chosen_epoch = None

        for idx in range(self.IMG_LIST_CAPACITY):
            slot = self.image_list_buffer[idx]
            if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                continue

            c_id = slot[self.IMG_LIST_CREATOR_OFFSET]
            if c_id != chosen_creator:
                continue

            epoch_bytes = slot[
                self.IMG_LIST_EPOCH_OFFSET : self.IMG_LIST_EPOCH_OFFSET
                + self.IMG_LIST_EPOCH_BYTES
            ]
            epoch_val = int.from_bytes(epoch_bytes, "big")

            if chosen_slot_idx is None or epoch_val < chosen_epoch:
                chosen_slot_idx = idx
                chosen_epoch = epoch_val

        if chosen_slot_idx is None:
            # Fallback: pick any oldest image regardless of creator
            for idx in range(self.IMG_LIST_CAPACITY):
                slot = self.image_list_buffer[idx]
                if slot[self.IMG_FILLED_FLAG_OFFSET] == 0:
                    continue
                epoch_bytes = slot[
                    self.IMG_LIST_EPOCH_OFFSET : self.IMG_LIST_EPOCH_OFFSET
                    + self.IMG_LIST_EPOCH_BYTES
                ]
                epoch_val = int.from_bytes(epoch_bytes, "big")
                if chosen_slot_idx is None or epoch_val < chosen_epoch:
                    chosen_slot_idx = idx
                    chosen_epoch = epoch_val

        if chosen_slot_idx is None:
            return None, None, None, None, None

        slot = self.image_list_buffer[chosen_slot_idx]

        # Decode header
        epoch_bytes = slot[
            self.IMG_LIST_EPOCH_OFFSET : self.IMG_LIST_EPOCH_OFFSET
            + self.IMG_LIST_EPOCH_BYTES
        ]
        epoch_ms = int.from_bytes(epoch_bytes, "big")

        creator_id = slot[self.IMG_LIST_CREATOR_OFFSET]
        retry = slot[self.IMG_LIST_RETRY_OFFSET]

        size_bytes = slot[self.IMG_LIST_SIZE_OFFSET : self.IMG_LIST_SIZE_OFFSET + 3]
        size = int.from_bytes(size_bytes, "big")

        if size <= 0 or size > self.IMG_LIST_DATA_CAPACITY:
            # Corrupt entry; clear it and skip
            logger.warning(f"[DB] Corrupt image slot at {chosen_slot_idx}, clearing")
            slot[self.IMG_FILLED_FLAG_OFFSET] = 0
            self.image_queued_count = max(0, self.image_queued_count - 1)
            return None, None, None, None, None

        data_start = self.IMG_LIST_HEADER_SIZE
        img_bytes = bytes(slot[data_start : data_start + size])

        # Mark slot as empty and update counters
        slot[self.IMG_FILLED_FLAG_OFFSET] = 0
        self.image_queued_count = max(0, self.image_queued_count - 1)

        # Update round-robin state
        self._last_sent_img_creator = creator_id
        img_md5 = ubinascii.hexlify(hashlib.md5(img_bytes).digest()).decode()
        return epoch_ms, creator_id, retry, img_bytes, img_md5

    # Listing Getters and Setters for images

    def update_img_queued_count(self, count):
        self.image_queued_count = self.image_queued_count + count

    def get_img_queued_count(self):
        return self.image_queued_count

    def get_img_queued_list(self):  # TODO akash to implement it
        return []

    def get_img_sent_count(self):
        return self.img_sent_count

    def update_img_sent_count(self, count):
        self.img_sent_count = self.img_sent_count + count

    def get_img_dropped_count(self):
        return self.img_dropped_count

    def update_img_dropped_count(self, count):
        self.img_dropped_count = self.img_dropped_count + count

    def get_img_failed_count(self):
        return self.img_failed_count

    def update_img_failed_count(self, count):
        self.img_failed_count = self.img_failed_count + count

    def get_fs_succ_count(self):
        return self.file_system_success

    def get_fs_err_count(self):
        return self.file_system_err

    def get_fs_consecutive_err_count(self):  # NOT IN USE
        return self.file_system_consecutive_err

    def db_store(self):
        # TODO akash, return th list of dict in form of
        return []

    def clear_image_list(self):
        # TODO akash, make it
        return True


# /vyomos log rotation. Mount/mkfs is owned by C (vyomos_fs_mount):
# LittleFS is created only when the volume has no filesystem magic (first boot
# after that flash region is erased, e.g. full-chip / firmware region wipe).
# Soft reboot and power cycle keep /vyomos. Python never mkfs/wipes the volume
# except when config.VERSION changes (new firmware deploy) — see recovery.
#
# Active file grows to 1MB, then becomes /vyomos/zip/log_NNNNN.zip.
# When active + zips exceed 3MB, delete the oldest zip.
LOG_ROOT = "/vyomos"
LOG_PATH = "/vyomos/log.txt"
LOG_ZIP_DIR = "/vyomos/zip"
LOG_SEQ_PATH = "/vyomos/zip/seq.txt"
LOG_SEQ_PARTIAL = "/vyomos/zip/seq.txt.partial"
LOG_FW_VERSION_PATH = "/vyomos/.fw_version"
LOG_SEGMENT_BYTES = 1 * 1024 * 1024
LOG_BUDGET_BYTES = 3 * 1024 * 1024
LOG_CHUNK_BYTES = 4096
# Local header (30) + EOCD (22) + central dir fixed (46) + 2 * name; name <= 32.
LOG_ZIP_OVERHEAD = 30 + 22 + 46 + 64

_log_ready = False
_log_busy = False


def _file_size(path):
    try:
        return os.stat(path)[6]
    except OSError as e:
        if e.args and e.args[0] == errno.ENOENT:
            return 0
        raise


def _sync_fs():
    try:
        os.sync()
    except Exception:
        pass


def _remove_path(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _rm_tree(path):
    """Remove a file or directory tree. Never touches the /vyomos mount itself."""
    try:
        st = os.stat(path)
    except OSError:
        return
    # MicroPython: st[0] mode; directories have 0o040000
    if (st[0] & 0o170000) == 0o040000:
        try:
            for name in os.listdir(path):
                _rm_tree(path + "/" + name)
        except OSError:
            pass
        try:
            os.rmdir(path)
        except OSError:
            pass
    else:
        _remove_path(path)


def _wipe_vyomos_contents():
    """Delete everything under /vyomos. Volume mount stays; used on new firmware only."""
    try:
        names = os.listdir(LOG_ROOT)
    except OSError:
        return
    for name in names:
        _rm_tree(LOG_ROOT + "/" + name)
    _sync_fs()


def _write_fw_version(version_str):
    partial = LOG_FW_VERSION_PATH + ".partial"
    try:
        with open(partial, "w") as f:
            f.write(version_str)
        _sync_fs()
        _remove_path(LOG_FW_VERSION_PATH)
        os.rename(partial, LOG_FW_VERSION_PATH)
        _sync_fs()
    except OSError:
        _remove_path(partial)


def _firmware_version_str():
    try:
        from config import VERSION, get_version_str

        return get_version_str(VERSION)
    except Exception:
        return ""


def _maybe_clear_on_new_firmware():
    """
    Clear /vyomos only when the running firmware version differs from the
    last version stored on the volume. Soft reboot with the same firmware
    leaves all data intact. Missing marker (first boot of this feature)
    records the current version without wiping existing files.
    """
    current = _firmware_version_str()
    if not current:
        return
    try:
        with open(LOG_FW_VERSION_PATH, "r") as f:
            stored = f.read().strip()
    except OSError:
        _write_fw_version(current)
        return
    if stored == current:
        return
    _wipe_vyomos_contents()
    _write_fw_version(current)


def _ensure_zip_dir():
    try:
        os.listdir(LOG_ZIP_DIR)
        return
    except OSError:
        pass
    try:
        os.mkdir(LOG_ZIP_DIR)
    except OSError:
        pass


def _parse_zip_seq(name):
    # log_00001.zip -> 1
    try:
        return int(name[4:9])
    except (ValueError, IndexError):
        return -1


def _list_zip_names():
    try:
        names = os.listdir(LOG_ZIP_DIR)
    except OSError:
        return []
    out = []
    for name in names:
        if name.startswith("log_") and name.endswith(".zip") and ".partial" not in name:
            out.append(name)
    out.sort()
    return out


def _store_zip_payload(path):
    """Return (arcname, payload_offset, payload_size) for a store-method zip.

    Payload is the uncompressed .txt sitting after the local file header.
    Returns None if the file is not a readable store-method zip.
    """
    try:
        with open(path, "rb") as f:
            if f.read(4) != b"PK\x03\x04":
                return None
            f.seek(8)
            method = struct.unpack("<H", f.read(2))[0]
            if method != 0:
                return None
            f.seek(18)
            comp, uncomp = struct.unpack("<II", f.read(8))
            name_len, extra_len = struct.unpack("<HH", f.read(4))
            name = f.read(name_len)
            if comp != uncomp:
                return None
            if extra_len:
                f.read(extra_len)
            return (name.decode(), 30 + name_len + extra_len, uncomp)
    except (OSError, ValueError, UnicodeError):
        return None


def list_vyomos_log_exports():
    """Log payloads to copy off /vyomos, oldest segment first, then the live file.

    Each dict:
      file_name: str  — log_NNNNN.txt for a rotated segment, or log.txt
      path: str       — filesystem path to open
      offset: int     — byte offset of the text payload (0 for log.txt)
      size: int       — payload length in bytes
    """
    items = []
    for name in _list_zip_names():
        path = LOG_ZIP_DIR + "/" + name
        info = _store_zip_payload(path)
        if not info:
            continue
        arcname, offset, size = info
        if size <= 0:
            continue
        items.append({
            "file_name": arcname,
            "path": path,
            "offset": offset,
            "size": size,
        })
    active_size = _file_size(LOG_PATH)
    if active_size > 0:
        items.append({
            "file_name": "log.txt",
            "path": LOG_PATH,
            "offset": 0,
            "size": active_size,
        })
    return items


def _expected_zip_size(src_size, arcname):
    name_len = len(arcname.encode() if isinstance(arcname, str) else arcname)
    return 30 + name_len + src_size + 46 + name_len + 22


def _verify_store_zip(path, src_size, arcname):
    """Return True if path looks like a complete store-method ZIP of src_size."""
    size = _file_size(path)
    if size != _expected_zip_size(src_size, arcname):
        return False
    try:
        with open(path, "rb") as f:
            if f.read(4) != b"PK\x03\x04":
                return False
            f.seek(14)
            crc_hdr, comp, uncomp = struct.unpack("<III", f.read(12))
            name_len, extra_len = struct.unpack("<HH", f.read(4))
            if comp != src_size or uncomp != src_size:
                return False
            f.seek(30 + name_len + extra_len)
            crc = 0
            remaining = src_size
            while remaining > 0:
                n = remaining if remaining < LOG_CHUNK_BYTES else LOG_CHUNK_BYTES
                chunk = f.read(n)
                if len(chunk) != n:
                    return False
                crc = binascii.crc32(chunk, crc)
                remaining -= n
            if (crc & 0xFFFFFFFF) != (crc_hdr & 0xFFFFFFFF):
                return False
            f.seek(size - 22)
            if f.read(4) != b"PK\x05\x06":
                return False
    except (OSError, ValueError):
        return False
    return True


def _scrub_zip_dir():
    """Remove incomplete/invalid archives left by power loss. Keeps good zips."""
    try:
        names = os.listdir(LOG_ZIP_DIR)
    except OSError:
        return
    for name in names:
        path = LOG_ZIP_DIR + "/" + name
        if name.endswith(".partial"):
            _remove_path(path)
            continue
        if not (name.startswith("log_") and name.endswith(".zip")):
            continue
        sz = _file_size(path)
        if sz == 0:
            _remove_path(path)
            continue
        try:
            with open(path, "rb") as f:
                if f.read(4) != b"PK\x03\x04":
                    _remove_path(path)
                    continue
                f.seek(sz - 22)
                if f.read(4) != b"PK\x05\x06":
                    _remove_path(path)
        except OSError:
            _remove_path(path)


def _recover_log_storage():
    """
    One-shot init: clear volume on new firmware version, scrub partials.
    Does not wipe on soft reboot when firmware version is unchanged.
    """
    _maybe_clear_on_new_firmware()
    _ensure_zip_dir()
    _scrub_zip_dir()
    _remove_path(LOG_SEQ_PARTIAL)
    _sync_fs()


def _total_log_bytes():
    total = _file_size(LOG_PATH)
    for name in _list_zip_names():
        total += _file_size(LOG_ZIP_DIR + "/" + name)
    return total


def _max_zip_seq():
    seq = 0
    for name in _list_zip_names():
        n = _parse_zip_seq(name)
        if n > seq:
            seq = n
    return seq


def _read_seq():
    try:
        with open(LOG_SEQ_PATH, "r") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return _max_zip_seq()


def _write_seq(seq):
    """Atomic seq update via partial + rename."""
    try:
        with open(LOG_SEQ_PARTIAL, "w") as f:
            f.write("%d" % seq)
        _sync_fs()
        _remove_path(LOG_SEQ_PATH)
        os.rename(LOG_SEQ_PARTIAL, LOG_SEQ_PATH)
        _sync_fs()
    except OSError:
        _remove_path(LOG_SEQ_PARTIAL)
        raise


def _alloc_seq():
    seq = _read_seq() + 1
    if seq <= _max_zip_seq():
        seq = _max_zip_seq() + 1
    _write_seq(seq)
    return seq


def _delete_oldest_zip():
    zips = _list_zip_names()
    if not zips:
        return False
    try:
        os.remove(LOG_ZIP_DIR + "/" + zips[0])
        _sync_fs()
    except OSError:
        return False
    return True


def _make_room(extra_needed):
    """Delete oldest zips until active+zips+extra_needed fits in budget."""
    while _total_log_bytes() + extra_needed > LOG_BUDGET_BYTES:
        if not _delete_oldest_zip():
            break


def _file_crc32(path):
    crc = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(LOG_CHUNK_BYTES)
            if not chunk:
                break
            crc = binascii.crc32(chunk, crc)
    return crc & 0xFFFFFFFF


def _zip_store_file(src_path, dest_path, arcname):
    """Write a minimal store-method ZIP (no compression) in chunked I/O."""
    size = _file_size(src_path)
    crc = _file_crc32(src_path)
    name_b = arcname.encode() if isinstance(arcname, str) else arcname
    name_len = len(name_b)

    with open(dest_path, "wb") as out:
        out.write(b"PK\x03\x04")
        out.write(struct.pack("<HHHHHIIIHH", 20, 0, 0, 0, 0, crc, size, size, name_len, 0))
        out.write(name_b)
        with open(src_path, "rb") as inp:
            while True:
                chunk = inp.read(LOG_CHUNK_BYTES)
                if not chunk:
                    break
                out.write(chunk)

        cd_offset = 30 + name_len + size
        out.write(b"PK\x01\x02")
        out.write(
            struct.pack(
                "<HHHHHHIIIHHHHHII",
                20,
                20,
                0,
                0,
                0,
                0,
                crc,
                size,
                size,
                name_len,
                0,
                0,
                0,
                0,
                0,
                0,
            )
        )
        out.write(name_b)
        cd_size = 46 + name_len
        out.write(b"PK\x05\x06")
        out.write(struct.pack("<HHHHIIH", 0, 0, 1, 1, cd_size, cd_offset, 0))


def _emergency_trim_active():
    """If rotate cannot run, drop the active file so /vyomos cannot fill forever."""
    _make_room(0)
    if _file_size(LOG_PATH) >= LOG_SEGMENT_BYTES:
        _remove_path(LOG_PATH)
        _sync_fs()


def _rotate_log():
    """Zip current log to /vyomos/zip/log_NNNNN.zip and start a new active file."""
    size = _file_size(LOG_PATH)
    if size == 0:
        return

    _ensure_zip_dir()
    _make_room(size + LOG_ZIP_OVERHEAD)

    seq = _alloc_seq()
    zip_name = "log_%05d.zip" % seq
    zip_path = LOG_ZIP_DIR + "/" + zip_name
    partial_path = zip_path + ".partial"
    arcname = "log_%05d.txt" % seq

    _remove_path(partial_path)
    try:
        _zip_store_file(LOG_PATH, partial_path, arcname)
        _sync_fs()
        if not _verify_store_zip(partial_path, size, arcname):
            _remove_path(partial_path)
            raise OSError("log zip verify failed")
        _remove_path(zip_path)
        os.rename(partial_path, zip_path)
        _sync_fs()
        _remove_path(LOG_PATH)
        _sync_fs()
    except OSError:
        _remove_path(partial_path)
        raise

    while _total_log_bytes() > LOG_BUDGET_BYTES:
        if not _delete_oldest_zip():
            break
    _sync_fs()


def append_log_line(line):
    global _log_ready, _log_busy
    if _log_busy:
        # Avoid re-entrancy if anything during rotate somehow logs again.
        return
    _log_busy = True
    try:
        if not _log_ready:
            try:
                _recover_log_storage()
            except OSError:
                pass
            _log_ready = True
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
        if _file_size(LOG_PATH) >= LOG_SEGMENT_BYTES:
            try:
                _rotate_log()
            except OSError:
                _emergency_trim_active()
    finally:
        _log_busy = False
