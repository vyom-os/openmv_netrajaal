/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (C) 2023-2026 OpenMV, LLC.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * LittleFS block device for the former ROMFS region, mounted at /vyomos.
 * This device is not registered as the USB MSC medium.
 */
#include <stdint.h>
#include <string.h>

#include "py/runtime.h"
#include "py/mperrno.h"
#include "extmod/vfs.h"
#include "fsl_common.h"
#include "fsl_romapi.h"

extern flexspi_nor_config_t qspiflash_config;

extern uint8_t __flash_start;
extern uint8_t _micropy_hw_romfs_part0_start;
extern uint8_t _micropy_hw_romfs_part0_size;

void flash_init(void);
status_t flash_erase_sector(uint32_t erase_addr);
void flash_read_block(uint32_t src_addr, uint8_t *dest, uint32_t length);
status_t flash_write_block(uint32_t dest_addr, const uint8_t *src, uint32_t length);

typedef struct _vyomos_bdev_obj_t {
    mp_obj_base_t base;
} vyomos_bdev_obj_t;

static uint32_t vyomos_flash_base(void) {
    return (uint32_t)((uintptr_t)&_micropy_hw_romfs_part0_start - (uintptr_t)&__flash_start);
}

static uint32_t vyomos_flash_size(void) {
    return (uint32_t)(uintptr_t)&_micropy_hw_romfs_part0_size;
}

static uint32_t vyomos_sector_size(void) {
    return qspiflash_config.sectorSize;
}

static mp_obj_t vyomos_bdev_readblocks(size_t n_args, const mp_obj_t *args) {
    mp_buffer_info_t bufinfo;
    mp_get_buffer_raise(args[2], &bufinfo, MP_BUFFER_WRITE);
    uint32_t offset = (uint32_t)mp_obj_get_int(args[1]) * vyomos_sector_size();
    if (n_args == 4) {
        offset += (uint32_t)mp_obj_get_int(args[3]);
    }
    if (((uint64_t)offset + bufinfo.len) > vyomos_flash_size()) {
        mp_raise_OSError(MP_EINVAL);
    }
    flash_read_block(vyomos_flash_base() + offset, bufinfo.buf, bufinfo.len);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_VAR_BETWEEN(vyomos_bdev_readblocks_obj, 3, 4, vyomos_bdev_readblocks);

static mp_obj_t vyomos_bdev_writeblocks(size_t n_args, const mp_obj_t *args) {
    mp_buffer_info_t bufinfo;
    mp_get_buffer_raise(args[2], &bufinfo, MP_BUFFER_READ);
    uint32_t offset = (uint32_t)mp_obj_get_int(args[1]) * vyomos_sector_size();
    if (n_args == 4) {
        offset += (uint32_t)mp_obj_get_int(args[3]);
    }
    if (((uint64_t)offset + bufinfo.len) > vyomos_flash_size()) {
        return MP_OBJ_NEW_SMALL_INT(-MP_EIO);
    }

    // Three-argument form erases the sector first. LittleFS erases through ioctl
    // and then calls the four-argument form to program inside that sector.
    if (n_args == 3) {
        status_t erase_status = flash_erase_sector(vyomos_flash_base() + offset);
        if (erase_status != kStatus_Success) {
            return MP_OBJ_NEW_SMALL_INT(-MP_EIO);
        }
    }

    status_t status = flash_write_block(vyomos_flash_base() + offset, bufinfo.buf, bufinfo.len);
    return MP_OBJ_NEW_SMALL_INT(status == kStatus_Success ? 0 : -MP_EIO);
}
static MP_DEFINE_CONST_FUN_OBJ_VAR_BETWEEN(vyomos_bdev_writeblocks_obj, 3, 4, vyomos_bdev_writeblocks);

static mp_obj_t vyomos_bdev_ioctl(mp_obj_t self_in, mp_obj_t cmd_in, mp_obj_t arg_in) {
    (void)self_in;
    mp_int_t cmd = mp_obj_get_int(cmd_in);
    switch (cmd) {
        case MP_BLOCKDEV_IOCTL_INIT:
            flash_init();
            return MP_OBJ_NEW_SMALL_INT(0);
        case MP_BLOCKDEV_IOCTL_DEINIT:
        case MP_BLOCKDEV_IOCTL_SYNC:
            return MP_OBJ_NEW_SMALL_INT(0);
        case MP_BLOCKDEV_IOCTL_BLOCK_COUNT:
            return MP_OBJ_NEW_SMALL_INT(vyomos_flash_size() / vyomos_sector_size());
        case MP_BLOCKDEV_IOCTL_BLOCK_SIZE:
            return MP_OBJ_NEW_SMALL_INT(vyomos_sector_size());
        case MP_BLOCKDEV_IOCTL_BLOCK_ERASE: {
            uint32_t offset = (uint32_t)mp_obj_get_int(arg_in) * vyomos_sector_size();
            if ((offset + vyomos_sector_size()) > vyomos_flash_size()) {
                return MP_OBJ_NEW_SMALL_INT(-MP_EINVAL);
            }
            status_t status = flash_erase_sector(vyomos_flash_base() + offset);
            return MP_OBJ_NEW_SMALL_INT(status == kStatus_Success ? 0 : -MP_EIO);
        }
        default:
            return mp_const_none;
    }
}
static MP_DEFINE_CONST_FUN_OBJ_3(vyomos_bdev_ioctl_obj, vyomos_bdev_ioctl);

static const mp_rom_map_elem_t vyomos_bdev_locals_dict_table[] = {
    { MP_ROM_QSTR(MP_QSTR_readblocks), MP_ROM_PTR(&vyomos_bdev_readblocks_obj) },
    { MP_ROM_QSTR(MP_QSTR_writeblocks), MP_ROM_PTR(&vyomos_bdev_writeblocks_obj) },
    { MP_ROM_QSTR(MP_QSTR_ioctl), MP_ROM_PTR(&vyomos_bdev_ioctl_obj) },
};
static MP_DEFINE_CONST_DICT(vyomos_bdev_locals_dict, vyomos_bdev_locals_dict_table);

MP_DEFINE_CONST_OBJ_TYPE(
    vyomos_bdev_type,
    MP_QSTR_Flash,
    MP_TYPE_FLAG_NONE,
    locals_dict, &vyomos_bdev_locals_dict
    );

static vyomos_bdev_obj_t vyomos_bdev = {
    .base = { &vyomos_bdev_type },
};

// LittleFS v2 keeps the 8-byte magic "littlefs" at offset 8 of a superblock.
// The root pair is the first two sectors. This is a raw flash read, so a
// leftover ROMFS image is never parsed.
static bool vyomos_has_lfs_magic(void) {
    static const uint8_t lfs_magic[8] = "littlefs";
    uint8_t found[8];
    uint32_t base = vyomos_flash_base();
    uint32_t sector = vyomos_sector_size();

    flash_read_block(base + 8, found, sizeof(found));
    if (memcmp(found, lfs_magic, sizeof(lfs_magic)) == 0) {
        return true;
    }
    if (vyomos_flash_size() > sector) {
        flash_read_block(base + sector + 8, found, sizeof(found));
        if (memcmp(found, lfs_magic, sizeof(lfs_magic)) == 0) {
            return true;
        }
    }
    return false;
}

// Create /vyomos on first boot, then mount it. mkfs erases the first two
// sectors only, the same path /flash uses. A failed mkfs or mount returns
// so boot.py still runs. Not the USB MSC medium.
void vyomos_fs_mount(void) {
    if (vyomos_flash_size() == 0 || vyomos_sector_size() == 0) {
        return;
    }

    flash_init();

    nlr_buf_t nlr;
    if (nlr_push(&nlr) != 0) {
        return;
    }

    mp_obj_t bdev = MP_OBJ_FROM_PTR(&vyomos_bdev);
    mp_obj_t vfs_mod = mp_import_name(MP_QSTR_vfs, mp_const_none, MP_OBJ_NEW_SMALL_INT(0));
    mp_obj_t lfs_type = mp_load_attr(vfs_mod, MP_QSTR_VfsLfs2);
    mp_obj_t mount_point = mp_obj_new_str("/vyomos", 7);
    mp_obj_t mount = mp_load_attr(vfs_mod, MP_QSTR_mount);

    if (!vyomos_has_lfs_magic()) {
        mp_call_function_1(mp_load_attr(lfs_type, MP_QSTR_mkfs), bdev);
    }
    mp_obj_t fs = mp_call_function_1(lfs_type, bdev);
    mp_call_function_2(mount, fs, mount_point);

    nlr_pop();
}
