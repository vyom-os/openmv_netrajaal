import sys
import machine
import utime
import uasyncio as asyncio
import logger
from utils import reboot_device, print_exception

SUP_MAX_CONSECUTIVE_CRASHES = 10
SUP_REBOOT_LOG_TIMEOUT_SEC = 10


async def supervised(
    name,
    fn,
    critical=False,
    delay=5,
    max_crashes=SUP_MAX_CONSECUTIVE_CRASHES,
):
    crashes = 0
    while True:
        started = utime.ticks_ms()
        try:
            await fn()
            logger.fatal(f"[SUP] `{name}` exited unexpectedly")
        except Exception as e:
            logger.fatal(f"[SUP] `{name}` crashed: {e}")
            print_exception(e)

        ran_ms = utime.ticks_diff(utime.ticks_ms(), started)
        logger.info(f"[SUP] `{name}` ran for {ran_ms} ms")
        crashes += 1

        if crashes >= max_crashes:
            if critical:
                logger.fatal(
                    f"[SUP] {name} failed {crashes} times in a row, resetting machine"
                )
                try:
                    # Save logs, but don't let a dead SD card block the reset
                    await asyncio.wait_for(reboot_device(), SUP_REBOOT_LOG_TIMEOUT_SEC)
                except Exception:
                    print_exception()
                    pass
                machine.reset()
            else:
                logger.fatal(f"[SUP] {name} failed {crashes} times in a row, exiting loop...")
                break
        else:
            logger.warning(
                f"[SUP] {name} failure {crashes}/{max_crashes} (ran {ran_ms} ms), restarting loop..."
            )

        await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Standalone test — run this file on the OpenMV board
# ---------------------------------------------------------------------------
# inf_background_loop ticks every 10s, then raises a mock error on every 5th
# iteration, catches it, and breaks. supervised() should start it again.
TEST_LOOP_INTERVAL_SEC = 10


async def inf_background_loop():
    iteration = 0
    logger.info("[TEST] inf_background_loop started")
    while True:
        try:
            iteration += 1
            msg = f"[TEST] inf_background_loop iteration {iteration}"
            logger.info(msg)
            if iteration % 5 == 0:
                raise RuntimeError(f"mock error on iteration {iteration}")
            await asyncio.sleep(TEST_LOOP_INTERVAL_SEC)
        except Exception as e:
            print_exception()
            logger.error(f"[TEST] inf_background_loop breaking: {e}")
            break
    logger.warning("[TEST] inf_background_loop completed \n\n")


async def main():
    asyncio.create_task(supervised("inf_loop", inf_background_loop))
    # asyncio.create_task(inf_background_loop)
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        print("꩜꩜꩜꩜꩜꩜ main loop completed ** ꩜꩜꩜꩜꩜꩜")
    except KeyboardInterrupt:
        print_exception()
        print("꩜꩜꩜꩜꩜꩜ stopped by user via keyboard interrupt ꩜꩜꩜꩜꩜꩜")
    except Exception as e:
        print(f"error... {e}")
        print_exception(e)
    finally:
        print("꩜꩜꩜꩜꩜꩜ SHUTTING DOWN, and restarting the device... ꩜꩜꩜꩜꩜꩜")
        machine.reset()
