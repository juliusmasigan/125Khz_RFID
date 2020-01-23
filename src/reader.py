import argparse
import asyncio
import evdev
import sys


def find_device(vid, pid, uid=None):
    devices = [evdev.InputDevice(path) for path in evdev.list_devices()]
    device = list(filter(
        lambda d: (d.info.vendor == int(vid, 16)
                   and d.info.product == int(pid, 16)) or d.uniq == uid,
        devices))

    if not device:
        print('No device found!')
        sys.exit()

    return device[0]


async def rfid_events(device: evdev.InputDevice):
    async for event in device.async_read_loop():
        # pylint: disable=fixme, no-member
        if event.type == evdev.ecodes.EV_KEY:
            key_event: evdev.KeyEvent = evdev.KeyEvent(event)
            if key_event.keystate == key_event.key_up:
                # Convert ASCII to char
                char = chr(key_event.scancode)
                print(char, key_event.scancode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--vendor_id',
                        help='Vendor ID of the device', required=True)
    parser.add_argument('-P', '--product_id',
                        help='Product ID of the device', required=True)
    parser.add_argument('--unique_id', help='Serial number of the device')
    args = parser.parse_args()

    vid = args.vendor_id
    pid = args.product_id
    uid = args.unique_id

    rfid = find_device(vid, pid, uid)

    # Get exclusive access to the device of the current process.
    rfid.grab()
    asyncio.ensure_future(rfid_events(rfid))

    try:
        loop = asyncio.get_event_loop()
        loop.run_forever()
    except KeyboardInterrupt:
        sys.exit()
    finally:
        rfid.ungrab()
