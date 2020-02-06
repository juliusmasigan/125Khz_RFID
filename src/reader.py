import argparse
import asyncio
import evdev
import sys
import logging
import pytz
import redis
import yaml
import json

from yaml import Loader
from datetime import datetime
from lib.MessageQueueing.rabbitmq.BaseSyncPublisher import BaseSyncPublisher


stream = open('./etc/config.yaml', 'r')
config = yaml.load(stream, Loader=Loader)

FORMAT = '%(name)-10s <%(levelname)s>: %(asctime)s - %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'
logging.basicConfig(format=FORMAT, datefmt=DATETIME_FORMAT,
                    level=logging.DEBUG)
logger = logging.getLogger('RFIDReader')

SPOOF_KEY = "{}:SPOOFPROTECT"
TAP_KEY = "{}:TAPSEQ"

# Enable a file logger
# c_handler = logging.FileHandler()
# c_handler.setLevel(logging.ERROR)
# logger.addHandler(c_handler)


def find_device():
    global config
    pcd = config.get('pcd', {})
    vid = pcd.get('vendor')
    pid = pcd.get('product')
    uid = pcd.get('serial')

    devices = [evdev.InputDevice(path) for path in evdev.list_devices()]
    device = list(filter(
        lambda d: (d.info.vendor == int(vid, 16)
                   and d.info.product == int(pid, 16)) or d.uniq == uid,
        devices))

    if not device:
        logger.error('No device found!')
        sys.exit()

    logger.info(f'Device found <vendor={vid}, product={pid}, serial={uid}>')
    return device[0]


async def rfid_events(device, redis_con, publisher):
    event_codes = []
    try:
        async for event in device.async_read_loop():
            # pylint: disable=fixme, no-member
            if event.type == evdev.ecodes.EV_KEY and\
                    event.value == evdev.KeyEvent.key_up:

                if event.code == evdev.ecodes.KEY_ENTER:
                    logger.info(f'Captured device events {event_codes}')
                    rfid_code = await parse_event_codes(tuple(event_codes))
                    await enqueue_rfid(redis_con, publisher, rfid_code)
                    event_codes = []
                else:
                    event_codes.append(event.code)
    except OSError:
        logger.critical('The device got detached!')
        loop = asyncio.get_event_loop()
        logger.info('Stopping application...')
        loop.stop()


async def parse_event_codes(codes):
    scancodes = {
        # Scancode: ASCIICode
        0: None, 1: u'ESC', 2: u'1', 3: u'2', 4: u'3', 5: u'4', 6: u'5', 7: u'6', 8: u'7', 9: u'8',
        10: u'9', 11: u'0', 12: u'-', 13: u'=', 14: u'BKSP', 15: u'TAB', 16: u'Q', 17: u'W', 18: u'E', 19: u'R',
        20: u'T', 21: u'Y', 22: u'U', 23: u'I', 24: u'O', 25: u'P', 26: u'[', 27: u']', 28: u'CRLF', 29: u'LCTRL',
        30: u'A', 31: u'S', 32: u'D', 33: u'F', 34: u'G', 35: u'H', 36: u'J', 37: u'K', 38: u'L', 39: u';',
        40: u'"', 41: u'`', 42: u'LSHFT', 43: u'\\', 44: u'Z', 45: u'X', 46: u'C', 47: u'V', 48: u'B', 49: u'N',
        50: u'M', 51: u',', 52: u'.', 53: u'/', 54: u'RSHFT', 56: u'LALT', 100: u'RALT'
    }

    return tuple([scancodes.get(code) for code in codes])


async def enqueue_rfid(redis_con, publisher, rfid_code):
    rfid_code = ''.join(rfid_code)
    if is_spoof(redis_con, rfid_code):
        return

    tap_records = redis_con.lrange(TAP_KEY.format(rfid_code), 0, -1)
    payload = {
        'piccId': rfid_code, # Proximity integrated circuit card id
        'taps': tap_records
    }
    publisher.publish(payload)


def is_spoof(redis_con, key) -> bool:
    global config
    spoof_protect_key = SPOOF_KEY.format(key)
    tap_key = TAP_KEY.format(key)
    if redis_con.exists(spoof_protect_key):
        return True

    tz = pytz.timezone(config.get('timezone'))
    now = datetime.now(tz=tz)
    midnight = tz.localize(datetime(now.year, now.month, now.day, 23, 59))
    spoof_tolerance = config.get('spoof_tolerance')

    pipe = redis_con.pipeline()
    pipe.set(spoof_protect_key, 'enabled')
    pipe.expire(spoof_protect_key, spoof_tolerance.get('duration'))
    pipe.rpush(tap_key, now.isoformat())
    pipe.expireat(tap_key, int(midnight.timestamp()))
    pipe.execute()
    return False


if __name__ == "__main__":
    logger.info('Application bootstrapped')

    logger.info('Finding device...')
    rfid = find_device()

    try:
        # Connect to redis
        logger.info('Connecting to redis server...')
        redis_config = config.get('redis')
        redis_con = redis.Redis(
            redis_config.get('host'), redis_config.get('port'),
            decode_responses=True)
        redis_con.ping()
        logger.info('Successfully connected to redis server')

        # Create a mq publisher
        logger.info('Initiating MQ publisher...')
        mq_config = config.get('rabbit_mq')
        mq_publisher = BaseSyncPublisher(
            mq_config.get('host'), mq_config.get('port'),
            mq_config.get('credential'), vhost=mq_config.get('vhost'),
            exchange_name=mq_config.get('exchange_name'))
        logger.info('Successfully connected to rabbitmq server')

        # Get exclusive access to the device of the current process.
        logger.info('Getting an exclusive access to the device...')
        rfid.grab()
        logger.info('Successfully got an exclusive access')

        # Listen for device events
        logger.info('Listening for device events...')
        asyncio.ensure_future(rfid_events(rfid, redis_con, mq_publisher))
        loop = asyncio.get_event_loop()
        loop.run_forever()
    except IOError as e:
        if e.errno == 16:
            logger.error(
                'The process still had the exclusive access to the device')
        if e.errno == 111:
            logger.error('Unable to connect to the rabbitmq server')
    except redis.connection.ConnectionError:
        logger.error('Unable to connect to the redis server')
    except KeyboardInterrupt:
        logger.info('Releasing exclusive access to the device...')
        rfid.ungrab()
        logger.info('Gracefully shutting down the application...')
        sys.exit()
