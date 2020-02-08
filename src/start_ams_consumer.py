import sys
import logging
import yaml

from yaml import Loader
from datetime import datetime
from lib.MessageQueueing.rabbitmq.BaseSyncConsumer import BaseSyncConsumer


yml_handle = open('./etc/config.yaml', 'r')
config = yaml.load(yml_handle, Loader=Loader)

FORMAT = '%(name)-10s <%(levelname)s>: %(asctime)s - %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'
logging.basicConfig(format=FORMAT, datefmt=DATETIME_FORMAT,
                    level=logging.DEBUG)
logger = logging.getLogger('RFIDReader')

# Enable a file logger
# c_handler = logging.FileHandler()
# c_handler.setLevel(logging.ERROR)
# logger.addHandler(c_handler)


def on_message(payload, message, *args, **kwargs):
    print(f'Saving in DB {payload}')
    message.ack()


if __name__ == "__main__":
    logger.info('Application bootstrapped')

    try:
        # Create a mq worker
        logger.info('Initiating MQ consumer...')
        mq_config = config.get('rabbit_mq')
        worker = BaseSyncConsumer(
            mq_config.get('host'), mq_config.get('port'),
            mq_config.get('credential'), vhost=mq_config.get('vhost'),
            exchange_name=mq_config.get('exchange_name'),
            queue_name='attendance_system',
            callbacks=[on_message])
        logger.info('Successfully connected to rabbitmq server')

        worker.run()
    except IOError as e:
        if e.errno == 111:
            logger.error('Unable to connect to the rabbitmq server')
    except KeyboardInterrupt:
        logger.info('Gracefully shutting down the application...')
        sys.exit()
