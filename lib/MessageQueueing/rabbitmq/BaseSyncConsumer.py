from kombu import Connection, Exchange, Queue, Consumer
from kombu.mixins import ConsumerMixin
from typing import NamedTuple


class URL(NamedTuple):
    transport: str
    userid: str
    password: str
    host: str
    port: int
    vhost: str


class BaseSyncConsumer(ConsumerMixin):

    def __init__(self, host, port, credential, queue_name, *args, **kwargs):
        exchange_name = kwargs.get('exchange_name', 'test_exchange')
        vhost = kwargs.get('vhost', '/')
        self.url: URL = URL('amqp', credential.get('username'),
                            credential.get('password'), host, port, vhost)
        self.callbacks = kwargs.get('callbacks')
        self.establish_connection()
        self.create_exchange(exchange_name)
        self.create_queue(queue_name)
        # self.db_conn

    def establish_connection(self):
        self.connection: Connection = Connection(
            transport=self.url.transport,
            userid=self.url.userid,
            password=self.url.password,
            hostname=self.url.host,
            port=self.url.port,
            virtual_host=self.url.vhost)
        self.connection.connect()

        return self.connection

    def create_exchange(self, exhcange_name, exchange_type='fanout'):
        self.exchange: Exchange = Exchange(
            exhcange_name, exchange_type, self.connection.channel())
        self.exchange.declare()

    def create_queue(self, queue_name=''):
        self.queues: List[Queue] = [
            Queue(queue_name, exchange=self.exchange),
        ]

    def get_consumers(self, _, channel):
        default_channel = self.connection.default_channel
        return [
            # Consumer(default_channel, self.queues,
            #          callbacks=self.callbacks, accept=['json'])
            Consumer(default_channel, self.queues,
                     on_message=self.on_message, accept=['json'])
        ]

    def on_message(self, message):
        self.callbacks[0](message.decode(), message, {'extra': 'args'})
