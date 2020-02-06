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

    def __init__(self, host, port, credential, *args, **kwargs):
        exchange_name = kwargs.get('exchange_name', 'test_exchange')
        vhost = kwargs.get('vhost', '/')
        self.url: URL = URL('amqp', credential.get('username'),
                            credential.get('password'), host, port, vhost)
        self.establish_connection()
        self.create_channel()
        self.create_exchange(exchange_name)
        self.create_queue()

    def establish_connection(self):
        self.connection: Connection = Connection(
            transport=self.url.transport,
            userid=self.url.userid,
            password=self.url.password,
            hostname=self.url.host,
            port=self.url.port,
            virtual_host=self.url.vhost)

    def create_channel(self):
        self.channel = self.connection.channel()

    def create_exchange(self, exhcange_name, exchange_type='fanout'):
        self.exchange: Exchange = Exchange(
            exhcange_name, exchange_type, self.channel)
        self.exchange.declare()

    def create_queue(self, queue_name=''):
        self.queue: Queue = Queue(
            'turnstile', exchange=self.exchange, channel=self.channel)
        self.queue.declare()

    def get_consumers(self, _, channel):
        return [
            Consumer(self.channel, self.queue,
                     callbacks=[self.on_message], accept=['json'])
        ]

    def on_message(self, payload, message):
        print(f'RECEIVED: ${payload}')
        message.ack()
