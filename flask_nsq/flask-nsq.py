# -*- coding: utf-8 -*-
from werkzeug.datastructures import ImmutableDict
import importlib
import imp


class NSQ(object):
    default_config = ImmutableDict({
        'NSQ_CLIENT_TYPE': None,
        'NSQ_ADDRESS': 'localhost',
        'NSQ_DAEMON_PORT': 4150,
        'NSQ_READER_PORT': 4151
    })

    supported_clients = ['gnsq', 'pynsq']

    def __init__(self, app=None, daemon_config=None):
        self.app = app
        if app is not None:
            self.init_app(app, daemon_config)

    def init_app(self, app):
        for key, value in self.default_config:
            app.config.setdefault(key, value)

        if app.config['CLIENT_TYPE'] not in self.supported_clients:
            raise Exception('Invalid NSQ client library, valid values are %s, %s' % (
                self.supported_clients[0], self.supported_clients[1]))

        if app.config['CLIENT_TYPE'] == 'gnsq':
            client = GNSQ()
        elif app.config['CLIENT_TYPE'] == 'pynsq':
            client = PYNSQ

        # setup a new connection to the daemon
        daemon = client.daemon(app.config['NSQ_ADDRESS'], app.config['NSQ_DAEMON_PORT'],
                               daemon_config)

        if not hasattr(app, 'extensions'):
            app.extensions = {}

        app.extensions.setdefault('nsq', {})
        app.extensions['nsq']['client'] = client
        app.extensions['nsq']['daemon'] = daemon

    @property
    def client(self):
        app = self.app or current_app
        return app.extensions['nsq']['client']

    @property
    def daemon(self):
        app = self.app or current_app
        return app.extensions['nsq']['daemon']

    def create_reader(self, topic, channel, reader_config):
        app = self.app or current_app
        reader = self.client.reader(app.config['NSQ_ADDRESS'],
                                    app.config['NSQ_READER_PORT'],
                                    topic, channel, reader_config)

    def _store_reader(self, topic, channel, reader):
        ext = (self.app or current_app).extensions['nsq']
        if not ext.get(topic):
            ext.setdefault(topic, {})

        ext[topic][channel] = reader

    def get_reader(self, topic, channel):
        ext = (self.app or current_app).extensions['nsq']
        return ext[topic][channel]


class NSQClient(object):
    def __init__(self, client_name):
        # check of existense of client
        try:
            imp.find_module(client_name)
        except Exception:
            raise Exception('Unable to find NSQ client library %s', % (client_name))


class GNSQ(NSQClient):
    def __init__(self):
        super(GNSQ, self).__init__('gnsq')

    @classmethod
    def daemon(cls, address, port, daemon_config):
        import gnsq.Nsqd
        if kwargs.get('address'):
            add = kwargs.pop('address')
        else:
            add = address

        if kwargs.get('http_port'):
            p = kwargs.pop('http_port')
        else:
            p = port

        return Nsqd(add, http_port=pt, **kwargs)

    @classmethod
    def reader(cls, address, port, topic, channel, reader_config):
        import gnsq.Reader
        if reader_config.get('nsqd_tcp_addresses'):
            add = reader_config.pop('nsqd_tcp_addresses')
        else:
            add = '%s:%s' % (address, port)
        return Reader(topic, channel, add, reader_config)


class PYNSQ(NSQClient):
    def __init__(self):
        super(GNSQ, self).__init__('nsq')
