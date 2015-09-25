#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_flask-nsq
----------------------------------

Tests for `flask-nsq` module.
"""

import pytest
from flask_nsq import Nsq
from flask import Flask
from integration_server import NsqdIntegrationServer
from gnsq import protocol as nsq
from gnsq import states, errors


def test_unset_clientlibrary():
    app = Flask(__name__)
    with pytest.raises(Exception):
        Nsq(app)


@pytest.fixture(scope='function')
def flaskapp():
    app = Flask(__name__)
    app.config['NSQ_CLIENT_TYPE'] = 'gnsq'
    return app


def test_daemon_connection(flaskapp):
    print flaskapp.config['NSQ_CLIENT_TYPE']
    with NsqdIntegrationServer() as server:
        config = {
            'address': server.address,
            'http_port': server.http_port
        }

        client = Nsq(flaskapp, config)
        client.daemon.connect()
        assert client.daemon.state == states.CONNECTED

        client.daemon.close()
        frame, error = client.daemon.read_response()
        assert frame == nsq.FRAME_TYPE_ERROR
        assert isinstance(error, errors.NSQInvalid)

