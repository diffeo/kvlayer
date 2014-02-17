
import time
import pytest
from kvlayer._decorators import retry

def mocksleep(time):
    return


def test_retry_fails(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    @retry()
    def raise_forever():
        raise

    with pytest.raises(Exception):
        raise_forever()


def test_retry_succeeds(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    params = {'times_to_fail': 2}
    @retry()
    def eventually_succeed(params):
        if params['times_to_fail'] > 0:
            params['times_to_fail'] -= 1
            raise
        else:
            return

    eventually_succeed(params)


def test_retry_limited_exceptions(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    params = {'times_to_fail': 2}

    @retry([BufferError])
    def eventually_succeed(params):
        if params['times_to_fail'] > 0:
            params['times_to_fail'] -= 1
            raise BufferError
        else:
            return

    eventually_succeed(params)


def test_retry_limited_exceptions_fail(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    params = {'times_to_fail': 2}

    @retry([LookupError])
    def fail(params):
        if params['times_to_fail'] > 0:
            params['times_to_fail'] -= 1
            raise BufferError
        else:
            return

    with pytest.raises(BufferError):
        fail(params)


def test_retry_attempts_insufficient(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    params = {'times_to_fail': 4}

    @retry([BufferError])
    def fail(params):
        if params['times_to_fail'] > 0:
            params['times_to_fail'] -= 1
            raise BufferError
        else:
            return

    with pytest.raises(BufferError):
        fail(params)


def test_retry_attempts_configurable(monkeypatch):

    monkeypatch.setattr(time, 'sleep', mocksleep)

    params = {'times_to_fail': 3}

    @retry([BufferError], retry_attempts=3)
    def eventually_succeeds(params):
        if params['times_to_fail'] > 0:
            params['times_to_fail'] -= 1
            raise BufferError
        else:
            return

    eventually_succeeds(params)
