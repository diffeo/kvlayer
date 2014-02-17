
import time
import logging
from functools import wraps


def retry(retry_exceptions=[Exception], retry_attempts=3):
    '''
    Configurable decorator that will retry after intermittent failures.

    @type retry_exceptions: list
    @param retry_exceptions: List of exceptions that will be retried.

    @type retry_attempts: integer
    @param retry_attempts: Number of times to retry a function after it has failed
    '''

    def decorator(method):
        ## This functools method just makes tracebacks easier to understand.
        @wraps(method)
        def wrapper(*args, **kwargs):
            tries = 0
            while 1:
                try:
                    tries += 1
                    return method(*args, **kwargs)
                    break
                except Exception, exc:
                    if (not any([isinstance(exc, e) for e in retry_exceptions]) or
                        tries > retry_attempts):
                        raise
                    else:
                        logging.warn('Retrying because of exception',  exc_info=True)
                        time.sleep(3 * tries)
        return wrapper
    return decorator
