import logging
import random
import time
from functools import wraps


def sleep_expo_gen(initial, maximum, factor):
    """Generator for exponential delay.
     Args:
         initial: The mathematical base of the exponentiation operation.
         factor: Factor to multiply the exponentiation by.
         maximum: The maximum value to yield.
     """
    n = 0
    delay = min(initial, maximum)
    while True:
        yield delay
        delay = min(factor * (2 ** n) + random.random(), maximum)
        n += 1


def on_exception(
        exception,
        start_sleep_time,
        factor=2,
        border_sleep_time=10,
        max_retries=10,
):
    def retry_exception(target):
        # Декорируем с помощью wraps,
        # чтобы не потерять описание декорируемой функции.
        @wraps(target)
        def retry(*args, **kwargs):
            step = 0
            sleep_gen = sleep_expo_gen(start_sleep_time, border_sleep_time, factor)
            while True:
                try:
                    res = target(*args, **kwargs)
                except exception as err:
                    logging.exception(err)
                    if max_retries <= step:
                        logging.info("Backoff retries the maximum number of steps has been reached")
                        raise err
                    step += 1
                    sleep_time = next(sleep_gen)
                    logging.info("Backoff retries %i step sleep %.2f sec", step, sleep_time)
                    time.sleep(sleep_time)
                else:
                    break
            return res
        return retry
    return retry_exception
