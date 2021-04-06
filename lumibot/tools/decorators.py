import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import lru_cache, wraps


def staticdecorator(func):
    """Makes a function decorated with staticmethod executable"""
    return func.__get__("")


def call_function_get_frame(func, *args, **kwargs):
    """
    Calls the function *func* with the specified arguments and keyword
    arguments and snatches its local frame before it actually executes.
    """

    frame = None
    trace = sys.gettrace()

    def snatch_locals(_frame, name, arg):
        nonlocal frame
        if frame is None and name == "call":
            frame = _frame
        return trace

    sys.settrace(snatch_locals)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(trace)
    return frame, result


def snatch_locals(store):
    """Snatch a function local variables
    and store them in store variable"""

    def wrapper(func_input):
        @wraps(func_input)
        def func_output(*args, **kwargs):
            global store
            frame, result = call_function_get_frame(func_input, *args, **kwargs)
            store = frame.f_locals
            return result

        return func_output

    return wrapper


def append_locals(func_input):
    """Snatch a function local variables
    and store them in store variable"""

    @wraps(func_input)
    def func_output(*args, **kwargs):
        frame, result = call_function_get_frame(func_input, *args, **kwargs)
        func_output.locals = frame.f_locals
        return result

    return func_output


def execute_after(actions):
    def decorator_func(input_func):
        @wraps(input_func)
        def output_func(*args, **kwargs):
            input_func(*args, **kwargs)
            for action in actions:
                action()

        return output_func

    return decorator_func


def datetime_cache(dt):
    def _wrapper(f):
        if dt.tzinfo is None:
            raise ValueError(
                "datetime_cache decorator takes only aware datetime as parameter"
            )

        next_update = dt.replace(tzinfo=timezone.utc)
        f = lru_cache(None)(f)

        @wraps(f)
        def _wrapped(*args, **kwargs):
            nonlocal next_update
            now = datetime.utcnow()
            if now >= next_update:
                f.cache_clear()
                next_update = next_update + timedelta(days=1)

            print("Here")
            return f(*args, **kwargs)

        return _wrapped

    return _wrapper()
