import datetime
import logging
import sys
import traceback
from typing import Any

log_format = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'
# date_format = '%Y-%m-%d %H:%M:%S,%f'
date_format = None


class MyFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s


def configure_logs_default(error_level_loggers=None,
                           disable_stdout_console: bool = False,
                           catch_unhandled: bool = True):
    if error_level_loggers is None:
        error_level_loggers = []
    handlers = []
    if not disable_stdout_console:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(MyFormatter(fmt=log_format, datefmt=date_format))
        handlers.append(stdout_handler)

    logging.basicConfig(**{'level': logging.INFO, 'handlers': handlers})
    for logger in error_level_loggers:
        logging.getLogger(logger).setLevel(logging.ERROR)

    if catch_unhandled:
        def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
            """Handler for unhandled exceptions that will write to the logs"""
            if issubclass(exc_type, KeyboardInterrupt):
                # call the default excepthook saved at __excepthook__
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            logging.getLogger('Unhandled').critical("Unhandled exception",
                                                    exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = handle_unhandled_exception


def delegates(exception, to):
    def dm(method_name):
        def fwraps(self, *args, **kwargs):
            wrappedf = getattr(getattr(self, to), method_name)
            return wrappedf(*args, **kwargs)

        fwraps.__name__ = method_name
        return fwraps

    def cwraps(cls):
        for name in [f for f in dir(logging.Logger) if f not in exception and not f.startswith("_")]:
            setattr(cls, name, dm(name))
        return cls

    return cwraps


# Delegates all the methods to logging.logger except the method exception that is overwritten
@delegates(exception={'exception'}, to='logger')
class RaceLogger(logging.Logger):

    def __init__(self, name: str):
        super().__init__(name)
        configure_logs_default()
        self.logger = logging.getLogger(name)

    def exception(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        _exception = sys.exc_info()
        final_msg = f"{_exception[0].__name__} {_exception[1]}"
        self.error(f"{final_msg} {msg}", *args, **kwargs)
        lines = [line for line in traceback.format_exc().split("\n") if len(line.strip())]
        self.error(f"{msg} traceback {';'.join(lines)}", *args, **kwargs)
