# logger_ext.py
import signal
import logging
import logging.handlers
from multiprocessing import Queue, Process
import sys

# Logging from multiprocess, check https://docs.python.org/3/howto/logging-cookbook.html
log_queue = Queue()

def logger_runloop(log_file:str=None, console: bool = True):
    root = logging.getLogger()
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    if log_file:
        h = logging.FileHandler(log_file)
        f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        h.setFormatter(f)
        root.addHandler(h)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(f)
        console_handler.setLevel(logging.INFO)
        root.addHandler(console_handler)
    root.setLevel(logging.INFO)
    while True:
        print('++++++++++++++++++++++++++++')
        log = log_queue.get()
        print(log)
        if log is None:
            break
        logger = logging.getLogger(log.name)
        logger.handle(log)

def end_log():
    print('end_log')
    log_queue.put(None)

def get_logger(logger_name:str = '') -> logging.Logger:
    root = logging.getLogger()
    if len(root.handlers) == 0:
        qh = logging.handlers.QueueHandler(log_queue)
        root.addHandler(qh)
        root.setLevel(logging.INFO)
    else:
        print('========================')
    return logging.getLogger(logger_name)

logger_process = None

def init_logger(log_file:str=None, console:bool=True):
    global logger_process
    if logger_process is None:
        logger_process = Process(target=logger_runloop, args=(log_file, console))
        logger_process.start()

def signal_handler(sig, frame):
    global logger_process
    end_log()
    if logger_process is not None:
        logger_process.join()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

"""
程序结束前, 需要调用
```
end_log()
logger_process.join()
```
"""
def end():
    end_log()
    logger_process.join()