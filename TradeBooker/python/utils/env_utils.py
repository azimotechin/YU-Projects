import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s: %(message)s')

def get_logger() -> logging.Logger:
    return logging.getLogger()

def get_filename(callerDepth):
    import inspect
    frame = inspect.stack()[callerDepth + 1]
    return frame.filename

def is_running_on_desktop():
    import platform
    return platform.system() in ['Darwin', 'Windows']

def get_module_name(depth=1):
    import inspect
    frame = inspect.currentframe()
    
    for _ in range(depth):
        if frame is not None:
            frame = frame.f_back
    module = inspect.getmodule(frame.f_back)

    return module.__name__ if module else None
