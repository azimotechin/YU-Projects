import os
import fastapi
import fastapi.staticfiles as staticfiles

import utils.env_utils as envu
from uvicorn import run as uvicorn_run

import logging
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

def create_fastapi(callerFileName=None, htmlMount=True, htmlSubdirectory='html') -> fastapi.FastAPI:
    app = fastapi.FastAPI()

    if callerFileName is None:
        filename = envu.get_filename(1)
    else:
        filename = callerFileName

    dir = os.path.abspath(os.path.join(os.path.dirname(filename), htmlSubdirectory))

    if os.path.exists(dir):
        # We can mount the requested directory since it exists
        app.mount('/html', staticfiles.StaticFiles(directory=dir, html=True), name='html')

    return app

def run_server(callerFastAPIInstanceName='app', setupEnv=True, host='0.0.0.0', port=8080, reloadOnFileChange=True, desktop_workers=2, server_workers=4):
    workers = desktop_workers if envu.is_running_on_desktop() else server_workers
    workers = int(os.environ.get('workers', str(workers)))
    reload = reloadOnFileChange if envu.is_running_on_desktop() else False

    applicationInstanceName = envu.get_module_name() + ':' + callerFastAPIInstanceName

    uvicorn_run(applicationInstanceName,
                workers=workers, reload=reload, reload_delay=.5,
                host=host, port=port, log_level='debug')