import sys
import os
import inspect
import threading

import utils.streamlit_css as streamlit_css
import streamlit as st

from streamlit import config as _streamlit_config
from streamlit.web.bootstrap import run as _streamlit_run

import logging
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

def _is_server_launched():
    # We can also check if the thread was started on the main thread
    if os.environ.get('STREAMLIT_LAUNCHED', '0') == '1':
        return True
    else:
        launched_from_python_main = threading.current_thread() is threading.main_thread()
        if not launched_from_python_main:
            os.environ['STREAMLIT_LAUNCHED'] = '1'
            return True

    return False

def _start_streamlit_server(streamlit_main_filename, port=8501, config_options={}, headless=0):
    if headless is None:
        # Show the browser on Mac and Windows if no headless option is provided
        headless = not (sys.platform == "darwin" or sys.platform.startswith("win"))
        
    # Set the common Streamlit configuration options
    _streamlit_config.set_option('server.port', port)
    _streamlit_config.set_option('server.headless', headless)

    # Set the provided config_options
    for k in config_options:
        _streamlit_config.set_option(k, config_options[k])

    # Set the STREAMLIT_LAUNCHED flag
    os.environ['STREAMLIT_LAUNCHED'] = '1'

    # Run the Streamlit server (this will intentionally block the calling thread)
    _streamlit_run(streamlit_main_filename, args=[], flag_options=[], is_hello=False)

def _run_initializer_if_needed(initializer_callback):
    if initializer_callback and os.environ.get('STREAMLIT_INIALIZER', '0') == '0':
        # Run the initializer before starting the streamlit server
        os.environ['STREAMLIT_INIALIZER'] = '1'
        initializer_callback()

def _apply_page_defaults(page_title, page_icon):
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")

def launch_streamlit(page_title, main_callback, page_icon=":building_construction:", initializer_callback=None, port=8501, config_options={}, apply_page_defaults=True, apply_navbar=True, headless=None, main_callback_filename=None):
    if _is_server_launched():
        # The server is already initialized, we can run the main callback
        if apply_page_defaults:
            _apply_page_defaults(page_title, page_icon)
        
        if apply_navbar:
            streamlit_css.configure_navbar(page_title)

        icon = '✨'
        logging.info(f"{icon*8}  {page_title} - Request Start  {icon*8}")
        main_callback()
        logging.info(f"{icon*8}  {page_title} - Request Done   {icon*8} ")

    else:
        # This initialization is needed if the server is being started from a python main 
        _run_initializer_if_needed(initializer_callback)

        if main_callback_filename is None:
            # Default to the source file of the main_callback if not provided
            main_callback_filename = inspect.getsourcefile(main_callback)

        # Start the Streamlit server (blocking)
        _start_streamlit_server(main_callback_filename, port=port, headless=headless, config_options=config_options)

def test_streamlit_main():
    st.title(":star2: Hello, Streamlit!")

if __name__ == "__main__":
    # Test app main
    launch_streamlit('Test App', test_streamlit_main)