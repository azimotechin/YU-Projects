
import streamlit as st
import pandas as pd
import numpy as np
import datetime

import utils.streamlit_utils as stu

import utils.env_utils as envu
logger = envu.get_logger()


def make_data_frame():
    # Create sample data
    data = {
        'Name': [f'Item {i}' for i in range(10)],
        'Category': ['A','B','C','D','E'] * 2,
        'Value1': np.random.randint(1, 100, 10),
        'Value2': np.random.uniform(0, 1, 10),
        'Value3': np.random.normal(50, 10, 10)
    }
    return pd.DataFrame(data)

def main():
    logger.info("Creating tabs...")
    tabs = st.tabs(["Tab 1", "Tab 2"])
    with tabs[0]:
        st.dataframe(make_data_frame())

    with tabs[1]:
        btn_clicked = st.button("Click me!")
        if btn_clicked:
            st.write("Button clicked!")

    current_time = datetime.datetime.now().strftime("%H:%M:%S")
    st.markdown(f"**Current time:** {current_time}")

if __name__ == "__main__":
    stu.launch_streamlit('Hello World', main)