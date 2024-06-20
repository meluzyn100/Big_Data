import streamlit as st
import socket
import threading
import pandas as pd
import time
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("darkgrid")
load_dotenv()
st.title("Live Stock Price Dashboard")

empty = st.empty()

def main():
    
    while True:
        df = pd.read_csv('stock_prices.csv')
        df['Date'] = pd.to_datetime(df['Date'], utc=True)
        df = df.set_index('Date')
        with empty.container():
            st.line_chart(df, use_container_width=True)
            st.dataframe(df, use_container_width=True )
            # st.pyplot(fig)
        time.sleep(10)

if __name__ == "__main__":
    main()