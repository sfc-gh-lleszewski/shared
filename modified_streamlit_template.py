# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Example Streamlit App :coin:")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
    """
)

# Get the current credentials
session = get_active_session()

# Use an interactive slider to get user input
hifives_val = st.slider(
    "Number of high-fives in Q3",
    min_value=0,
    max_value=90,
    value=60,
    help="Use this to enter the number of high-fives you gave in Q3",
)

query=f"""
    SELECT uniform(1,10,random()) as gr, uniform(1, {hifives_val}, random()) as rand 
    FROM TABLE(GENERATOR(ROWCOUNT => 20)) v;
    """
results = session.sql(query).to_pandas()

# Create a simple bar chart
# See docs.streamlit.io for more types of charts
st.subheader("Number of high-fives")
st.bar_chart(data=results, x="GR", y="RAND")

st.subheader("Underlying data")
st.data_editor(results, use_container_width=True)
