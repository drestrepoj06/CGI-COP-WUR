import streamlit as st

st.title("Hello from Streamlit in Docker!")
st.write("This is running inside a Docker container.")

x = st.slider("Select a value")
st.write(x, "squared is", x * x)

st.write("Test")