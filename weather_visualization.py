import streamlit as st
import pandas as pd

from weather_flow import get_weather

@st.cache_data(ttl=60*60, show_spinner="Fetching weather forecast data...")
def get_data(location: str):
    return get_weather(insert_data=False, location=location)


def main():
    st.title("Weather forecast")

    location = st.text_input("City")

    if location:
        measurements, predictions = get_data(location)

        if measurements:
            show_measurements = st.expander(label = 'Current measurements')
            with show_measurements:
                df_measurements = pd.DataFrame([measurements])
                st.dataframe(df_measurements)
        else:
            st.error("Failed to fetch measurements data.")

        if predictions:
            show_predictions = st.expander(label = 'Predictions')
            df_predictions = pd.DataFrame(predictions)
            with show_predictions:
                st.dataframe(df_predictions)
            
            st.bar_chart(
                df_predictions,
                x="datetime",
                y="air_temperature"
           )
        else:
            st.error("Failed to fetch predictions data.")   
    
    
if __name__ == '__main__':
    main()