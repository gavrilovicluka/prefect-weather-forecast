from datetime import timedelta
from prefect import flow, task
import requests
from geopy.geocoders import Nominatim
from prefect.blocks.system import JSON
from db_tasks.measurements_table_tasks import insert_measurements_data, setup_measurements_table
from db_tasks.predictions_table_tasks import insert_predictions_data, setup_predictions_table
from utils.extract_weather_data import extract_weather_details
from utils.time_utils import convert_utc_to_local_time
from prefect.tasks import task_input_hash


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_location_coordinates(location_name: str):

    locations_block = JSON.load("location-coordinates")
    coordinates_data = locations_block.value

    if location_name not in coordinates_data:
        geolocator = Nominatim(user_agent="WeatherForecastApp")
        location = geolocator.geocode(location_name)
        if location:
            coordinates_data[location_name] = (location.latitude, location.longitude)
            locations_block.value = coordinates_data
            locations_block.save(name="location-coordinates", overwrite=True)
        else:
            raise ValueError(f"Location '{location_name}' not found.")

    return coordinates_data[location_name]


@task
def create_url(coordinates: tuple):
    base_url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    lat = f"lat={round(coordinates[0], 3)}"
    lon = f"lon={round(coordinates[1], 3)}"
    url = f"{base_url}?{lat}&{lon}"
    return url


@task(retries=3)
def fetch_data(url: str):
    try:
        headers = {"User-Agent": "WeatherForecastApp/1.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"API request failed: {e}") 


@task
def process_data(data, is_current_measurement: bool):

    if is_current_measurement:
        data_instant_details = data["properties"]["timeseries"][0]["data"]["instant"]["details"]

        utc_datetime = data["properties"]["meta"]["updated_at"]
        local_datetime = convert_utc_to_local_time(utc_datetime)

        return extract_weather_details(local_datetime, data_instant_details)
    
    else:
        predictions_data = []
        for prediction in data["properties"]["timeseries"][1:60]:
            prediction_time = convert_utc_to_local_time(prediction["time"])
            prediction_details = prediction["data"]["instant"]["details"]

            predictions_data.append(extract_weather_details(prediction_time, prediction_details))

        return predictions_data
        


@flow(log_prints=True)
def get_weather(insert_data: bool = True, location: str = "Niš"):
    coordinates = get_location_coordinates(location)
    url = create_url(coordinates)
    data = fetch_data(url)
    processed_measurement_data = process_data(data, True)
    processed_predictions_data = process_data(data, False)

    if insert_data:
        setup_measurements_table()
        setup_predictions_table()

        measurement_id = insert_measurements_data(location, processed_measurement_data)
        insert_predictions_data(measurement_id, processed_predictions_data)

    return processed_measurement_data, processed_predictions_data



if __name__ == "__main__":
    get_weather.serve(
        name="weather-deployment",
        interval=60,
        description="Getting weather data every one minute",
    )
