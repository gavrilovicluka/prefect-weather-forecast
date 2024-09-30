from datetime import datetime, timedelta
from prefect import flow, task
import requests
from geopy.geocoders import Nominatim
from prefect.blocks.system import JSON
from db_tasks.measurements_table_tasks import insert_measurements_data, setup_measurements_table
from db_tasks.predictions_table_tasks import insert_predictions_data, setup_predictions_table
from utils.time_utils import convert_utc_to_local_time
from prefect.server.schemas.schedules import IntervalSchedule

@task(retries=3)
def get_location_coordinates(location_name: str):

    locations_block = JSON.load("location-coordinates")
    coordinates_data = locations_block.value

    if location_name in coordinates_data:
        return coordinates_data[location_name]
    else:
        geolocator = Nominatim(user_agent="MyApp")
        location = geolocator.geocode(location_name)

        if location:
            coordinates_data[location_name] = (location.latitude, location.longitude)

            locations_block.value = coordinates_data
            locations_block.save(name="location-coordinates", overwrite=True)

            return (location.latitude, location.longitude)
        else:
            raise Exception(f"Location '{location_name}' not found.")


@task
def create_url(coordinates: tuple):
    base_url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    lat = f"lat={round(coordinates[0], 3)}"
    lon = f"lon={round(coordinates[1], 3)}"
    url = f"{base_url}?{lat}&{lon}"
    return url


@task(retries=3)
def fetch_data(url: str):
    headers = {
        "User-Agent": "MyApp/1.0 (myemail@example.com)"
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


@task
def process_data(data, is_current_measurement: bool):

    if is_current_measurement:
        data_instant_details = data["properties"]["timeseries"][0]["data"]["instant"]["details"]

        utc_datetime = data["properties"]["meta"]["updated_at"]
        local_datetime = convert_utc_to_local_time(utc_datetime)
        return {
            "datetime": local_datetime,
            "air_pressure_at_sea_level": data_instant_details["air_pressure_at_sea_level"],
            "air_temperature": data_instant_details["air_temperature"],
            "cloud_area_fraction": data_instant_details["cloud_area_fraction"],
            "relative_humidity": data_instant_details["relative_humidity"],
            "wind_from_direction": data_instant_details["wind_from_direction"],
            "wind_speed": data_instant_details["wind_speed"]
        }
    
    else:
        predictions_data = []
        for prediction in data["properties"]["timeseries"][1:60]:
            prediction_time = convert_utc_to_local_time(prediction["time"])
            prediction_details = prediction["data"]["instant"]["details"]

            predictions_data.append({
                "datetime": prediction_time,
                "air_pressure_at_sea_level": prediction_details["air_pressure_at_sea_level"],
                "air_temperature": prediction_details["air_temperature"],
                "cloud_area_fraction": prediction_details["cloud_area_fraction"],
                "relative_humidity": prediction_details["relative_humidity"],
                "wind_from_direction": prediction_details["wind_from_direction"],
                "wind_speed": prediction_details["wind_speed"]
            })

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
