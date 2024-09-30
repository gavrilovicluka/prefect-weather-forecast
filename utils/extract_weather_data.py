def extract_weather_details(datetime, data_point):
    return {
            "datetime": datetime,
            "air_pressure_at_sea_level": data_point["air_pressure_at_sea_level"],
            "air_temperature": data_point["air_temperature"],
            "cloud_area_fraction": data_point["cloud_area_fraction"],
            "relative_humidity": data_point["relative_humidity"],
            "wind_from_direction": data_point["wind_from_direction"],
            "wind_speed": data_point["wind_speed"]
        }