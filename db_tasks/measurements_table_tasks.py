from prefect import task
from prefect_sqlalchemy import SqlAlchemyConnector

table_name = "yrno_measurements"

@task(retries=3)
def setup_measurements_table(block_name: str="postgres-connection-block"):
    with SqlAlchemyConnector.load(block_name) as connector:

        connector.execute(f"""
                          CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            location VARCHAR(50),
                            datetime TIMESTAMP,
                            air_pressure_at_sea_level FLOAT,
                            air_temperature FLOAT,
                            cloud_area_fraction FLOAT,
                            relative_humidity FLOAT,
                            wind_from_direction FLOAT,
                            wind_speed FLOAT
                          );
                    """)


@task(retries=3)
def insert_measurements_data(location: str, processed_data: dict, block_name: str="postgres-connection-block") -> int:

    with SqlAlchemyConnector.load(block_name) as connector:\
        
        result = connector.execute(f"""
            INSERT INTO {table_name} (
                location, 
                datetime,
                air_pressure_at_sea_level,
                air_temperature,
                cloud_area_fraction,
                relative_humidity,
                wind_from_direction,
                wind_speed
            )
            VALUES (
                :location, 
                :datetime, 
                :air_pressure_at_sea_level, 
                :air_temperature, 
                :cloud_area_fraction,
                :relative_humidity,
                :wind_from_direction,
                :wind_speed
            )
            RETURNING id;
        """, 
        parameters={
            "location": location,
            "datetime": processed_data['datetime'],
            "air_pressure_at_sea_level": processed_data['air_pressure_at_sea_level'],
            "air_temperature": processed_data['air_temperature'],
            "cloud_area_fraction": processed_data['cloud_area_fraction'],
            "relative_humidity": processed_data['relative_humidity'],
            "wind_from_direction": processed_data['wind_from_direction'],
            "wind_speed": processed_data['wind_speed']
        })

        measurement_id = result.fetchone()[0]
        return measurement_id