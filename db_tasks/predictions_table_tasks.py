from prefect import task
from prefect_sqlalchemy import SqlAlchemyConnector

table_name = "yrno_predictions"
measurements_table_name = "yrno_measurements"

@task(retries=3)
def setup_predictions_table(block_name: str="postgres-connection-block"):

    with SqlAlchemyConnector.load(block_name) as connector:
        
        connector.execute(f"""
                          CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            measurement_id INT REFERENCES {measurements_table_name}(id),
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
def insert_predictions_data(measurement_id: int, processed_data: list, block_name: str="postgres-connection-block"):

    for prediction in processed_data:
        prediction["measurement_id"] = measurement_id

    with SqlAlchemyConnector.load(block_name) as connector:
        
        connector.execute_many(f"""
            INSERT INTO {table_name} (
                measurement_id, 
                datetime,
                air_pressure_at_sea_level,
                air_temperature,
                cloud_area_fraction,
                relative_humidity,
                wind_from_direction,
                wind_speed
            )
            VALUES (
                :measurement_id, 
                :datetime, 
                :air_pressure_at_sea_level, 
                :air_temperature, 
                :cloud_area_fraction,
                :relative_humidity,
                :wind_from_direction,
                :wind_speed
            );
        """,
        seq_of_parameters=processed_data)
        