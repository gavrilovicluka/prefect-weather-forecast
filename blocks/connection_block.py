from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from prefect.blocks.system import Secret

db_username = Secret.load("postgres-username")
db_password = Secret.load("postgres-password")

connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.POSTGRESQL_PSYCOPG2,
        username=db_username.get(),
        password=db_password.get(),
        host="localhost",
        port="5432",
        database="yrno_weather_db"
    )
)

connector.save("postgres-connection-block", overwrite=True)
