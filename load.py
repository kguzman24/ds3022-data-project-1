import duckdb
import os
import logging
from pathlib import Path

logging.basicConfig(
    filename = "taxi.log",
    encoding = "utf-8",
    filemode = "a",
    format = "{asctime} - {levelname} - {message}",
    style = "{",
    datefmt = "%Y-%m-%d %H:%M",
    level = "DEBUG"
)
logger = logging.getLogger(__name__)
#logging.info("Logging is set up.")

script_dir = Path(__file__).resolve().parent
data_dir   = script_dir / "data"
yellow_glob = (data_dir / "yellow_tripdata_2024-*.parquet").as_posix()
green_glob  = (data_dir / "green_tripdata_2024-*.parquet").as_posix()
emissions_csv = (data_dir / "vehicle_emissions.csv").as_posix()

#loading and aggragating specific .parquet files with only needed columns
def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #loading multiple file sources programatically, instead of statically
        con.execute(f"""
            CREATE OR REPLACE TABLE yellow_2024 AS
            SELECT 'yellow' as color,
            VendorID,
            passenger_count, 
            trip_distance, 
            tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime
            FROM read_parquet('{yellow_glob}');
        """)
        con.execute(f"""
            CREATE OR REPLACE TABLE green_2024 AS
            SELECT 
            'green' as color,
            VendorID,
            passenger_count, 
            trip_distance, 
            lpep_pickup_datetime AS pickup_datetime, lpep_dropoff_datetime AS dropoff_datetime
            FROM read_parquet('{green_glob}', union_by_name = true);
        """)

        #aggregation to one table by joining on name
        con.execute("""
        CREATE OR REPLACE TABLE trips_2024 AS 
        SELECT * FROM yellow_2024 UNION ALL
        SELECT * FROM green_2024

        """)

        #loading emissions table into separate duckdb table
        con.execute("""
        CREATE OR REPLACE TABLE emissions_lookup AS
        SELECT * FROM read_csv('{emissions_csv}')
        """)


        #print statements for rows of each table 
        print("Rows trips_2024: ", con.execute("SELECT COUNT(*) FROM trips_2024").fetchone()[0])
        print("Rows emissions_lookup: ", con.execute("SELECT COUNT(*) FROM emissions_lookup").fetchone()[0])

        #output log for data loading stage
        logger.info("Data loaded and simple aggregations done on ingested data. Both green and yellow taxi data are in one table with only needed columns and emissions data is in a separate table.")
        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()