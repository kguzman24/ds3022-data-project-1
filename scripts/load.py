import duckdb
import os
import logging
from pathlib import Path

#paths
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
logs_dir = project_root / "logs"
data_dir   = project_root / "data"
db_path = project_root / "emissions.duckdb"

logs_dir.mkdir(parents=True, exist_ok=True)
data_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename = str(logs_dir/"load.log"),
    encoding = "utf-8",
    filemode = "a",
    format = "{asctime} - {levelname} - {message}",
    style = "{",
    datefmt = "%Y-%m-%d %H:%M",
    level = "DEBUG"
)
logger = logging.getLogger(__name__)
#logging.info("Logging is set up.")

CDN = "https://d37ci6vzurychx.cloudfront.net/trip-data"  #base url for taxi data
YEARS = list(range(2015, 2025))  #full range of 2015-2024 years
MONTHS = [f"{m:02d}" for m in range(1, 13)]

#building urls for all years and months for both yellow and green taxi data
def build_urls(color: str):
    return [
        f"{CDN}/{color}_tripdata_{y}-{m}.parquet"
        for y in YEARS
        for m in MONTHS
    ]


#loading and aggragating specific .parquet files with only needed columns
def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database=str(db_path), read_only=False)
        logger.info("Connected to DuckDB instance for LOADING")
        # Define the file paths and URLs
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("SET enable_object_cache=true;")

        #build urls
        #loading multiple file sources programatically, instead of statically
        yellow_urls = build_urls("yellow")
        green_urls  = build_urls("green")


        con.execute(f"""
            CREATE OR REPLACE TABLE yellow_all AS
            SELECT 'yellow' as color,
            passenger_count, 
            trip_distance, 
            tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime
            FROM read_parquet(?, union_by_name = TRUE);
        """, [yellow_urls])
        con.execute(f"""
            CREATE OR REPLACE TABLE green_all AS
            SELECT 
            'green' as color,
            passenger_count, 
            trip_distance, 
            lpep_pickup_datetime AS pickup_datetime, lpep_dropoff_datetime AS dropoff_datetime
            FROM read_parquet(?, union_by_name = TRUE);
        """, [green_urls])

        #both colors in one table
        con.execute("""
            CREATE OR REPLACE TABLE trips_all AS
            SELECT * FROM yellow_all
            UNION ALL
            SELECT * FROM green_all;
        """)

        trips_count = con.execute("SELECT COUNT(*) FROM trips_all").fetchone()[0]
        avg_dist    = con.execute("SELECT AVG(trip_distance) FROM trips_all").fetchone()[0]
        avg_passenger  = con.execute("SELECT AVG(passenger_count) FROM trips_all").fetchone()[0]

        #print statements for rows of each table  and basic stats
        print("Rows trips_2024: ", trips_count)
        logger.info(f"Trips_all row count: {trips_count}")

  
        print("Avg trip distance: ", avg_dist)
        print("Avg passenger count: ",  avg_passenger)
        logger.info(f"Average trip distance: {avg_dist}")
        logger.info(f"Average passenger count: {avg_passenger}")


        #loading emissions table into separate duckdb table

        emissions_csv = (data_dir / "vehicle_emissions.csv").as_posix()
        if os.path.exists(emissions_csv):
            con.execute("""
                CREATE OR REPLACE TABLE emissions_lookup AS
                SELECT * FROM read_csv_auto(?);
            """, [emissions_csv])
            emissions_count = con.execute("SELECT COUNT(*) FROM emissions_lookup").fetchone()[0]
            print("Rows emissions_lookup:", emissions_count)
            logger.info("emissions_lookup row count: %s", emissions_count)
        else:
            logger.warning("vehicle_emissions.csv not found at %s; skipped loading.", emissions_csv)

        #output log for data loading stage
        logger.info("Data loaded and simple aggregations done on ingested data. Both green and yellow taxi data are in one table with only needed columns and emissions data is in a separate table.")
        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()