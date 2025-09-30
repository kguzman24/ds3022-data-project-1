import duckdb
import logging


logging.basicConfig(
    filename = "logs/clean.log",
    encoding = "utf-8",
    filemode = "a",
    format = "{asctime} - {levelname} - {message}",
    style = "{",
    datefmt = "%Y-%m-%d %H:%M",
    level = "DEBUG"
)
logger = logging.getLogger(__name__)

#creating function that cleans the combined table of yellow and green taxi data
def clean_trip_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance for CLEANING")

        #getting statistics to check PRE cleaning
        pre = con.execute("""
            SELECT
              COUNT(*)                            AS total_rows,
              AVG(trip_distance)                  AS avg_distance,
              MAX(trip_distance)                  AS max_distance,
              AVG(passenger_count)                AS avg_passengers
            FROM trips_2024
        """).fetchone()

        #table UPDATE
        con.execute(f"""
        CREATE OR REPLACE TABLE trips_2024 AS
        SELECT DISTINCT *,
            date_diff('second', pickup_datetime, dropoff_datetime) AS trip_duration_sec
        FROM trips_2024
        WHERE passenger_count > 0 
            AND trip_distance > 0 
            AND trip_distance <= 100 
            AND date_diff('second', pickup_datetime, dropoff_datetime) BETWEEN 0 AND 86400;
    """)

        #getting statistics to check POST cleaning
        post = con.execute("""
        SELECT
            COUNT(*)                            AS total_rows,
            AVG(trip_distance)                  AS avg_distance,
            MAX(trip_distance)                  AS max_distance,
            AVG(passenger_count)                AS avg_passengers,
            MAX(trip_duration_sec)              AS max_duration_sec
        FROM trips_2024
    """).fetchone()

        #printing stats PRE cleaning
        print("\nBEFORE CLEANING (trips_2024)")
        print(f"Total rows:         {pre[0]:,}")
        print(f"Avg distance (mi):  {pre[1]:.2f}")
        print(f"Max distance (mi):  {pre[2]:.2f}")
        print(f"Avg passengers:     {pre[3]:.2f}")
        

        #printing stats POST cleaning
        print("\nAFTER CLEANING (trips_2024)")
        print(f"Total rows:         {post[0]:,}")
        print(f"Avg distance (mi):  {post[1]:.2f}")
        print(f"Max distance (mi):  {post[2]:.2f}")
        print(f"Avg passengers:     {post[3]:.2f}")
        print(f"Max duration (sec): {int(post[4]) if post[4] is not None else None}")

        #logging to file
        logger.info("BEFORE CLEANING: total_rows=%s, avg_dist=%.2f, max_dist=%.2f, avg_pass=%.2f",
                    pre[0], pre[1], pre[2], pre[3])
        logger.info("AFTER CLEANING: total_rows=%s, avg_dist=%.2f, max_dist=%.2f, avg_pass=%.2f, max_dur=%s",
                    post[0], post[1], post[2], post[3], post[4])

        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_trip_files()