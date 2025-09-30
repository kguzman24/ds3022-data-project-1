import duckdb
import logging

logging.basicConfig(
    filename = "logs/analysis.log",
    encoding = "utf-8",
    filemode = "a",
    format = "{asctime} - {levelname} - {message}",
    style = "{",
    datefmt = "%Y-%m-%d %H:%M",
    level = "DEBUG"
)
logger = logging.getLogger(__name__)

#analyze and aggregate data from trips_enriched table
def analyze_files():
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(
    database="/mnt/c/Users/krngz/OneDrive - University of Virginia/DS 3022 - Data Engineering/ds3022-data-project-1/emissions.duckdb",
    read_only=True,
    config={
        "threads": 1,
        "memory_limit": "1GB",
        "temp_directory": "/tmp",
    },
)
        #logging connection
        logger.info("Connected to DuckDB instance for ANALYSIS")

        #daily totals subquery to be used in other queries
        daily_total_sql = """
        SELECT
          DATE(pickup_datetime)                              AS d,
          CAST(strftime(DATE(pickup_datetime), '%w') AS INT) AS dow_num,   -- 0=Sun..6=Sat
          strftime(DATE(pickup_datetime), '%a')              AS dow_name,  -- Sun..Sat
          EXTRACT(week  FROM DATE(pickup_datetime))          AS week_of_year, -- 1..53
          EXTRACT(month FROM DATE(pickup_datetime))          AS month_num,    -- 1..12
          SUM(trip_co2_kgs) AS day_co2
        FROM trips_enriched
        WHERE color = ?
        GROUP BY 1,2,3,4,5
        """

        #day of the week averages daily co2, ordered by highest or lowest
        dow_agg_sql = """
        WITH daily AS ({daily})
        SELECT
        dow_num, dow_name,
        AVG(day_co2) AS avg_co2
        FROM daily
        GROUP BY 1,2
        ORDER BY avg_co2 {direction}
        LIMIT 1
        """

        #sums daily totals into weeks and averages those weeks, ordered by highest or lowest
        week_agg_sql = """
        WITH daily AS ({daily}),
        weekly AS (
        SELECT week_of_year, SUM(day_co2) AS week_co2
        FROM daily
        GROUP BY week_of_year
        )
        SELECT week_of_year, AVG(week_co2) AS avg_week_co2
        FROM weekly
        GROUP BY week_of_year
        ORDER BY avg_week_co2 {direction}
        LIMIT 1
        """
        #sums daily totals into months and averages those months, ordered by highest or lowest
        month_agg_sql = """
        WITH daily AS ({daily}),
        monthly AS (
        SELECT month_num, SUM(day_co2) AS month_co2
        FROM daily
        GROUP BY month_num
        )
        SELECT month_num, AVG(month_co2) AS avg_month_co2
        FROM monthly
        GROUP BY month_num
        ORDER BY avg_month_co2 {direction}
        LIMIT 1
        """

        #using for plotting, gives total co2 per month (not an average) for line chart
        month_totals_sqlL = """
        WITH daily AS ({daily})
        SELECT month_num, SUM(day_co2) AS month_total_co2
        FROM daily
        GROUP BY month_num
        ORDER BY month_num
        """

        #largest single carbon producing trip per taxi color
        largest_carbon_yellow = con.execute("""
        SELECT MAX(trip_co2_kgs) FROM trips_enriched
        WHERE color='yellow';
        """).fetchone()[0]

        largest_carbon_green = con.execute("""
        SELECT MAX(trip_co2_kgs) FROM trips_enriched
        WHERE color='green';        
        """).fetchone()[0]

        #heaviest and lightest hour per taxi color
        heaviest_hour_yellow = con.execute("""
        WITH hourly AS (
        SELECT DATE(pickup_datetime) AS d, hour_of_day AS h, SUM(trip_co2_kgs) AS co2
        FROM trips_enriched
        WHERE color='yellow'
        GROUP BY 1,2
        ),
        avg_hour AS (
        SELECT h AS hour_of_day, AVG(co2) AS avg_co2
        FROM hourly
        GROUP BY 1
        )
        SELECT hour_of_day, avg_co2
        FROM avg_hour
        ORDER BY avg_co2 DESC
        LIMIT 1;
        """).fetchone()  #gives (hour_of_day, avg_co2)

        lightest_hour_yellow = con.execute("""
        WITH hourly AS (
        SELECT DATE(pickup_datetime) AS d, hour_of_day AS h, SUM(trip_co2_kgs) AS co2
        FROM trips_enriched
        WHERE color='yellow'
        GROUP BY 1,2
        ),
        avg_hour AS (
        SELECT h AS hour_of_day, AVG(co2) AS avg_co2
        FROM hourly
        GROUP BY 1
        )
        SELECT hour_of_day, avg_co2
        FROM avg_hour
        ORDER BY avg_co2 ASC
        LIMIT 1;
        """).fetchone()

        heaviest_hour_green = con.execute("""
        WITH hourly AS (
        SELECT DATE(pickup_datetime) AS d, hour_of_day AS h, SUM(trip_co2_kgs) AS co2
        FROM trips_enriched
        WHERE color='green'
        GROUP BY 1,2
        ),
        avg_hour AS (
        SELECT h AS hour_of_day, AVG(co2) AS avg_co2
        FROM hourly
        GROUP BY 1
        )
        SELECT hour_of_day, avg_co2
        FROM avg_hour
        ORDER BY avg_co2 DESC
        LIMIT 1;
        """).fetchone()

        lightest_hour_green = con.execute("""
        WITH hourly AS (
        SELECT DATE(pickup_datetime) AS d, hour_of_day AS h, SUM(trip_co2_kgs) AS co2
        FROM trips_enriched
        WHERE color='green'
        GROUP BY 1,2
        ),
        avg_hour AS (
        SELECT h AS hour_of_day, AVG(co2) AS avg_co2
        FROM hourly
        GROUP BY 1
        )
        SELECT hour_of_day, avg_co2
        FROM avg_hour
        ORDER BY avg_co2 ASC
        LIMIT 1;
        """).fetchone()

        #day of week, week, month heaviest and lightest averages, plus month totals for plotting

        #YELLOW
        heaviest_dow_yellow   = con.execute(dow_agg_sql.format(daily=daily_total_sql,  direction="DESC"), ["yellow"]).fetchone()
        lightest_dow_yellow   = con.execute(dow_agg_sql.format(daily=daily_total_sql,  direction="ASC"),  ["yellow"]).fetchone()
        heaviest_week_yellow  = con.execute(week_agg_sql.format(daily=daily_total_sql, direction="DESC"), ["yellow"]).fetchone()
        lightest_week_yellow  = con.execute(week_agg_sql.format(daily=daily_total_sql, direction="ASC"),  ["yellow"]).fetchone()
        heaviest_month_yellow = con.execute(month_agg_sql.format(daily=daily_total_sql, direction="DESC"), ["yellow"]).fetchone()
        lightest_month_yellow = con.execute(month_agg_sql.format(daily=daily_total_sql, direction="ASC"),  ["yellow"]).fetchone()
        yellow_month_totals   = dict(con.execute(month_totals_sql.format(daily=daily_total_sql), ["yellow"]).fetchall())

        # GREEN
        heaviest_dow_green   = con.execute(dow_agg_sql.format(daily=daily_total_sql,  direction="DESC"), ["green"]).fetchone()
        lightest_dow_green   = con.execute(dow_agg_sql.format(daily=daily_total_sql,  direction="ASC"),  ["green"]).fetchone()
        heaviest_week_green  = con.execute(week_agg_sql.format(daily=daily_total_sql, direction="DESC"), ["green"]).fetchone()
        lightest_week_green  = con.execute(week_agg_sql.format(daily=daily_total_sql, direction="ASC"),  ["green"]).fetchone()
        heaviest_month_green = con.execute(month_agg_sql.format(daily=daily_total_sql, direction="DESC"), ["green"]).fetchone()
        lightest_month_green = con.execute(month_agg_sql.format(daily=daily_total_sql, direction="ASC"),  ["green"]).fetchone()
        green_month_totals   = dict(con.execute(month_totals_sql.format(daily=daily_total_sql), ["green"]).fetchall())





        con.close()

        #printing helper for month names
        def month_name(m):  # 1..12
            return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][int(m)-1]


        print(f"Largest single carbon producing trip") 
        print(f"YELLOW: {largest_carbon_yellow}")
        print(f"GREEN : {largest_carbon_green}\n")

        print("Hours of Day with heaviest and lightest average CO2 emissions")
        print(f"YELLOW heaviest hour: {heaviest_hour_yellow[0]} (avg={heaviest_hour_yellow[1]:.3f} kg)")
        print(f"YELLOW lightest hour: {lightest_hour_yellow[0]} (avg={lightest_hour_yellow[1]:.3f} kg)")
        print(f"GREEN  heaviest hour: {heaviest_hour_green[0]} (avg={heaviest_hour_green[1]:.3f} kg)")
        print(f"GREEN  lightest hour: {lightest_hour_green[0]} (avg={lightest_hour_green[1]:.3f} kg)\n")

        print("Day of Week with heaviest and lightest average CO2 emissions")
        print(f"YELLOW heaviest DoW: {heaviest_dow_yellow[1]} (#{heaviest_dow_yellow[0]}, avg={heaviest_dow_yellow[2]:.3f} kg)")
        print(f"YELLOW lightest DoW: {lightest_dow_yellow[1]} (#{lightest_dow_yellow[0]}, avg={lightest_dow_yellow[2]:.3f} kg)")
        print(f"GREEN  heaviest DoW: {heaviest_dow_green[1]} (#{heaviest_dow_green[0]}, avg={heaviest_dow_green[2]:.3f} kg)")
        print(f"GREEN  lightest DoW: {lightest_dow_green[1]} (#{lightest_dow_green[0]}, avg={lightest_dow_green[2]:.3f} kg)\n")

        print("Week of Year with heaviest and lightest average CO2 emissions")
        print(f"YELLOW heaviest week: {int(heaviest_week_yellow[0])} (avg={heaviest_week_yellow[1]:.3f} kg)")
        print(f"YELLOW lightest week: {int(lightest_week_yellow[0])} (avg={lightest_week_yellow[1]:.3f} kg)")
        print(f"GREEN  heaviest week: {int(heaviest_week_green[0])} (avg={heaviest_week_green[1]:.3f} kg)")
        print(f"GREEN  lightest week: {int(lightest_week_green[0])} (avg={lightest_week_green[1]:.3f} kg)\n")


        print("Month of Year with heaviest and lightest average CO2 emissions")
        print(f"YELLOW heaviest month: {month_name(heaviest_month_yellow[0])} (avg={heaviest_month_yellow[1]:.3f} kg)")
        print(f"YELLOW lightest month: {month_name(lightest_month_yellow[0])} (avg={lightest_month_yellow[1]:.3f} kg)")
        print(f"GREEN  heaviest month: {month_name(heaviest_month_green[0])} (avg={heaviest_month_green[1]:.3f} kg)")
        print(f"GREEN  lightest month: {month_name(lightest_month_green[0])} (avg={lightest_month_green[1]:.3f} kg)\n")

        
        #attempting to plot
        try: 
            import matplotlib.pyplot as plt
            months = list(range(1,13))
            y_vals = [yellow_month_totals.get(m,0) for m in months]
            g_vals = [green_month_totals.get(m,0)  for m in months]
            labels = [month_name(m) for m in months]

            plt.figure(figsize=(10,5))
            plt.plot(months, y_vals, marker='o', label='Yellow')
            plt.plot(months, g_vals, marker="o", label='Green')
            plt.xticks(months, labels)
            plt.xlabel("Month")
            plt.ylabel("Total CO₂ (kg)")
            plt.title("Monthly Taxi CO₂ Totals by Color")
            plt.legend()
            plt.tight_layout()
            out_path = "outputs/monthly_co2_by_color.png"
            plt.savefig(out_path, dpi=150)
            plt.close()
            print(f"Saved plot to {out_path}")


        except Exception as e:
            print(f"An error occurred: {e}")
            logger.error(f"An error occurred: {e}")
        
        #logging to file
            logger.info("Largest single trip CO2 (kg): YELLOW=%s, GREEN=%s", largest_carbon_yellow, largest_carbon_green)

        logger.info("Hours (avg CO2): YELLOW heaviest=(hour=%s, avg=%s), lightest=(hour=%s, avg=%s)",
                    heaviest_hour_yellow[0], heaviest_hour_yellow[1],
                    lightest_hour_yellow[0], lightest_hour_yellow[1])
        logger.info("Hours (avg CO2): GREEN heaviest=(hour=%s, avg=%s), lightest=(hour=%s, avg=%s)",
                    heaviest_hour_green[0], heaviest_hour_green[1],
                    lightest_hour_green[0], lightest_hour_green[1])

        logger.info("DoW (avg daily CO2): YELLOW heaviest=(%s #%s, avg=%s), lightest=(%s #%s, avg=%s)",
                    heaviest_dow_yellow[1], heaviest_dow_yellow[0], heaviest_dow_yellow[2],
                    lightest_dow_yellow[1], lightest_dow_yellow[0], lightest_dow_yellow[2])
        logger.info("DoW (avg daily CO2): GREEN heaviest=(%s #%s, avg=%s), lightest=(%s #%s, avg=%s)",
                    heaviest_dow_green[1], heaviest_dow_green[0], heaviest_dow_green[2],
                    lightest_dow_green[1], lightest_dow_green[0], lightest_dow_green[2])

        logger.info("Week (avg weekly CO2): YELLOW heaviest=(week=%s, avg=%s), lightest=(week=%s, avg=%s)",
                    int(heaviest_week_yellow[0]), heaviest_week_yellow[1],
                    int(lightest_week_yellow[0]), lightest_week_yellow[1])
        logger.info("Week (avg weekly CO2): GREEN heaviest=(week=%s, avg=%s), lightest=(week=%s, avg=%s)",
                    int(heaviest_week_green[0]), heaviest_week_green[1],
                    int(lightest_week_green[0]), lightest_week_green[1])

        logger.info("Month (avg monthly CO2): YELLOW heaviest=(%s, avg=%s), lightest=(%s, avg=%s)",
                    month_name(heaviest_month_yellow[0]), heaviest_month_yellow[1],
                    month_name(lightest_month_yellow[0]), lightest_month_yellow[1])
        logger.info("Month (avg monthly CO2): GREEN heaviest=(%s, avg=%s), lightest=(%s, avg=%s)",
                    month_name(heaviest_month_green[0]), heaviest_month_green[1],
                    month_name(lightest_month_green[0]), lightest_month_green[1])

        logger.info("Saved plot to outputs/monthly_co2_by_color.png")
        logger.info("Completed analysis successfully.")


    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    

if __name__ == "__main__":
    analyze_files()