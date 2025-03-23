"""
Filename: ave_session_time.py
Author: Ethan Vosburg
Date: March 20, 2025
Version: 1.1
Description: This script creates a timeline of timestamps, calculates the average session time 
             for sessions active at each timestamp for 'esl_brazil' using DuckDB, and plots the 
             results with matplotlib.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt


def main():
    # Connect to the DuckDB database (or create it if it doesn't exist)
    con = duckdb.connect("stream.db")

    # SQL query to generate a timeline and calculate the average session duration at each timestamp.
    # The session duration is computed as (end_time - start_time) for each session.
    timeline_query = """
    WITH sessions AS (
        SELECT *, (end_time - start_time) AS session_length
        FROM twitch_data
        WHERE streamer_id = 'riotgames'
    ),
    timeline AS (
        SELECT ts
        FROM UNNEST(generate_series(0, 6148)) AS t(ts)
    )
    SELECT 
        timeline.ts AS timestamp,
        AVG(sessions.session_length) AS avg_session_length
    FROM timeline
    LEFT JOIN sessions
        ON timeline.ts >= sessions.start_time
       AND timeline.ts < sessions.end_time
    GROUP BY timeline.ts
    ORDER BY timeline.ts;
    """

    # Execute the query and fetch the result into a pandas DataFrame.
    result_df = con.execute(timeline_query).fetchdf()
    print(result_df)

    # Create a plot using matplotlib
    plt.figure(figsize=(10, 6))
    plt.plot(result_df['timestamp'],
             result_df['avg_session_length'], marker='o', linestyle='-')
    plt.title("Average Session Time Over Time for esl_brazil")
    plt.xlabel("Timestamp")
    plt.ylabel("Average Session Time (minutes)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
