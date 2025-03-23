"""
Filename: query.py
Author: Ethan Vosburg
Date: March 20, 2025
Version: 1.1
Description: This script creates a timeline of timestamps, calculates concurrent viewership for 'esl_brazil' 
             using DuckDB, and plots the results with matplotlib.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt


def main():
    # Connect to the DuckDB database (or create it if it doesn't exist)
    con = duckdb.connect("stream.db")

    # SQL query to generate a timeline and count concurrent viewers per timestamp
    timeline_query = """
    WITH sessions AS (
        SELECT *
        FROM twitch_data
        WHERE streamer_id = 'riotgames'
    ),
    timeline AS (
        SELECT ts
        FROM UNNEST(generate_series(0, 6148)) AS t(ts)
    )
    SELECT 
        timeline.ts AS timestamp,
        COUNT(sessions.user_id) AS concurrent_viewers
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
             result_df['concurrent_viewers'], marker='o', linestyle='-')
    plt.title("Concurrent Viewers Over Time for esl_brazil")
    plt.xlabel("Timestamp")
    plt.ylabel("Concurrent Viewers")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()

