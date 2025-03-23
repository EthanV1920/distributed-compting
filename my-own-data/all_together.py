"""
Filename: query.py
Author: Ethan Vosburg
Date: March 20, 2025
Version: 1.2
Description: This script creates a timeline of timestamps, calculates concurrent viewership
             and average session time for sessions active at each timestamp for 'esl_brazil'
             using DuckDB, and plots the results on a dual-axis graph using matplotlib.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt


def main():
    # Connect to the DuckDB database (or create it if it doesn't exist)
    con = duckdb.connect("stream.db")

    # SQL query that computes both metrics:
    # - concurrent_viewers: counts sessions active at each timestamp.
    # - avg_session_length: average duration (end_time - start_time) of sessions active at each timestamp.
    query = """
    WITH sessions AS (
        SELECT *, (end_time - start_time) AS session_length
        FROM twitch_data
        WHERE streamer_id = 'esl_brazil'
    ),
    timeline AS (
        SELECT ts
        FROM UNNEST(generate_series(1043, 1095)) AS t(ts)
    )
    SELECT
        timeline.ts AS timestamp,
        COUNT(sessions.user_id) AS concurrent_viewers,
        AVG(sessions.session_length) AS avg_session_length
    FROM timeline
    LEFT JOIN sessions
        ON timeline.ts >= sessions.start_time
       AND timeline.ts < sessions.end_time
    GROUP BY timeline.ts
    ORDER BY timeline.ts;
    """

    # Execute the query and fetch the result into a pandas DataFrame.
    result_df = con.execute(query).fetchdf()
    print(result_df)

    # Create a dual-axis plot using matplotlib.
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plot concurrent viewers on the primary y-axis.
    color1 = 'tab:blue'
    ax1.set_xlabel("Timestamp")
    ax1.set_ylabel("Concurrent Viewers", color=color1)
    ax1.plot(result_df['timestamp'], result_df['concurrent_viewers'],
             marker='o', linestyle='-', color=color1, label="Concurrent Viewers")
    ax1.tick_params(axis='y', labelcolor=color1)

    # Create a secondary y-axis for average session time.
    # Change time data from 10 of minutes to minutes
    result_df['avg_session_length'] = result_df['avg_session_length'] * 10 / 60
    ax2 = ax1.twinx()
    color2 = 'tab:red'
    ax2.set_ylabel("Average Session Time (hours)", color=color2)
    ax2.plot(result_df['timestamp'], result_df['avg_session_length'],
             marker='s', linestyle='--', color=color2, label="Avg Session Time")
    ax2.tick_params(axis='y', labelcolor=color2)

    # Title and layout adjustments.
    plt.title("Concurrent Viewers and Average Session Time Over Time for esl_brazil")
    fig.tight_layout()

    # Create a combined legend.
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")

    # Display the grid and plot.
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    main()
