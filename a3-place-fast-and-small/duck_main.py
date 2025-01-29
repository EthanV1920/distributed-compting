"""
Filename: duck_main.py
Author: Ethan Vosburg
Date: January 27, 2025
Version: 1.0
Description: Using duckdb to do more advanced r/place analysis
"""

# Imports
import webcolors
import duckdb
import sys


def main():
    number_args = len(sys.argv)
    day = 0
    hour = 0
    duration = 0

    # Validate user input
    for i in range(number_args):
        # print(f"INFO: argument entered {sys.argv[i]}")
        if i == 1:
            try:
                day = int(sys.argv[1])
            except Exception as e:
                print(f"ERROR: {e}")
                exit(1)

        if i == 2:
            try:
                hour = int(sys.argv[2])
            except Exception as e:
                print(f"ERROR: {e}")
                exit(1)

        if i == 3:
            try:
                duration = int(sys.argv[3])
            except Exception as e:
                print(f"ERROR: {e}")
                exit(1)
        if i > 3:
            print("INFO: Extra arguments provided, ignoring rest")

    color_rank_sql = f"""
SELECT
    pixel_color,
    COUNT(*) AS pixel_count,
    COUNT(DISTINCT user_int) AS distinct_users
FROM 'place-snappy.parquet' AS file
WHERE file.changed_at BETWEEN '2022-04-0{day} {hour:02}:00:00' and '2022-04-0{day} {(hour + duration):02}:00:00'
GROUP BY pixel_color
ORDER BY distinct_users DESC;
    """

    ave_session_len_sql = f"""
WITH sessions AS (
    SELECT user_int,
           changed_at,
           LAG(changed_at) OVER (PARTITION BY user_int ORDER BY changed_at) AS prev_time,
            DATE_DIFF('SECOND', LAG(changed_at) OVER (PARTITION BY user_int ORDER BY changed_at), changed_at) AS session_length
    FROM 'place-snappy.parquet' AS file
WHERE file.changed_at BETWEEN '2022-04-0{day} {hour:02}:00:00' AND '2022-04-0{day} {(hour + duration):02}:00:00'
    ),
    valid_sessions AS (
-- Filter sessions where the gap is <= 15 minutes (900 seconds)
SELECT user_int, session_length
FROM sessions
WHERE session_length <= 900
    ),
    multi_pixel_users AS (
-- Only include users who placed more than one pixel
SELECT user_int
FROM valid_sessions
GROUP BY user_int
HAVING COUNT(*) > 1
    )

SELECT AVG(session_length) AS avg_session_length
FROM valid_sessions
WHERE user_int IN (SELECT user_int FROM multi_pixel_users);
    """

    percentile_sql = f"""
    select percentile_cont(0.5) WITHIN GROUP (ORDER BY value ) as p50,
 percentile_cont(0.75) WITHIN
GROUP (ORDER BY value ) as p75,
    percentile_cont(0.9) WITHIN
GROUP (ORDER BY value ) as p90,
    percentile_cont(0.99) WITHIN
GROUP (ORDER BY value ) as p99
from (
    select count (*) as value
    from 'place-snappy.parquet' as file
    where file.changed_at between '2022-04-0{day} {hour:02}:00:00' and '2022-04-0{day} {(hour + duration):02}:00:00'
    group by user_int
    ) balls;
    """

    first_time_user_sql = f"""
WITH prev_users AS (
    SELECT DISTINCT user_int AS prev_d_user
    FROM 'place-snappy.parquet' AS file
WHERE file.changed_at BETWEEN '2022-04-01 12:00:00' AND '2022-04-0{day} {(hour):02}:00:00'
    ),
    current_users AS (
SELECT DISTINCT user_int AS d_users
FROM 'place-snappy.parquet' AS file
WHERE file.changed_at BETWEEN '2022-04-0{day} {hour:02}:00:00' AND '2022-04-0{day} {(hour + duration):02}:00:00'
    ),
    first_time_users AS (
SELECT COUNT(d_users) AS first_users
FROM current_users
    LEFT JOIN prev_users ON current_users.d_users = prev_users.prev_d_user
WHERE prev_users.prev_d_user IS NULL -- Only users who are not in prev_users
    )

SELECT first_users FROM first_time_users;
    """

    color_rank_data = duckdb.sql(color_rank_sql)
    ave_session_len_data = duckdb.sql(ave_session_len_sql)
    percentile_data = duckdb.sql(percentile_sql)
    first_time_user_data = duckdb.sql(first_time_user_sql)

    # print(f"INFO: Sample SQL:\n{color_rank_data}")

    print("COLOR RANK:")
    for i in range(3):
        color_data = color_rank_data.fetchone()
        print(f"    Rank{i + 1}: {webcolors.hex_to_name(color_data[0])} with {color_data[2]} users")
    print("")

    # print(f"INFO: Sample SQL:\n{ave_session_len_data}")
    print("AVE SESSION LENGTH:")
    print(
        f"    Session Len: {ave_session_len_data.fetchone()[0]:10.2f} seconds")

    # print(f"INFO: Sample SQL:\n{percentile_data}")
    print("")
    print("PECENTILE DATA:")
    percentile_data = percentile_data.fetchone()
    print(f"    P50: {percentile_data[0]:10.0f} pixels")
    print(f"    P75: {percentile_data[1]:10.0f} pixels")
    print(f"    P90: {percentile_data[2]:10.0f} pixels")
    print(f"    P99: {percentile_data[3]:10.0f} pixels")
    print("")

    # print(f"INFO: Sample SQL:\n{first_time_user_data}")
    print("FIRST TIME USERS:")
    print(f"    First Time Users: {first_time_user_data.fetchone()[0]}")


if __name__ == "__main__":
    main()
