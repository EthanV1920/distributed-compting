"""
Filename: duck_main.py
Author: Ethan Vosburg
Date: January 27, 2025
Version: 1.0
Description: Using duckdb to do more advanced r/place analysis
"""

# Imports
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
        # Get the color change frequency
    color_sql = f"""
        select pixel_color, count(pixel_color) as changes
        from pixel_data
        where changed_at between '2022-04-0{day} {hour:02}:00:00' and '2022-04-0{day} {(hour + duration):02}:00:00'
        group by pixel_color
        order by changes desc
        """

    # Get the coordinate change frequency
    coordinate_sql = f"""
        select coordinate, count(coordinate) as changes
        from pixel_data
        where changed_at between '2022-04-0{day} {hour:02}:00:00' and '2022-04-0{day} {(hour + duration):02}:00:00'
        group by coordinate
        order by changes desc
        limit 20;
    """

    sample_sql = """
        SELECT
            count(DISTINCT user_id)
        FROM
            pixel_data;
    """

    with duckdb.connect("duck_place.db") as con:
        # color_data = con.sql(color_sql).fetchone()
        # coord_data = con.sql(coordinate_sql).fetchone()
        sample_data = con.sql(sample_sql).fetchone()

        print(f"INFO: Sample SQL:\n{sample_data}")

        # print(f"""
        # The most changed color was: {color_data[0]}
        # which was changed {color_data[1]} times""")

        # print(f"""
        # The most changed coordinate was {coord_data[1]}
        # Which was changed {coord_data[1]} times
        # """)


if __name__ == "__main__":
    main()
