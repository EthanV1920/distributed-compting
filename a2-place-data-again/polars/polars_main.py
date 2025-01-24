"""
Filename: polars_main.py
Author: Ethan Vosburg
Date: January 23, 2025
Version: 1.0
Description: This python file, makes and creates a database from the csv file
             with polars.
"""

import polars as pl
import sys
import sqlite3 as sql


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

    connection = sql.connect("../../a1-place-data/place.db")

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
    # connection = "sqlite:///../../a1-place-data/place.db"

    color_data = pl.read_database(color_sql, connection)
    coord_data = pl.read_database(coordinate_sql, connection)

    color_df = pl.DataFrame(color_data)
    coord_df = pl.DataFrame(coord_data)

    # print(color_df)
    # print(coord_df)
    print(f"""
        the most changed color was: {color_df['pixel_color'][0]}
        which was changed {color_df['changes'][0]} times""")

    print(f"""
        the most changed coordinate was {coord_df['coordinate'][0]}
        which was changed {coord_df['changes'][0]} times
        """)


if __name__ == "__main__":
    main()
