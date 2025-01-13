"""
Filename: main.py
Author: Ethan Vosburg
Date: January 11, 2025
Version: 1.0
Description: This is the main script for the cli tool
             and manipulate it to improve performance
"""

# Imports
import sqlite3 as sql
import sys


# Create a color chip in the terminal with escape characters
def color_block(hex_string):
    hex_string = hex_string.strip("#")
    red = int(hex_string[:2], 16)
    green = int(hex_string[2:4], 16)
    blue = int(hex_string[4:6], 16)
    return f"\033[48:2::{red}:{green}:{blue}m \033[49m"


def main():
    number_args = len(sys.argv)
    day = 0
    hour = 0
    duration = 0

    # Validate user input
    for i in range(number_args):
        print(f"INFO: argument entered {sys.argv[i]}")
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
    # Create a connector to the database
    connector = sql.connect("place.db")

    # Create a cursor to manipulate data
    cursor = connector.cursor()

    # Execute the sql
    color_data = cursor.execute(color_sql)
    color_data = color_data.fetchall()
    # print(color_data)

    coordinate_data = cursor.execute(coordinate_sql)
    coordinate_data = coordinate_data.fetchall()
    # print(coordinate_data)

    print(f"""

'2022-04-0{day} {hour:02}:00:00' and '2022-04-0{day} {(hour + duration):02}:00:00'

        """)

    for i in range(3):
        print(
            f"COLOR: {color_block(color_data[i][0])} \"{color_data[i][0]}\" used {color_data[i][1]} times")
        print(
            f"COORDINATE:  \"{coordinate_data[i][0]}\" changed {coordinate_data[i][1]} times")


if __name__ == '__main__':
    main()
