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
import re


def color_block(hex_string):
    hex_string = hex_string.strip("#")
    red = int(hex_string[:2], 16)
    green = int(hex_string[2:4], 16)
    blue = int(hex_string[4:6], 16)
    return f"\033[48:2::{red}:{green}:{blue}m \033[49m"


# Get the color change frequency
color_sql = """
    select pixel_color, count(pixel_color) as changes
    from pixel_data
    where changed_at between '2022-04-01 12:00:00' and '2022-04-01 18:00:00'
    group by pixel_color
    order by changes desc
    """

# Get the coordinate change frequency
coordinate_sql = """
    select coordinate, count(coordinate) as changes
    from pixel_data
    where changed_at between '2022-04-01 12:00:00' and '2022-04-01 18:00:00'
    group by coordinate
    order by changes desc
    limit 20;
    """
# Create a connector to the database
connector = sql.connect("place.db")

# Create a cursor to manipulate data
cursor = connector.cursor()


def main():
    # TODO:add the main code here
    number_args = len(sys.argv)
    for i in range(number_args):
        print(f"INFO: argument entered {sys.argv[i]}")
        if 




if __name__ == '__main__':
    main()
