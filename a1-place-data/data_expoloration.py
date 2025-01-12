"""
Filename: data_exploration.py
Author: Ethan Vosburg
Date: January 7, 2025
Version: 1.1
Description: This script will be used to explore the shape of the database file
             and manipulate it to improve performance
"""

# Imports
import sqlite3 as sql


def color_block(hex_string):
    hex_string = hex_string.strip("#")
    red = int(hex_string[:2], 16)
    green = int(hex_string[2:4], 16)
    blue = int(hex_string[4:6], 16)
    return f"\033[48:2::{red}:{green}:{blue}m \033[49m"


# Create a connector to the database
connector = sql.connect("place.db")

# Create a cursor to manipulate data
cursor = connector.cursor()


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

# Create an index on the 'coordinate' column
coordinate_index_sql = """
    create index pixel_coordinate_index
    on pixel_data(coordinate);
    """

# Create an index on the 'changed_at' column
time_index_sql = """
    create index pixel_time_index
    on pixel_data(changed_at);
    """

# Create an index on the 'pixel_color' column
index_sql = """
    create index pixel_color_index
    on pixel_data(pixel_color);
    """

# Change the name of the 'timestamp' column to not use reserved word
column_name_sql = """
    alter table pixel_data
    rename column timestamp to changed_at;
    """


# Initial testing sql statement
sql = """
    select pixel_color, count(pixel_color) as changes
    from pixel_data
    group by pixel_color
    order by 2 desc
    limit 20;
    """


# data = cursor.execute(index_sql)
data = cursor.execute(coordinate_sql)
# data = cursor.execute(color_sql)
print(data.fetchall())

# print("INFO: Printing data...")
# for d in data.fetchall():
#     print(f"COLOR: {color_block(d[0])} \"{d[0]}\" used {d[1]} times")
