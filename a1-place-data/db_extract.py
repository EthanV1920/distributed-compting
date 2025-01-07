"""
Filename: db_extract.py
Author: Ethan Vosburg
Date: January 7, 2025
Version: 1.0
Description: This script will extract data from a csv file and then create a
             database to use
"""

# Imports
import sqlite3 as sql
import csv


def create_database(csv_filename, db_filename):
    """
    Creat a sqlite3 database from a imported csv file.

    Parameters
    ----------
    csv_filename: string
        name of the csv file to process
    db_filename: string
        name of the final database file

    Returns
    -------
    None
    """

    table_creation_sql = """
        create table pixel_data
        (timestamp,user_id,pixel_color,coordinate);
        """

    table_insert_sql = """
        insert into pixel_data
            (timestamp,user_id,pixel_color,coordinate)
        values
            (?, ?, ?, ?)
        """

    try:
        # Create/connect to the database
        connector = sql.connect(db_filename)

        # Create a cursor object
        cursor = connector.cursor()

        # Create a table that matches the csv format
        cursor.execute(table_creation_sql)

        with open(csv_filename, 'r') as file:
            csv_reader = csv.reader(file)
            # Remove the header row from the csv
            next(csv_reader)
            # Execute many sql statements
            cursor.executemany(table_insert_sql, csv_reader)

        # Close out connector
        connector.commit()
        connector.close()

    except Exception:
        print(f"ERROR: {Exception}")


create_database("2022_place_canvas_history.csv", "place.db")
