"""
Filename: duckdb_create.py
Author: Ethan Vosburg
Date: January 14, 2025
Version: 1.0
Description: This python file makes and creates a database from the csv file
             in duckdb.
"""

# Imports
import duckdb

con = duckdb.connect("duck_place.db")
createtion_sql = """
    CREATE TABLE pixele_data AS
    FROM '../a1-place-data/2022_place_canvas_history.csv';
"""

con.sql(createtion_sql)
