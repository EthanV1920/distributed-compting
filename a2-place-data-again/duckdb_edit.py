"""
Filename: duckdb_edit.py
Author: Ethan Vosburg
Date: January 16, 2025
Version: 1.0
Description: This python file is for creating indexes and manipulating data in
             duckdb.
"""

# Imports
import duckdb

con = duckdb.connect("duck_place.db")

# con.sql("""
# ALTER TABLE pixele_data RENAME TO pixel_data;
#         ALTER TABLE pixel_data RENAME timestamp TO changed_at;
#         """)

con.sql("""
        CREATE INDEX s_idx ON pixel_data(pixel_color)

        """)


# print(con.sql("""
#         select pixel_color, count(pixel_color) as changes
#         from pixel_data
#         where changed_at between '2022-04-01 12:00:00' and '2022-04-01 18:00:00'
#         group by pixel_color
#         order by changes desc
#         limit 5;
#         """))
