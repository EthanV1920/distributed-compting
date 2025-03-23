"""
Filename: create_db.py
Author: Ethan Vosburg
Date: March 20, 2025
Version: 1.0
Description: This python file makes and creates a database from the csv file
             in duckdb.
"""

# Imports
import duckdb

con = duckdb.connect("stream.db")

# Create the table from the csv
creation_sql = """
    CREATE TABLE twitch_data AS
    FROM '../my-own-data/full_a.csv';
"""

# con.sql(creation_sql)

# Rename columns
rename_sql = """
ALTER TABLE twitch_data RENAME column0 TO stream_id;
ALTER TABLE twitch_data RENAME column1 TO user_id;
ALTER TABLE twitch_data RENAME column2 TO streamer_id;
ALTER TABLE twitch_data RENAME column3 TO start_time;
ALTER TABLE twitch_data RENAME column4 TO end_time;
"""

con.sql(rename_sql)


"""
Filename: create_db.py
Author: Ethan Vosburg
Date: March 20, 2025
Version: 1.0
Description: This python file makes and creates a database from the csv file
             in duckdb.
"""

# Imports
import duckdb

con = duckdb.connect("stream.db")


# Rename columns
rename_sql = """
select *
from twitch_data
where streamer_id = 'esl_brazil'
and start_time between 1045 and 1100;

"""

con.sql(rename_sql)
