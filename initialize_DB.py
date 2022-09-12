import sqlite3

# SQLite DB Name
DB_Name =  "solar_panel_sep05.db"

# SQLite DB Table Schema
TableSchema="""
drop table if exists master_table;
create table master_table (
    id integer primary key autoincrement,
    avg integer,
    min integer,
    max integer,
    maxpos_x integer,
    maxpos_y integer,
    minpos_x integer,
    minpos_y integer,
    unit text,
    box text,
    alert text,
    Timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'NOW'))
);


drop table if exists solar_sensor;
create table solar_sensor (
    id integer primary key autoincrement,
    bv float,
    sv float,
    i float,
    lv float,
    p float,
    Timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'NOW'))
);
"""

#Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

#Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

#Close DB
curs.close()
conn.close()