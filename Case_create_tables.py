import pandas as pd
import sqlalchemy as SA

engine = SA.create_engine("mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

with engine.connect() as conn:
    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Location (
            Location_id INT AUTO_INCREMENT PRIMARY KEY,
            Country_name VARCHAR(255),
            State_name VARCHAR(255),
            Major_city VARCHAR(255),
            City_name VARCHAR(255),
            Latitude VARCHAR(50) NOT NULL,
            Longitude VARCHAR(50) NOT NULL
        )
    '''))

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Temperatures (
            Temp_id INT AUTO_INCREMENT PRIMARY KEY,
            Country_date DATE NOT NULL,
            Country_avg_temp FLOAT,
            Country_avg_temp_uncertainty FLOAT,
            State_date DATE NOT NULL,
            State_avg_temp FLOAT,
            State_avg_temp_uncertainty FLOAT,
            Major_city_date DATE NOT NULL,
            Major_city_avg_temp FLOAT,
            Major_city_avg_temp_uncertainty FLOAT,
            City_date DATE NOT NULL,
            City_avg_temp FLOAT,
            City_avg_temp_uncertainty FLOAT
        )
    '''))

    #location ID

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Global_temperatures (
            Global_id INT AUTO_INCREMENT PRIMARY KEY,
            Date DATE NOT NULL,
            Land_avg_temp FLOAT,
            Land_avg_temp_uncertainty FLOAT,
            Land_max_temp FLOAT,
            Land_max_temp_uncertainty FLOAT,
            Land_min_temp FLOAT,
            Land_min_temp_uncertainty FLOAT,
            Land_ocean_avg_temp FLOAT,
            Land_ocean_avg_temp_uncertainty FLOAT
        )
    '''))

print("Succesvol tabellen aangemaakt")
