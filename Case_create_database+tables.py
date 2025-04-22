import mysql.connector
import pandas as pd
import sqlalchemy as SA

# Maak verbinding met MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)

mycursor = mydb.cursor()

# Controleer of de database al bestaat, zo niet, maak de database aan
mycursor.execute("SHOW DATABASES LIKE 'CSV_climate_watch'")

if not mycursor.fetchone():
    mycursor.execute("CREATE DATABASE CSV_climate_watch")
    print("Database 'CSV_climate_watch' succesvol aangemaakt")
else:
    print("Database 'CSV_climate_watch' bestaat al")

# Maak verbinding met de specifieke database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="CSV_climate_watch"
)

engine = SA.create_engine("mysql+pymysql://root@localhost/CSV_climate_watch?charset=utf8mb4")

# Tabellen aan maken indien ze niet bestaan
with engine.connect() as conn:
    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Country (
            dt DATE NOT NULL,
            AverageTemperature FLOAT,
            AverageTemperatureUncertainty FLOAT,
            Country VARCHAR(50) NOT NULL                       
        )
    '''))

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS state (
            dt DATE NOT NULL,
            AverageTemperature FLOAT,
            AverageTemperatureUncertainty FLOAT,
            State VARCHAR(50) NOT NULL,
            Country VARCHAR(50) NOT NULL                   
        )
    '''))

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Major_city (
            dt DATE NOT NULL,
            AverageTemperature FLOAT,
            AverageTemperatureUncertainty FLOAT,
            City VARCHAR(50) NOT NULL,
            Country VARCHAR(50) NOT NULL,
            Latitude VARCHAR(50) NOT NULL,
            Longitude VaRCHAR(50) NOT NULL                
        )
    '''))

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS City (
            dt DATE NOT NULL,
            AverageTemperature FLOAT,
            AverageTemperatureUncertainty FLOAT,
            City VARCHAR(50) NOT NULL,
            Country VARCHAR(50) NOT NULL,
            Latitude VARCHAR(50) NOT NULL,
            Longitude VaRCHAR(50) NOT NULL                         
        )
    '''))

    conn.execute(SA.text('''
        CREATE TABLE IF NOT EXISTS Global_temperatures (
            dt DATETIME,
            LandAverageTemperature FLOAT,
            LandAverageTemperatureUncertainty FLOAT,
            LandMaxTemperature FLOAT,
            LandMaxTemperatureUncertainty FLOAT,
            LandMinTemperature FLOAT,
            LandMinTemperatureUncertainty FLOAT,
            LandAndOceanAverageTemperature FLOAT,
            LandAndOceanAverageTemperatureUncertainty FLOAT
        );
    '''))

print("Succesvol tabellen aangemaakt (indien ze nog niet bestonden).")
