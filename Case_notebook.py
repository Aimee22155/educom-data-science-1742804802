Globale opzet voor opschonen data
________________________________
import pandas as pd
import sqlalchemy as SA
________________________________
engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")
________________________________
countries= 'countries'
cities= 'cities'
states= 'states'
country_temp= 'country_temperatures'
city_temp= 'city_temperatures'
state_temp= 'state_temperatures'
global_temp= 'global_temperatures'

__________Optie 1_______________
df = pd.read_sql_table(countries, engine)
print(df.head())
_______________________________
print("\n--- Controle op ontbrekende waarden ---")
print("Aantal ontbrekende waarden per kolom:")
print(df.isnull().sum())

print("\nPercentage ontbrekende waarden per kolom:")
print(df.isnull().mean() * 100)
______________________________
print("\n--- Vervangen van ontbrekende waarden door het gemiddelde ---")
for column in df.columns:
    if df[column].isnull().any(): # Controleer of de kolom ontbrekende waarden heeft
        if pd.api.types.is_numeric_dtype(df[column]): # Alleen voor numerieke kolommen
            gemiddelde = df[column].mean()
            df[column].fillna(gemiddelde, inplace=True)
            print(f"Ontbrekende waarden in kolom '{column}' vervangen door het gemiddelde: {gemiddelde:.2f}")
        else:
            print(f"Kolom '{column}' bevat ontbrekende waarden maar is niet numeriek. Overweeg een andere strategie.")

print("\nDataFrame na het vervangen van ontbrekende numerieke waarden door het gemiddelde:")
print(df.head())

__________Optie 2_______________
df = pd.read_sql_table(country_temperatures, engine)
print(df.head())

print("\n--- Controle op ontbrekende waarden ---")
print("Aantal ontbrekende waarden per kolom:")
print(df.isnull().sum())

print("\nPercentage ontbrekende waarden per kolom:")
print(df.isnull().mean() * 100)

for column in df.columns:
    if df[column].isnull().any():
        if pd.api.types.is_numeric_dtype(df[column]):
            gemiddelde = df[column].mean()
            df[column] = df[column].fillna(gemiddelde)
            print(f"Ontbrekende waarden in kolom '{column}' vervangen door het gemiddelde: {gemiddelde:.2f}")
        else:
            print(f"Kolom '{column}' bevat ontbrekende waarden maar is niet numeriek. Overweeg een andere strategie.")
