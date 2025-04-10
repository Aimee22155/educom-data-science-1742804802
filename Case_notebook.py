import pandas as pd
import sqlalchemy as SA

engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

countries= 'countries'
cities= 'cities'
states= 'states'
country_temperatures= 'country_temperatures'
city_temperatures= 'city_temperatures'
state_temperatures= 'state_temperatures'
global_temperatures= 'global_temperatures'

#--------------------------------------------

df = pd.read_sql_table(countries, engine)
print(df.head())

print("\n--- Controle op ontbrekende waarden ---")
print("Aantal ontbrekende waarden per kolom:")
print(df.isnull().sum())

print("\nPercentage ontbrekende waarden per kolom:")
print(df.isnull().mean() * 100)

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

df.to_sql('opgeschoond_countries', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{countries}' is opgeslagen in tabel 'opgeschoond_countries'.")

#--------------------------------------------

df = pd.read_sql_table(cities, engine)
print(df.head())

print("\n--- Controle op ontbrekende waarden ---")
print("Aantal ontbrekende waarden per kolom:")
print(df.isnull().sum())

print("\nPercentage ontbrekende waarden per kolom:")
print(df.isnull().mean() * 100)

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

df.to_sql('opgeschoond_cities', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{cities}' is opgeslagen in tabel 'opgeschoond_cities'.")

#--------------------------------------------

df = pd.read_sql_table(states, engine)
print(df.head())

print("\n--- Controle op ontbrekende waarden ---")
print("Aantal ontbrekende waarden per kolom:")
print(df.isnull().sum())

print("\nPercentage ontbrekende waarden per kolom:")
print(df.isnull().mean() * 100)

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

df.to_sql('opgeschoond_states', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{states}' is opgeslagen in tabel 'opgeschoond_states'.")

#--------------------------------------------

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

df.to_sql('opgeschoond_country_temperatures', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{country_temperatures}' is opgeslagen in tabel 'opgeschoond_country_temperatures'.")

#--------------------------------------------

df = pd.read_sql_table(city_temperatures, engine)
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

df.to_sql('opgeschoond_city_temperatures', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{city_temperatures}' is opgeslagen in tabel 'opgeschoond_city_temperatures'.")

#--------------------------------------------

df = pd.read_sql_table(state_temperatures, engine)
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

df.to_sql('opgeschoond_state_temperatures', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{state_temperatures}' is opgeslagen in tabel 'opgeschoond_state_temperatures'.")

#--------------------------------------------

df = pd.read_sql_table(global_temperatures, engine)
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

df.to_sql('opgeschoond_global_temperatures', engine, if_exists='replace', index=False)
print(f"\nOpgeschoonde data van '{global_temperatures}' is opgeslagen in tabel 'opgeschoond_global_temperatures'.")
