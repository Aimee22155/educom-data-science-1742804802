import pandas as pd
import sqlalchemy as SA

# Lees de CSV-bestanden in
df_city = pd.read_csv('GlobalLandTemperaturesByCity.csv')
df_country = pd.read_csv('GlobalLandTemperaturesByCountry.csv')
df_major_city = pd.read_csv('GlobalLandTemperaturesByMajorCity.csv')
df_state = pd.read_csv('GlobalLandTemperaturesByState.csv')
df_global = pd.read_csv('GlobalTemperatures.csv')

# Maak een SQLAlchemy engine (verbinding met de database)
engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    # 1. Voeg landen toe aan de 'countries' tabel
    with engine.begin() as conn:
        countries_df = pd.concat([
            df_country['Country'],
            df_city['Country'],
            df_major_city['Country'],
            df_state['Country']
        ]).dropna().drop_duplicates().to_frame(name='country_name')

        # Voeg landen toe aan de database
        for index, row in countries_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO countries (country_name) VALUES (:country_name)"),
                         {"country_name": row['country_name']})
        print(f"Succesvol {len(countries_df)} unieke landen toegevoegd.")
    
    # Haal de 'country_id' op uit de 'countries' tabel
    with engine.connect() as conn:
        countries_in_db = pd.read_sql(SA.text("SELECT country_id, country_name FROM countries"), conn)

    # 2. Voeg staten toe aan de 'states' tabel
    with engine.connect() as conn:
        existing_states_count = conn.execute(SA.text("SELECT COUNT(*) FROM states")).scalar_one()

    if existing_states_count == 0:
        states_df = df_state[['State', 'Country']].dropna().drop_duplicates()
        states_df = states_df.rename(columns={'State': 'state_name', 'Country': 'country_name'})

        # Opschonen van state_name
        states_df['state_name'] = states_df['state_name'].str.strip()
        states_df['state_name'] = states_df['state_name'].str.replace(r'-\d+$', '', regex=True)
        states_df['state_name'] = states_df['state_name'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

        # Voeg 'country_id' toe aan de 'states' tabel door te mergen
        states_df = states_df.merge(countries_in_db, on='country_name', how='left').dropna(subset=['country_id'])
        
        # Deduplicatie op 'state_name' en 'country_id'
        states_df.drop_duplicates(subset=['state_name', 'country_id'], inplace=True)

        # Voeg de staten toe aan de database
        states_df_to_db = states_df[['state_name', 'country_id']]
        states_df_to_db.to_sql('states', engine, if_exists='append', index=False)
        print(f"Succesvol {len(states_df_to_db)} unieke staten toegevoegd.")
    else:
        print(f"De 'states'-tabel bevat al {existing_states_count} staten. Het vullen wordt overgeslagen.")

    # Haal de 'state_id' op uit de 'states' tabel
    with engine.connect() as conn:
        states_in_db = pd.read_sql(SA.text("SELECT state_id, state_name, country_id FROM states"), conn)

    # 3. Voeg temperatuurgegevens per staat toe aan de 'state_temperatures' tabel
    with engine.connect() as conn:
        existing_state_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM state_temperatures")).scalar_one()

    if existing_state_temps_count == 0:
        states_temp_df = df_state.rename(columns={
            'dt': 'date',
            'AverageTemperature': 'avg_temp',
            'AverageTemperatureUncertainty': 'avg_temp_uncertainty',
            'State': 'state_name',
            'Country': 'country_name'
        })
        
        # Voeg 'country_id' en 'state_id' toe aan de temperatuurdata
        states_temp_df = states_temp_df.merge(countries_in_db, on='country_name', how='left')
        states_temp_df = states_temp_df.merge(states_in_db, on=['state_name', 'country_id'], how='left').dropna(subset=['state_id'])
        
        # Alleen de relevante kolommen behouden en duplicate entries verwijderen
        states_temp_df = states_temp_df[['date', 'avg_temp', 'avg_temp_uncertainty', 'state_id']].drop_duplicates(subset=['date', 'state_id'])

        # Voeg de temperatuurgegevens toe aan de database
        states_temp_df.to_sql('state_temperatures', engine, if_exists='append', index=False)
        print(f"Succesvol {len(states_temp_df)} temperatuurgegevens per staat toegevoegd.")
    else:
        print(f"De 'state_temperatures'-tabel bevat al {existing_state_temps_count} records. Het vullen wordt overgeslagen.")

    # 4. Voeg temperatuurgegevens per stad toe aan de 'city_temperatures' tabel
    with engine.connect() as conn:
        existing_city_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM city_temperatures")).scalar_one()

    if existing_city_temps_count == 0:
        cities_temp_df = pd.concat([df_city, df_major_city], ignore_index=True)
        
        cities_temp_df = cities_temp_df.rename(columns={
            'dt': 'date',
            'AverageTemperature': 'avg_temp',
            'AverageTemperatureUncertainty': 'avg_temp_uncertainty',
            'City': 'city_name',
            'Country': 'country_name'
        })

        # Opschonen van city_name
        cities_temp_df['city_name'] = cities_temp_df['city_name'].str.strip()
        cities_temp_df['city_name'] = cities_temp_df['city_name'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

        # Voeg 'country_id' toe aan de 'cities' data
        cities_temp_df = cities_temp_df.merge(countries_in_db, on='country_name', how='left').dropna(subset=['country_id'])

        # Haal de 'city_id' op uit de 'cities' tabel
        with engine.connect() as conn:
            cities_in_db = pd.read_sql(SA.text("SELECT city_id, city_name, country_id FROM cities"), conn)

        # Voeg 'city_id' toe aan de 'cities' data
        cities_temp_df = cities_temp_df.merge(cities_in_db, on=['city_name', 'country_id'], how='left').dropna(subset=['city_id'])

        # Alleen de relevante kolommen behouden en duplicate entries verwijderen
        cities_temp_df = cities_temp_df[['date', 'avg_temp', 'avg_temp_uncertainty', 'city_id']].drop_duplicates(subset=['date', 'city_id'])

        # Voeg de temperatuurgegevens per stad toe aan de database
        cities_temp_df.to_sql('city_temperatures', engine, if_exists='append', index=False)
        print(f"Succesvol {len(cities_temp_df)} temperatuurgegevens per stad toegevoegd.")
    else:
        print(f"De 'city_temperatures'-tabel bevat al {existing_city_temps_count} records. Het vullen wordt overgeslagen.")

    # 5. Voeg globale temperatuurgegevens toe aan de 'global_temperatures' tabel
    with engine.connect() as conn:
        existing_global_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM global_temperatures")).scalar_one()

    if existing_global_temps_count == 0:
        global_temp_df = df_global.rename(columns={
            'dt': 'date',
            'LandAverageTemperature': 'land_avg_temp',
            'LandAverageTemperatureUncertainty': 'land_avg_temp_uncertainty',
            'LandMaxTemperature': 'land_max_temp',
            'LandMaxTemperatureUncertainty': 'land_max_temp_uncertainty',
            'LandMinTemperature': 'land_min_temp',
            'LandMinTemperatureUncertainty': 'land_min_temp_uncertainty',
            'LandAndOceanAverageTemperature': 'land_ocean_avg_temp',
            'LandAndOceanAverageTemperatureUncertainty': 'land_ocean_avg_temp_uncertainty'
        })
        
        global_temp_df = global_temp_df.drop_duplicates(subset=['date'])
        global_temp_df.to_sql('global_temperatures', engine, if_exists='append', index=False)
        print(f"Succesvol {len(global_temp_df)} globale temperatuurgegevens toegevoegd.")
    else:
        print(f"De 'global_temperatures'-tabel bevat al {existing_global_temps_count} records. Het vullen wordt overgeslagen.")

    print("Alle relevante CSV-bestanden zijn succesvol geladen in de juiste tabellen.")

except FileNotFoundError as e:
    print(f"Fout: Een van de CSV-bestanden is niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden bij het verwerken van de tabellen: {e}")

