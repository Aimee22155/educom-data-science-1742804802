import pandas as pd
import sqlalchemy as SA

# Lees de CSV-bestanden in
df_city = pd.read_csv('GlobalLandTemperaturesByCity.csv')
df_country = pd.read_csv('GlobalLandTemperaturesByCountry.csv')
df_major_city = pd.read_csv('GlobalLandTemperaturesByMajorCity.csv')
df_state = pd.read_csv('GlobalLandTemperaturesByState.csv')
df_global = pd.read_csv('GlobalTemperatures.csv')

# Maak een SQLAlchemy engine
engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    # 1. === Tabel 'countries' ===
    with engine.begin() as conn:
        countries_df = pd.concat([
            df_country['Country'],
            df_city['Country'],
            df_major_city['Country'],
            df_state['Country']
        ]).dropna().drop_duplicates().to_frame(name='country_name')

        countries_df.drop_duplicates(subset='country_name', inplace=True)

        for index, row in countries_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO countries (country_name) VALUES (:country_name)"),
                         {"country_name": row['country_name']})
        conn.commit()
        print(f"Succesvol {len(countries_df)} unieke landen verwerkt.")

    # Haal de country_id op uit de countries tabel
    with engine.connect() as conn:
        countries_in_db = pd.read_sql(SA.text("SELECT country_id, country_name FROM countries"), conn)

    # 2. === Tabel 'states' ===
    with engine.connect() as conn:
        existing_states_count = conn.execute(SA.text("SELECT COUNT(*) FROM states")).scalar_one()

    if existing_states_count == 0:
        states_df = df_state[['State', 'Country']].dropna().drop_duplicates()\
            .rename(columns={'State': 'state_name', 'Country': 'country_name'})
        states_df['state_name'] = states_df['state_name'].str.strip().str.replace(r'-\d+$', '', regex=True).str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        states_df_merged = states_df.merge(countries_in_db, on='country_name', how='left').dropna(subset=['country_id']).drop_duplicates(subset=['state_name', 'country_id'])
        try:
            states_df_merged[['state_name', 'country_id']].to_sql('states', engine, if_exists='append', index=False)
            print(f"Succesvol {len(states_df_merged)} unieke staten toegevoegd aan de 'states'-tabel.")
        except Exception as e:
            print(f"❌ Fout bij het verwerken van staten: {e}")
    else:
        print(f"De 'states'-tabel bevat al {existing_states_count} rijen. Het vullen wordt overgeslagen.")

    # Haal de state_id op uit de states tabel
    with engine.connect() as conn:
        states_in_db = pd.read_sql(SA.text("SELECT state_id, state_name, country_id FROM states"), conn)

    # 3. === Tabel 'cities' ===
    with engine.connect() as conn:
        existing_cities_count = conn.execute(SA.text("SELECT COUNT(*) FROM cities")).scalar_one()

    if existing_cities_count == 0:
        cities_df = pd.concat([
            df_city[['City', 'Latitude', 'Longitude', 'Country']],
            df_major_city[['City', 'Latitude', 'Longitude', 'Country']]
        ]).dropna(subset=['City']).drop_duplicates(subset=['City'])

        cities_df = cities_df.rename(columns={
            'City': 'city_name',
            'Country': 'country_name',
            'Latitude': 'latitude',
            'Longitude': 'longitude'
        })

        # Koppel de country_id aan de cities tabel
        cities_df = cities_df.merge(countries_in_db, on='country_name', how='left').dropna(subset=['country_id'])

        cities_df = cities_df[['city_name', 'country_id', 'latitude', 'longitude']]
        cities_df.drop_duplicates(subset=['city_name', 'country_id', 'latitude', 'longitude'], inplace=True)

        cities_df.to_sql('cities', engine, if_exists='append', index=False)

        print(f"Succesvol {len(cities_df)} unieke steden toegevoegd aan de 'cities'-tabel (gekoppeld aan countries).")
    else:
        print(f"De 'cities'-tabel bevat al {existing_cities_count} rijen. Het vullen wordt overgeslagen.")

    # Haal de city_id op uit de cities tabel (dit gebeurt nu altijd)
    with engine.connect() as conn:
        cities_in_db = pd.read_sql(SA.text("SELECT city_id, city_name, country_id FROM cities"), conn)

    # 5. === Tabel 'state_temperatures' ===
    with engine.connect() as conn:
        existing_state_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM state_temperatures")).scalar_one()

    if existing_state_temps_count == 0:
        states_temp_df = df_state.rename(columns={'dt': 'date', 'AverageTemperature': 'avg_temp',
                                                   'AverageTemperatureUncertainty': 'avg_temp_uncertainty',
                                                   'State': 'state_name', 'Country': 'country_name'})
        states_temp_df = states_temp_df.merge(states_in_db, on=['state_name', 'country_name'], how='left').dropna(subset=['state_id'])\
            [['date', 'avg_temp', 'avg_temp_uncertainty', 'state_id']].dropna().drop_duplicates(subset=['date', 'state_id'])
        try:
            states_temp_df.to_sql('state_temperatures', engine, if_exists='append', index=False)
            print(f"Succesvol {len(states_temp_df)} temperatuurgegevens per staat toegevoegd aan 'state_temperatures'.")
        except Exception as e:
            print(f"Fout bij het schrijven naar state_temperatures: {e}")
    else:
        print(f"De 'state_temperatures'-tabel bevat al {existing_state_temps_count} rijen. Het vullen wordt overgeslagen.")

    # 6. === Tabel 'city_temperatures' ===
    with engine.connect() as conn:
        existing_city_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM city_temperatures")).scalar_one()

    if existing_city_temps_count == 0:
        cities_temp_df = pd.concat([df_city, df_major_city], ignore_index=True)
        cities_temp_df['is_major_city'] = cities_temp_df['Major'].apply(lambda x: True if pd.notna(x) else False)
        cities_temp_df = cities_temp_df.rename(columns={'dt': 'date', 'AverageTemperature': 'avg_temp',
                                                        'AverageTemperatureUncertainty': 'avg_temp_uncertainty',
                                                        'City': 'city_name', 'Country': 'country_name'})
        cities_temp_df['city_name'] = cities_temp_df['city_name'].str.strip().str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        cities_temp_df['country_name'] = cities_temp_df['country_name'].str.strip().str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        cities_temp_df = cities_temp_df.merge(cities_in_db, on='country_name', how='left').dropna(subset=['country_id'])

        unique_cities = cities_temp_df[['city_name', 'country_id']].drop_duplicates()
        try:
            unique_cities.to_sql('cities', engine, if_exists='append', index=False)
            print(f"Succesvol {len(unique_cities)} unieke steden verwerkt.")
            cities_in_db = pd.read_sql(SA.text("SELECT city_id, city_name, country_id FROM cities"), engine)

            city_temps_df = cities_temp_df.merge(cities_in_db, on=['city_name', 'country_id'], how='left').dropna(subset=['city_id'])
            city_temps_df = city_temps_df[['date', 'avg_temp', 'avg_temp_uncertainty', 'city_id', 'is_major_city']].dropna().drop_duplicates(subset=['date', 'city_id'])
            city_temps_df.to_sql('city_temperatures', engine, if_exists='append', index=False)
            print(f"Succesvol {len(city_temps_df)} temperatuurgegevens per stad toegevoegd aan 'city_temperatures'.")
        except Exception as e:
            print(f"Fout bij het verwerken van steden en stadstemperaturen: {e}")
    else:
        print(f"De 'city_temperatures'-tabel bevat al {existing_city_temps_count} rijen. Het vullen wordt overgeslagen.")

    # 7. === Tabel 'global_temperatures' ===
    with engine.connect() as conn:
        existing_global_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM global_temperatures")).scalar_one()

    if existing_global_temps_count == 0:
        global_temp_df = df_global.rename(columns={'dt': 'date', 'LandAverageTemperature': 'land_avg_temp',
                                                 'LandAverageTemperatureUncertainty': 'land_avg_temp_uncertainty',
                                                 'LandMaxTemperature': 'land_max_temp',
                                                 'LandMaxTemperatureUncertainty': 'land_max_temp_uncertainty',
                                                 'LandMinTemperature': 'land_min_temp',
                                                 'LandMinTemperatureUncertainty': 'land_min_temp_uncertainty',
                                                 'LandAndOceanAverageTemperature': 'land_ocean_avg_temp',
                                                 'LandAndOceanAverageTemperatureUncertainty': 'land_ocean_avg_uncertainty'})
        global_temp_df.drop_duplicates(subset=['date'], inplace=True)
        try:
            global_temp_df.to_sql('global_temperatures', engine, if_exists='append', index=False)
            print(f"Succesvol {len(global_temp_df)} globale temperatuurgegevens toegevoegd aan 'global_temperatures'.")
        except Exception as e:
            print(f"Fout bij het schrijven naar global_temperatures: {e}")
    else:
        print(f"De 'global_temperatures'-tabel bevat al {existing_global_temps_count} rijen. Het vullen wordt overgeslagen.")

    # 8. === Tabel 'country_temperatures' ===
    with engine.connect() as conn:
        existing_country_temps_count = conn.execute(SA.text("SELECT COUNT(*) FROM country_temperatures")).scalar_one()

    if existing_country_temps_count == 0:
        country_temps_df = df_country.rename(columns={'dt': 'date', 'AverageTemperature': 'avg_temp',
                                                     'AverageTemperatureUncertainty': 'avg_temp_uncertainty',
                                                     'Country': 'country_name'})
        country_temps_df = country_temps_df.merge(countries_in_db, on='country_name', how='left').dropna(subset=['country_id'])
        country_temps_df = country_temps_df[['date', 'avg_temp', 'avg_temp_uncertainty', 'country_id']].dropna().drop_duplicates(subset=['date', 'country_id'])
        try:
            country_temps_df.to_sql('country_temperatures', engine, if_exists='append', index=False)
            print(f"Succesvol {len(country_temps_df)} temperatuurgegevens per land toegevoegd aan 'country_temperatures'.")
        except Exception as e:
            print(f"Fout bij het schrijven naar country_temperatures: {e}")
    else:
        print(f"De 'country_temperatures'-tabel bevat al {existing_country_temps_count} rijen. Het vullen wordt overgeslagen.")

    # Feedback
    print("Alle relevante CSV-bestanden zijn succesvol geladen in de juiste tabellen.")

except FileNotFoundError as e:
    print(f"Fout: Een van de CSV-bestanden is niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden bij het verwerken van de tabellen: {e}")
