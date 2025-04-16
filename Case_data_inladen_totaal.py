import pandas as pd
import sqlalchemy as SA

df_country = pd.read_csv('GlobalLandTemperaturesByCountry.csv')
df_state = pd.read_csv('GlobalLandTemperaturesByState.csv')
df_major_city = pd.read_csv('GlobalLandTemperaturesByMajorCity.csv')
df_city = pd.read_csv('GlobalLandTemperaturesByCity.csv')
df_global = pd.read_csv('GlobalTemperatures.csv')

engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    with engine.begin() as conn:
        # ========== TABEL: Location ==========
        # Landen
        country_df = pd.concat([
            df_country['Country'],
            df_state['Country'],
            df_major_city['Country'],
            df_city['Country']
        ]).dropna().drop_duplicates().to_frame(name='country_name')

        for _, row in country_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO Location (country_name) VALUES (:country_name)"),
                         {"country_name": row['country_name']})

        print(f"Succesvol {len(country_df)} unieke landen verwerkt.")

        # Staten
        state_df = df_state[['State']].dropna().drop_duplicates().rename(columns={'State': 'state_name'})
        for _, row in state_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO Location (state_name) VALUES (:state_name)"),
                         {"state_name": row['state_name']})
        print(f"Succesvol {len(state_df)} unieke staten verwerkt.")

        # Grote steden
        major_city_df = df_major_city[['City']].dropna().drop_duplicates().rename(columns={'City': 'Major_city'})
        for _, row in major_city_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO Location (Major_city) VALUES (:Major_city)"),
                         {"Major_city": row['Major_city']})
        print(f"Succesvol {len(major_city_df)} unieke grote steden verwerkt.")

        # Steden
        city_df = df_city[['City']].dropna().drop_duplicates().rename(columns={'City': 'City_name'})
        for _, row in city_df.iterrows():
            conn.execute(SA.text("INSERT IGNORE INTO Location (City_name) VALUES (:City_name)"),
                         {"City_name": row['City_name']})
        print(f"Succesvol {len(city_df)} unieke steden verwerkt.")

        # Latitude en Longitude
        for col in ['Latitude', 'Longitude']:
            geo_df = df_city[[col]].dropna().drop_duplicates()
            for _, row in geo_df.iterrows():
                conn.execute(SA.text(f"INSERT IGNORE INTO Location ({col}) VALUES (:{col})"),
                             {col: row[col]})
            print(f"Succesvol {len(geo_df)} unieke {col} verwerkt.")

        # ========== TABEL: temperatures ==========
        def process_temperature(df, column_map):
            for original, new in column_map.items():
                temp_df = df[[original]].dropna().drop_duplicates().rename(columns={original: new})
                for _, row in temp_df.iterrows():
                    conn.execute(SA.text(f"INSERT IGNORE INTO temperatures ({new}) VALUES (:{new})"),
                                 {new: row[new]})
                print(f"Succesvol {len(temp_df)} unieke {new} verwerkt.")

        # Country
        process_temperature(df_country, {
            'dt': 'Country_date',
            'AverageTemperature': 'Country_avg_temp',
            'AverageTemperatureUncertainty': 'Country_avg_temp_uncertainty'
        })

        # State
        process_temperature(df_state, {
            'dt': 'State_date',
            'AverageTemperature': 'state_avg_temp',
            'AverageTemperatureUncertainty': 'state_avg_temp_uncertainty'
        })

        # Major City
        process_temperature(df_major_city, {
            'dt': 'Major_city_date',
            'AverageTemperature': 'Major_city_avg_temp',
            'AverageTemperatureUncertainty': 'Major_city_avg_temp_uncertainty'
        })

        # City
        process_temperature(df_city, {
            'dt': 'City_date',
            'AverageTemperature': 'City_avg_temp',
            'AverageTemperatureUncertainty': 'City_avg_temp_uncertainty'
        })

        # ========== TABEL: global_temperatures ==========
        process_temperature(df_global, {
            'dt': 'Date',
            'LandAverageTemperature': 'Land_avg_temp',
            'LandAverageTemperatureUncertainty': 'Land_avg_temp_uncertainty',
            'LandMaxTemperature': 'Land_max_temp',
            'LandMaxTemperatureUncertainty': 'Land_max_temp_uncertainty',
            'LandMinTemperature': 'Land_min_temp',
            'LandMinTemperatureUncertainty': 'Land_min_temp_uncertainty',
            'LandAndOceanAverageTemperature': 'Land_ocean_avg_temp',
            'LandAndOceanAverageTemperatureUncertainty': 'Land_ocean_avg_temp_uncertainty'
        })

except FileNotFoundError as e:
    print(f"Fout: Een van de CSV-bestanden is niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden bij het verwerken van de data: {e}")

engine.dispose()
