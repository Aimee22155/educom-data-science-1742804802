import pandas as pd
import sqlalchemy as SA

csv_files = {
    'country': 'GlobalLandTemperaturesByCountry.csv',
    'state': 'GlobalLandTemperaturesByState.csv',
    'major_city': 'GlobalLandTemperaturesByMajorCity.csv',
    'city': 'GlobalLandTemperaturesByCity.csv'
}

CHUNK_SIZE = 10000
engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    with engine.begin() as conn:
        location_data = []
        processed_locations = set()

        def process_country_data(filename):
            try:
                for chunk in pd.read_csv(filename, chunksize=CHUNK_SIZE):
                    for country in chunk['Country'].dropna().unique():
                        location_tuple = (country, None, None, None, None, None)
                        if location_tuple not in processed_locations:
                            location_data.append({
                                'Country_name': country,
                                'State_name': None,
                                'Major_city': None,
                                'City_name': None,
                                'Latitude': None,
                                'Longitude': None
                            })
                            processed_locations.add(location_tuple)
            except Exception as e:
                print(f"Fout bij verwerken van landenbestand: {e}")

        def process_state_data(filename):
            try:
                for chunk in pd.read_csv(filename, chunksize=CHUNK_SIZE):
                    for _, row in chunk.iterrows():
                        country = row.get('Country')
                        state = row.get('State')
                        if pd.notna(country) and pd.notna(state):
                            location_tuple = (country, state, None, None, None, None)
                            if location_tuple not in processed_locations:
                                location_data.append({
                                    'Country_name': country,
                                    'State_name': state,
                                    'Major_city': None,
                                    'City_name': None,
                                    'Latitude': None,
                                    'Longitude': None
                                })
                                processed_locations.add(location_tuple)
            except Exception as e:
                print(f"Fout bij verwerken van statenbestand: {e}")

        def process_major_city_data(filename):
            try:
                for chunk in pd.read_csv(filename, chunksize=CHUNK_SIZE):
                    for _, row in chunk.iterrows():
                        country = row.get('Country')
                        state = row.get('State')
                        major_city = row.get('City')
                        if pd.notna(country) and pd.notna(major_city):
                            location_tuple = (country, state, major_city, None, None, None)
                            if location_tuple not in processed_locations:
                                location_data.append({
                                    'Country_name': country,
                                    'State_name': state if pd.notna(state) else None,
                                    'Major_city': major_city,
                                    'City_name': None,
                                    'Latitude': None,
                                    'Longitude': None
                                })
                                processed_locations.add(location_tuple)
            except Exception as e:
                print(f"Fout bij verwerken van major city bestand: {e}")

        def process_city_data(filename):
            try:
                for chunk in pd.read_csv(filename, chunksize=CHUNK_SIZE):
                    for _, row in chunk.iterrows():
                        country = row.get('Country')
                        state = row.get('State')
                        major_city = row.get('MajorCity')
                        city = row.get('City')
                        latitude = row.get('Latitude')
                        longitude = row.get('Longitude')
                        if pd.notna(city) and pd.notna(latitude) and pd.notna(longitude):
                            location_tuple = (country, state, major_city, city, latitude, longitude)
                            if location_tuple not in processed_locations:
                                location_data.append({
                                    'Country_name': country if pd.notna(country) else None,
                                    'State_name': state if pd.notna(state) else None,
                                    'Major_city': major_city if pd.notna(major_city) else None,
                                    'City_name': city,
                                    'Latitude': latitude,
                                    'Longitude': longitude
                                })
                                processed_locations.add(location_tuple)
            except Exception as e:
                print(f"Fout bij verwerken van stedenbestand: {e}")

        # Verwerk elk bestandstype
        process_country_data(csv_files['country'])
        process_state_data(csv_files['state'])
        process_major_city_data(csv_files['major_city'])
        process_city_data(csv_files['city'])

        # Schrijf naar database
        if location_data:
            conn.execute(SA.text("""
                INSERT IGNORE INTO Location (
                    Country_name,
                    State_name,
                    Major_city,
                    City_name,
                    Latitude,
                    Longitude
                ) VALUES (
                    :Country_name,
                    :State_name,
                    :Major_city,
                    :City_name,
                    :Latitude,
                    :Longitude
                )
            """), location_data)
            print(f"Succesvol {len(location_data)} unieke locaties verwerkt.")
        else:
            print("Geen locatiegegevens gevonden om te verwerken.")

except FileNotFoundError as e:
    print(f"Fout: Een van de CSV-bestanden is niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden bij het verwerken van de locaties: {e}")

engine.dispose()
