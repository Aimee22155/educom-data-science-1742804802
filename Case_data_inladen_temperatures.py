import pandas as pd
import sqlalchemy as SA

csv_files = {
    'country': 'GlobalLandTemperaturesByCountry.csv',
    'state': 'GlobalLandTemperaturesByState.csv',
    'major_city': 'GlobalLandTemperaturesByMajorCity.csv',
    'city': 'GlobalLandTemperaturesByCity.csv'
}

engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    with engine.begin() as conn:
        def load_and_process(file_path, label_prefix):
            df = pd.read_csv(file_path)

            # Verwijder records zonder datum of temperatuur
            df = df.dropna(subset=['dt', 'AverageTemperature'])

            # Groepeer op datum en neem het gemiddelde
            df_grouped = df.groupby('dt', as_index=False).mean(numeric_only=True)

            # Hernoem kolommen
            df_grouped.rename(columns={
                'dt': 'date',
                'AverageTemperature': f'{label_prefix}_avg_temp',
                'AverageTemperatureUncertainty': f'{label_prefix}_avg_temp_uncertainty'
            }, inplace=True)

            return df_grouped

        # Verwerk alle datasets
        country_df = load_and_process(csv_files['country'], 'country')
        state_df = load_and_process(csv_files['state'], 'state')
        major_city_df = load_and_process(csv_files['major_city'], 'major_city')
        city_df = load_and_process(csv_files['city'], 'city')

        # Merge alle datasets op datum
        merged_df = country_df
        for df in [state_df, major_city_df, city_df]:
            merged_df = pd.merge(merged_df, df, on='date', how='outer')

        # Loop door alle records en vervang NaN door None
        for _, row in merged_df.iterrows():
            record = {
                "unified_date": row['date'],
                "country_avg_temp": None if pd.isna(row.get('country_avg_temp')) else row.get('country_avg_temp'),
                "country_avg_temp_uncertainty": None if pd.isna(row.get('country_avg_temp_uncertainty')) else row.get('country_avg_temp_uncertainty'),
                "state_avg_temp": None if pd.isna(row.get('state_avg_temp')) else row.get('state_avg_temp'),
                "state_avg_temp_uncertainty": None if pd.isna(row.get('state_avg_temp_uncertainty')) else row.get('state_avg_temp_uncertainty'),
                "major_city_avg_temp": None if pd.isna(row.get('major_city_avg_temp')) else row.get('major_city_avg_temp'),
                "major_city_avg_temp_uncertainty": None if pd.isna(row.get('major_city_avg_temp_uncertainty')) else row.get('major_city_avg_temp_uncertainty'),
                "city_avg_temp": None if pd.isna(row.get('city_avg_temp')) else row.get('city_avg_temp'),
                "city_avg_temp_uncertainty": None if pd.isna(row.get('city_avg_temp_uncertainty')) else row.get('city_avg_temp_uncertainty')
            }

            # Insert in database
            conn.execute(SA.text("""
                INSERT INTO temperatures (
                    unified_date,
                    country_avg_temp,
                    country_avg_temp_uncertainty,
                    state_avg_temp,
                    state_avg_temp_uncertainty,
                    major_city_avg_temp,
                    major_city_avg_temp_uncertainty,
                    city_avg_temp,
                    city_avg_temp_uncertainty
                ) VALUES (
                    :unified_date,
                    :country_avg_temp,
                    :country_avg_temp_uncertainty,
                    :state_avg_temp,
                    :state_avg_temp_uncertainty,
                    :major_city_avg_temp,
                    :major_city_avg_temp_uncertainty,
                    :city_avg_temp,
                    :city_avg_temp_uncertainty
                )
                ON DUPLICATE KEY UPDATE
                    country_avg_temp = VALUES(country_avg_temp),
                    country_avg_temp_uncertainty = VALUES(country_avg_temp_uncertainty),
                    state_avg_temp = VALUES(state_avg_temp),
                    state_avg_temp_uncertainty = VALUES(state_avg_temp_uncertainty),
                    major_city_avg_temp = VALUES(major_city_avg_temp),
                    major_city_avg_temp_uncertainty = VALUES(major_city_avg_temp_uncertainty),
                    city_avg_temp = VALUES(city_avg_temp),
                    city_avg_temp_uncertainty = VALUES(city_avg_temp_uncertainty)
            """), record)

        print(f"Succesvol {len(merged_df)} records ingevoerd of geüpdatet in 'temperatures'.")

except FileNotFoundError as e:
    print(f"Fout: CSV-bestand niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden: {e}")

engine.dispose()
