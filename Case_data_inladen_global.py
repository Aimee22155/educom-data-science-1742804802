import pandas as pd
import sqlalchemy as SA

csv_file = 'GlobalTemperatures.csv'
engine = SA.create_engine(f"mysql+pymysql://root@localhost/climate_watch?charset=utf8mb4")

try:
    with engine.begin() as conn:
        df = pd.read_csv(csv_file)

        # Verwijder records zonder datum
        df = df.dropna(subset=['dt'])

        # Groepeer op datum en neem het gemiddelde (voor het geval er duplicaten zijn)
        df_grouped = df.groupby('dt', as_index=False).mean(numeric_only=True)

        # Loop door alle records en vervang NaN door None voordat ze worden ingevoerd
        for _, row in df_grouped.iterrows():
            # Vervang NaN door None in de kolommen
            record = {
                "Date": row['dt'],
                "Land_avg_temp": None if pd.isna(row['LandAverageTemperature']) else row['LandAverageTemperature'],
                "Land_avg_temp_uncertainty": None if pd.isna(row['LandAverageTemperatureUncertainty']) else row['LandAverageTemperatureUncertainty'],
                "Land_max_temp": None if pd.isna(row['LandMaxTemperature']) else row['LandMaxTemperature'],
                "Land_max_temp_uncertainty": None if pd.isna(row['LandMaxTemperatureUncertainty']) else row['LandMaxTemperatureUncertainty'],
                "Land_min_temp": None if pd.isna(row['LandMinTemperature']) else row['LandMinTemperature'],
                "Land_min_temp_uncertainty": None if pd.isna(row['LandMinTemperatureUncertainty']) else row['LandMinTemperatureUncertainty'],
                "Land_ocean_avg_temp": None if pd.isna(row['LandAndOceanAverageTemperature']) else row['LandAndOceanAverageTemperature'],
                "Land_ocean_avg_temp_uncertainty": None if pd.isna(row['LandAndOceanAverageTemperatureUncertainty']) else row['LandAndOceanAverageTemperatureUncertainty']
            }

            # Voer de insert uit, en gebruik None waar NaN wordt gedetecteerd
            conn.execute(SA.text("""
                INSERT INTO global_temperatures (
                    Date,
                    Land_avg_temp,
                    Land_avg_temp_uncertainty,
                    Land_max_temp,
                    Land_max_temp_uncertainty,
                    Land_min_temp,
                    Land_min_temp_uncertainty,
                    Land_ocean_avg_temp,
                    Land_ocean_avg_temp_uncertainty
                ) VALUES (
                    :Date,
                    :Land_avg_temp,
                    :Land_avg_temp_uncertainty,
                    :Land_max_temp,
                    :Land_max_temp_uncertainty,
                    :Land_min_temp,
                    :Land_min_temp_uncertainty,
                    :Land_ocean_avg_temp,
                    :Land_ocean_avg_temp_uncertainty
                )
                ON DUPLICATE KEY UPDATE
                    Land_avg_temp = VALUES(Land_avg_temp),
                    Land_avg_temp_uncertainty = VALUES(Land_avg_temp_uncertainty),
                    Land_max_temp = VALUES(Land_max_temp),
                    Land_max_temp_uncertainty = VALUES(Land_max_temp_uncertainty),
                    Land_min_temp = VALUES(Land_min_temp),
                    Land_min_temp_uncertainty = VALUES(Land_min_temp_uncertainty),
                    Land_ocean_avg_temp = VALUES(Land_ocean_avg_temp),
                    Land_ocean_avg_temp_uncertainty = VALUES(Land_ocean_avg_temp_uncertainty)
            """), record)

        print(f"Succesvol {len(df_grouped)} records ingevoerd of geüpdatet in 'global_temperatures'.")

except FileNotFoundError as e:
    print(f"Fout: CSV-bestand niet gevonden: {e}")
except Exception as e:
    print(f"Er is een fout opgetreden: {e}")

engine.dispose()
