import requests
import json
import duckdb
from datetime import datetime

# Basisinstellingen
base_currency = "USD"
db_filename = "database_1.duckdb"

def get_exchange_rates():
    url = f"https://v6.exchangerate-api.com/v6/6f941e2a0fa59948fc359f77/latest/{base_currency}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()

        if raw_data.get("result") != "success":
            print("API-fout: data niet opgehaald.")
            return

        print("Ruwe API-response:")
        print(json.dumps(raw_data, indent=4))

        # Verbind met de database
        conn = duckdb.connect(db_filename)

        # Haal datum en conversiekoersen uit de API-response
        # Verwijder de tijdzone-informatie (bijvoorbeeld '+0000')
        datetime_str =  raw_data["time_last_update_utc"].split(' ')[0:5]  # Verwijder de tijdzone-informatie
        datetime_str = " ".join(datetime_str)  # "Thu, 03 Apr 2025 00:00:01"
        conversion_date = datetime.strptime(datetime_str, "%a, %d %b %Y %H:%M:%S").date()
        base_code =  raw_data["base_code"]  # Verander van 'raw_data' naar 'data'
        rates =  raw_data["conversion_rates"]

        # Voeg nieuwe valuta toe en update bestaande in één stap
        currencies = [(code,) for code in rates.keys()]
        conn.executemany("""
            INSERT INTO currency (id, currency_code)
            SELECT COALESCE(MAX(id), 0) + 1, ?
            FROM currency
            WHERE NOT EXISTS (SELECT 1 FROM currency WHERE currency_code = ?);
        """, [(code, code) for code in rates.keys()])

        # Voeg conversiekoersen toe (alleen als ze nog niet bestaan)
        conversion_data = [
            (currency_code, conversion_date, base_code, rate) 
            for currency_code, rate in rates.items()
        ]
        conn.executemany("""
            INSERT INTO conversion_rates (id, currency_id, date, base_code, conversion_rate)
            SELECT COALESCE(MAX(id), 0) + 1, 
                   (SELECT id FROM currency WHERE currency_code = ?), ?, ?, ?
            FROM conversion_rates
            WHERE NOT EXISTS (
                SELECT 1 FROM conversion_rates 
                WHERE currency_id = (SELECT id FROM currency WHERE currency_code = ?)
                AND date = ? AND base_code = ?
            );
        """, [(code, conversion_date, base_code, rate, code, conversion_date, base_code) for code, rate in rates.items()])

        print("Data succesvol ingevoerd in de database!")
        conn.close()

    except requests.RequestException as e:
        print(f"Fout bij ophalen data: {e}")

# Start de functie
if __name__ == "__main__":
    get_exchange_rates()
