import requests
import json
import duckdb
from datetime import datetime

# Basisinstellingen
base_currency = "USD"
db_filename = "database_1.duckdb"

def get_next_id(conn, table_name):
    # Haal de laatste ID op en verhoog deze met 1
    result = conn.execute(f"""
        SELECT last_id FROM sequence WHERE table_name = '{table_name}'
    """).fetchone()
    
    if result:
        next_id = result[0] + 1
        conn.execute(f"""
            UPDATE sequence SET last_id = {next_id} WHERE table_name = '{table_name}'
        """)
        return next_id
    else:
        raise ValueError(f"No sequence entry found for table {table_name}")

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

        # Verbinding met de database
        conn = duckdb.connect(db_filename)

        # Haal datum en conversiekoersen uit de API-response
        datetime_str = raw_data["time_last_update_utc"].split(' ')[0:5]  # Verwijder de tijdzone-informatie
        datetime_str = " ".join(datetime_str)  # "Thu, 03 Apr 2025 00:00:01"
        conversion_date = datetime.strptime(datetime_str, "%a, %d %b %Y %H:%M:%S").date()
        base_code = raw_data["base_code"]  # Basisvaluta
        rates = raw_data["conversion_rates"]  # Conversiekoersen

        # Voeg de conversiekoersen toe aan de tabel
        for currency_code, rate in rates.items():
            next_id = get_next_id(conn, 'conversion_rates')  # Verkrijg de volgende ID

            conn.execute("""
                INSERT INTO conversion_rates (id, currency_code, date, base_code, conversion_rate)
                VALUES (?, ?, ?, ?, ?)
            """, (next_id, currency_code, conversion_date, base_code, rate))

        print("Data succesvol ingevoerd in de database!")
        conn.close()

    except requests.RequestException as e:
        print(f"Fout bij ophalen data: {e}")
    except ValueError as ve:
        print(ve)

# Start de functie
if __name__ == "__main__":
    get_exchange_rates()
