import requests
import json
import duckdb

# Basisinstellingen
base_currency = "USD"
db_filename = "database_1.duckdb"

def get_exchange_rates():
    # API URL met de basisvaluta
    url = f"https://v6.exchangerate-api.com/v6/6f941e2a0fa59948fc359f77/latest/{base_currency}"

    try:
        # Haal de data op van de API
        response = requests.get(url)
        response.raise_for_status()  # Zorgt ervoor dat er geen fout is
        raw_data = response.json()

        # Controleer of de API succesvol is
        if raw_data.get("result") != "success":
            print("Er is een probleem met de API.")
            return

       # Sla de response op in een JSON-bestand
        with open("exchange_rates2.json", "w") as f:
            json.dump(raw_data, f, indent=4)

        # Toon de ruwe JSON-response
        print("Ruwe API-response:")
        print(json.dumps(raw_data, indent=4))

        # Verbinding maken met de DuckDB-database (of maak deze aan als deze niet bestaat)
        conn = duckdb.connect(db_filename)

 
        # # JSON-bestand laden als tabel
        # conn.execute("""
        #     CREATE TABLE exchange_rates AS
        #     SELECT * FROM read_json_auto('exchange_rates2.json');
        # """)

        conn.close
  
    except requests.RequestException as e:
        print(f"Er is een fout bij het ophalen van de data: {e}")

# Start de functie
if __name__ == "__main__":
    get_exchange_rates()
