import requests
import json

# Basisinstellingen
base_currency = "USD"
target_currencies = ["EUR", "GBP", "JPY"]

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
        with open("exchange_rates.json", "w") as f:
            json.dump(raw_data, f, indent=4)

        print("De API-response is opgeslagen in 'exchange_rates.json'.")

        # Toon de wisselkoersen
        print("\nWisselkoersen:")
        for currency in target_currencies:
            rate = raw_data.get("conversion_rates", {}).get(currency)
            if rate:
                print(f"1 {base_currency} = {rate} {currency}")
            else:
                print(f"Valuta '{currency}' niet gevonden.")

    except requests.RequestException as e:
        print(f"Er is een fout bij het ophalen van de data: {e}")

# Start de functie
if __name__ == "__main__":
    get_exchange_rates()
