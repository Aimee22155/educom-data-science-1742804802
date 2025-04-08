import json

# importeer de JSON-data
with open("world-population.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# aanmaken tabel-namen
countries = []
entities = []
seen_countries = {} # een hulplijst (dictionary) om dubbele te voorkomen.

# Unieke ID's voor landen
country_id_counter = 1
entity_id_counter = 1

for item in data:
    country_key = item["country"]
    
    # Alleen toevoegen als dit land nog niet is gezien
    if country_key not in seen_countries:
        country_data = {
            "id": country_id_counter,
            "rank": item.get("rank"),
            "cca3": item.get("cca3"),
            "country": item.get("country"),
            "capital": item.get("capital"),
            "continent": item.get("continent"),
            "area_km2": item.get("area (km²)")
        }
        countries.append(country_data)
        seen_countries[country_key] = country_id_counter
        country_id_counter += 1
    
    # Voeg entiteit toe (populatie per jaar)
    entity_data = {
        "id": entity_id_counter,
        "country_id": seen_countries[country_key],
        "year": item.get("year"),
        "population": item.get("population")
    }
    entities.append(entity_data)
    entity_id_counter += 1

# Combineer beide lijsten in één dictionary
combined_data = {
    "countries": countries,
    "entities": entities
}

# Schrijf alles weg naar één bestand
with open("world-data.json", "w", encoding="utf-8") as f:
    json.dump(combined_data, f, indent=2)

print("Alles opgeslagen in world-data.json")
