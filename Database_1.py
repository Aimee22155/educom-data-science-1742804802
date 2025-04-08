import json
import csv

# === 1. Inlezen van originele JSON ===
with open("7_world_population.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# === 2. Tabellen aanmaken ===
countries = []
entities = []
seen_countries = {}

country_id_counter = 1
entity_id_counter = 1

# === 3. Data opsplitsen ===
for item in data:
    country_key = item["country"]
    
    if country_key not in seen_countries:
        # Voeg land toe aan 'countries'
        country_data = {
            "id": country_id_counter,
            "rank": item.get("rank"),
            "cca3": item.get("cca3"),
            "country": item.get("country"),
            "capital": item.get("capital"),
            "continent": item.get("continent"),
            "area_km2": item.get("area_km2")
        }
        countries.append(country_data)
        seen_countries[country_key] = country_id_counter
        country_id_counter += 1
    
    # === 4. Verwerk de populatie per jaar ===
    for population_data in item.get("population", []):  # Itereer over de lijst van populaties
        entity_data = {
            "id": entity_id_counter,
            "country_id": seen_countries[country_key],
            "year": population_data.get("year"),
            "population": population_data.get("population")
        }
        entities.append(entity_data)
        entity_id_counter += 1

# === 5. Combineer in één JSON-bestand ===
combined_data = {
    "countries": countries,
    "entities": entities
}

with open("world-data.json", "w", encoding="utf-8") as f:
    json.dump(combined_data, f, indent=2)

print("JSON opgeslagen als world-data.json")

# === 6. Exporteer naar CSV-bestanden ===

# Countries → CSV
with open("countries.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=countries[0].keys())
    writer.writeheader()
    writer.writerows(countries)

# Entities → CSV
with open("entities.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=entities[0].keys())
    writer.writeheader()
    writer.writerows(entities)

print("CSV-bestanden opgeslagen als countries.csv en entities.csv")
