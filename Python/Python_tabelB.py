import duckdb

def create_database():
    # Verbinding maken met DuckDB (maakt de database aan als deze niet bestaat)
    conn = duckdb.connect("database_1.duckdb")

    # Maak de "conversion_rates" tabel aan zonder AUTOINCREMENT
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversion_rates (
            id INTEGER PRIMARY KEY,  -- Gebruik INTEGER als sleutel zonder auto-increment
            date DATE,
            base_code VARCHAR,
            conversion_rate DOUBLE,
            currency_code VARCHAR
        )
    """)

    # Maak de "sequence" tabel voor handmatige ID-increment
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sequence (
            table_name VARCHAR PRIMARY KEY,
            last_id INTEGER
        )
    """)

    # Zorg ervoor dat de "sequence" tabel een startwaarde heeft voor 'conversion_rates'
    conn.execute("""
        INSERT INTO sequence (table_name, last_id)
        SELECT 'conversion_rates', 0
        WHERE NOT EXISTS (SELECT 1 FROM sequence WHERE table_name = 'conversion_rates');
    """)

    print("Database en tabel succesvol aangemaakt!")
    conn.close()

# Start de functie
if __name__ == "__main__":
    create_database()
