import duckdb

def create_database():
    # Verbinding maken met DuckDB (maakt de database aan als deze niet bestaat)
    conn = duckdb.connect("database_1.duckdb")

    # Maak de "conversion_rates" tabel aan
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversion_rates (
            id INTEGER PRIMARY KEY,
            date DATE,
            base_code VARCHAR,
            conversion_rate DOUBLE,
            currency_code VARCHAR
        )
    """)

    print("Database en tabel succesvol aangemaakt!")

if __name__ == "__main__":
    create_database()
