import duckdb

def create_database():
    # Connect to DuckDB (creates the database file if it doesn't exist)
    conn = duckdb.connect("database_1.duckdb")

    # Create the "currency" table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS currency (
            id INTEGER PRIMARY KEY,
            currency_code VARCHAR
        )
    """)

    # Create the "conversion_rates" table with a foreign key to the "currency" table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversion_rates (
            id INTEGER PRIMARY KEY,
            currency_id INTEGER,
            date DATE,
            base_code VARCHAR,
            conversion_rate DOUBLE,
            FOREIGN KEY(currency_id) REFERENCES currency(id)
        )
    """)

    print("Database and tables created successfully!")

if __name__ == "__main__":
    create_database()
