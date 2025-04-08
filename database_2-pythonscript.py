import argparse
import sqlalchemy as SA
import pandas as pd

# Argumenten parseren
parser = argparse.ArgumentParser(description="Voer een SQL-query uit op de database")
parser.add_argument('-q', '--query', type=str, required=True, help="Pad naar SQL-bestand met query")
args = parser.parse_args()

# Maak de engine voor de verbinding
engine = SA.create_engine(f"mysql+pymysql://root@localhost/mijn_database?charset=utf8mb4")

# Lees de query uit het bestand
with open(args.query, 'r') as f:
    sql = f.read()

# Voer de query uit en laad de resultaten in een DataFrame
df = pd.read_sql(sql, engine)

# Laat de eerste paar rijen van de DataFrame zien
print(df.head())
