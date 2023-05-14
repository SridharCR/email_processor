from sqlalchemy import create_engine

PSQL_DB_URI = "postgresql+psycopg2://postgres:sridharpass@localhost:5433/postgres"
postgresql_engine = create_engine(PSQL_DB_URI)
