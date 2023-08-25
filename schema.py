import psycopg2


def createTable(dbName, userName, pwd, host):
    conn = psycopg2.connect(
    dbName="your_db_name",
    userName="your_username",
    pwd="your_password",
    host="your_host"
    )

    cur = conn.cursor()

    schemaName = "brazil"
    tableName = "flights"

    tableSupport = "stateCodes"

    createFlights= f"""
    CREATE TABLE IF NOT EXISTS {schemaName}.{tableName} (
        flightId SERIAL PRIMARY KEY,
        date DATE,
        company VARCHAR,
        origin VARCHAR,
        destiny VARCHAR,
        price NUMERICAL(10,1)
        seats INTEGER
    );
    """

    createSupport = f"""
    CREATE TABLE IF NOT EXISTS {schemaName}.{tableSupport} (
        aerodromoId SERIAL PRIMARY KEY,
        code VARCHAR,
        state VARCHAR,
        city VARCHAR
    );
    """

     # Adding a foreign key column to the flights table
    addForeignKeyQuery = f"""
        ALTER TABLE {schemaName}.{tableName}
        ADD COLUMN state_id INTEGER REFERENCES {schemaName}.{tableSupport}(aerodromoId);
    """


    cur.execute(addForeignKeyQuery)
    cur.execute(createFlights)
    cur.execute(createSupport)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":

    createTable(dbName=, userName=, pwd=, host=)