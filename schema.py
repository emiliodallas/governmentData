import psycopg2

class DatabaseManager:
    def __init__(self, dbname, user, password, host):
        # Constructor for DatabaseManager class
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None
        self.cur = None

    def connect(self):
        # Establish a connection to the database
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

    def disconnect(self):
        # Close the cursor and connection
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def execute_query(self, query):
        # Execute a query and commit changes
        self.cur.execute(query)
        self.conn.commit()

    def create_schema(self, schema_name):
        # Create a schema if it doesn't exist
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        self.execute_query(create_schema_query)

    def create_flights_table(self, schema_name):
        # Create a flights table if it doesn't exist
        create_flights_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.flights (
                flightId SERIAL PRIMARY KEY,
                date DATE,
                company VARCHAR,
                origin VARCHAR,
                destiny VARCHAR,
                price NUMERIC(10, 1),
                seats INTEGER
            );
        """
        self.execute_query(create_flights_query)

    def create_state_codes_table(self, schema_name):
        # Create a state codes table if it doesn't exist
        create_state_codes_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.state_codes (
                aerodromoId SERIAL PRIMARY KEY,
                code VARCHAR,
                state VARCHAR,
                city VARCHAR
            );
        """
        self.execute_query(create_state_codes_query)

    def add_foreign_key(self, schema_name):
        # Add foreign key references to flights table
        add_foreign_key_query = f"""
            ALTER TABLE {schema_name}.flights
            ADD COLUMN stateOriginId INTEGER REFERENCES {schema_name}.state_codes(aerodromoId),
            ADD COLUMN stateDestinyId INTEGER REFERENCES {schema_name}.state_codes(aerodromoId);
        """
        self.execute_query(add_foreign_key_query)
