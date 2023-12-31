from treatment import DataProcessor  # Import DataProcessor class
from schema import DatabaseManager  # Import DatabaseManager class
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv()

    # Initialize DataProcessor and DatabaseManager instances
    data_manager = DataProcessor(
        dbname=os.getenv("dbname"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host")
    )

    db_manager = DatabaseManager(
        dbname=os.getenv("dbname"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host")
    )

    path_state = 'data/state'
    path_flight = 'data/flights'
    schema_name = "brazil"

    db_manager.connect()  # Connect to the database

    # Create schema and tables, and add foreign keys
    db_manager.create_schema(schema_name)
    db_manager.create_flights_table(schema_name)
    db_manager.create_state_codes_table(schema_name)
    db_manager.add_foreign_key(schema_name)

    db_manager.disconnect()  # Disconnect from the database

    data_manager.process_files_state(directory=path_state)  # Process state files and load data
    data_manager.process_files_flights(directory=path_flight)  # Process flight files and load data
