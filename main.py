from treatment import DataProcessor
from schema import DatabaseManager

if __name__ == "__main__":
    
    data_manager = DataProcessor(
        dbname="database_Hilab",
        user="postgres",
        password="123",
        host="localhost"
    )

    db_manager = DatabaseManager(
        dbname="database_Hilab",
        user="postgres",
        password="123",
        host="localhost"
    )

    pathState = 'data/state'
    pathFlight = 'data/flights'
    schema_name = "brazil"
    
    db_manager.connect()

    db_manager.create_schema(schema_name)
    db_manager.create_flights_table(schema_name)
    db_manager.create_state_codes_table(schema_name)
    db_manager.add_foreign_key(schema_name)

    db_manager.disconnect()

    data_manager.process_files_state(directory=pathState)
    data_manager.process_files_flights(directory=pathFlight)