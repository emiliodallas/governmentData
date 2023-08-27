import pandas as pd
import psycopg2
import glob
import unidecode
import unicodedata
import os


class DataProcessor:
    def __init__(self, dbname, user, password, host):
        # Constructor for DataProcessor class
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None
        self.cur = None

    def _clean_strings(self, df):
        def remove_accents(input_str):
            # Helper function to remove accents from strings
            nfkd_form = unicodedata.normalize('NFKD', input_str)
            return unidecode.unidecode(nfkd_form)

        # Remove spaces and special characters from Município column
        df["Município"] = df["Município"].apply(remove_accents).str.replace(r'[^a-zA-Z0-9]', '', regex=True)

        return df

    def _insert_state_data(self, df):
        # Insert state data into the database
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

        cur = self.conn.cursor()

        schema_name = "brazil"
        state_codes_table_name = "state_codes"

        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {schema_name}.{state_codes_table_name} (code, state, city)
                VALUES ('{row['Indicador']}', '{row['UF']}', '{row['Município']}');
            """
            cur.execute(insert_query)

        self.conn.commit()
        cur.close()
        self.conn.close()

    def _insert_flight_data(self, df):
        # Insert flight data into the database
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

        cur = self.conn.cursor()

        schema_name = "brazil"
        state_flights_table_name = "flights"
        table_support = 'state_codes'

        for index, row in df.iterrows():
            price = float(row['TARIFA'].replace(',', '.'))
            origin_query = f"SELECT aerodromoId FROM {schema_name}.{table_support} WHERE code = '{row['ORIGEM']}'"
            destiny_query = f"SELECT aerodromoId FROM {schema_name}.{table_support} WHERE code = '{row['DESTINO']}'"

            self.cur.execute(origin_query)
            state_origin_result = self.cur.fetchone()
            state_origin_id = state_origin_result[0] if state_origin_result else '999'

            self.cur.execute(destiny_query)
            state_destiny_result = self.cur.fetchone()
            state_destiny_id = state_destiny_result[0] if state_destiny_result else '999'

            insert_query = f"""
                INSERT INTO {schema_name}.{state_flights_table_name} (date, company, origin, destiny, price,
                                                                    seats, stateOriginId, stateDestinyId)
                VALUES ('{row['DATA']}', '{row['EMPRESA']}', '{row['ORIGEM']}',
                '{row['DESTINO']}', {price}, '{row['ASSENTOS']}', {state_origin_id}, {state_destiny_id});
            """
            cur.execute(insert_query)
        self.conn.commit()
        cur.close()
        self.conn.close()

    def process_files_state(self, directory):
        # Process state files and load data to the database
        state_files = glob.glob(f"{directory}/*.xlsx")

        for state_file in state_files:
            df = pd.read_excel(state_file)  # Read Excel file

            # Drop rows with missing codes
            df = df.dropna(subset=["Indicador"])

            # Clean up strings
            df = self._clean_strings(df)

            # Save cleaned data to a new Excel file
            cleaned_filename = f"data/cleaned_{state_file}"
            df.to_excel(cleaned_filename, index=False)
            self._insert_state_data(df=df)
            print("Processed state file inserted into DB")

    def process_files_flights(self, directory):
        # Process flight files and load data to the database
        flights_files = []

        # Recursively search for CSV files in the specified directory and its subdirectories
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(".csv"):
                    flights_files.append(os.path.join(root, file))

        for csv_file in flights_files:
            print(f"Processing flight file: {csv_file}")
            df = pd.read_csv(csv_file, sep=';', encoding='latin1')

            # Create a new column "DATA" by concatenating "Year" and "Month"
            try:
                df["DATA"] = df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-1"
            except:
                df = df.rename(columns={'Ano de Referência': 'ANO',
                                        'Mês de Referência': 'MES',
                                        'ICAO Empresa Aérea': 'EMPRESA',
                                        'ICAO Aeródromo Origem': 'ORIGEM',
                                        'ICAO Aeródromo Destino': 'DESTINO',
                                        'Tarifa-N': 'TARIFA',
                                        'Assentos Comercializados': 'ASSENTOS'}
                              )
                df["DATA"] = df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-1"

            # Extract the directory path where the cleaned file will be saved
            cleaned_directory = os.path.join("data", "cleaned_data", "flight",
                                             os.path.dirname(os.path.relpath(csv_file, 
                                                                             start=directory)))

            # Create the directory if it doesn't exist
            os.makedirs(cleaned_directory, exist_ok=True)

            # Construct the path for the cleaned file
            cleaned_filename = os.path.join(cleaned_directory, os.path.basename(csv_file))

            # Save cleaned data to a new CSV file
            df.to_csv(cleaned_filename, index=False, sep=';')
            print(f"Processed flight file saved: {cleaned_filename}")
            self._insert_flight_data(df=df)
            print(f"Processed flight file inserted into DB: {cleaned_filename}")
