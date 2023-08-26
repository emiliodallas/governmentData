import pandas as pd
import psycopg2
import glob
import unidecode, unicodedata
import os


class DataProcessor:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None
        self.cur = None

    def _clean_strings(self, df):
        def remove_accents(input_str):
            nfkd_form = unicodedata.normalize('NFKD', input_str)
            return unidecode.unidecode(nfkd_form)

        # Remove spaces and special characters
        df["Município"] = df["Município"].apply(remove_accents).str.replace(r'[^a-zA-Z0-9]', '', regex=True)

        return df

    def _insert_state_data(self, df):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

        cur = self.conn.cursor()

        schemaName = "brazil"
        stateCodesTableName = "stateCodes"

        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {schemaName}.{stateCodesTableName} (code, state, city)
                VALUES ('{row['Indicador']}', '{row['UF']}', '{row['Município']}');
            """
            cur.execute(insert_query)

        self.conn.commit()
        cur.close()
        self.conn.close()

    def insert_flight_data(self, df):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

        cur = self.conn.cursor()

        schemaName = "brazil"
        stateFlightsTableName = "flights"
        tableSupport = 'stateCodes'

        for index, row in df.iterrows():
            price = float(row['TARIFA'].replace(',', '.'))
            origin_query = f"SELECT aerodromoId FROM {schemaName}.{tableSupport} WHERE code = '{row['ORIGEM']}'"
            destiny_query = f"SELECT aerodromoId FROM {schemaName}.{tableSupport} WHERE code = '{row['DESTINO']}'"

            
            self.cur.execute(origin_query)
            state_origin_result = self.cur.fetchone()
            state_origin_id = state_origin_result[0] if state_origin_result else '999'

            self.cur.execute(destiny_query)
            state_destiny_result = self.cur.fetchone()
            state_destiny_id = state_destiny_result[0] if state_destiny_result else '999'

            insert_query = f"""
                INSERT INTO {schemaName}.{stateFlightsTableName} (date, company, origin, destiny, price, 
                                                                    seats, stateOriginId, stateDestinyId)
                VALUES ('{row['DATA']}', '{row['EMPRESA']}', '{row['ORIGEM']}',
                '{row['DESTINO']}', {price}, '{row['ASSENTOS']}', {state_origin_id}, {state_destiny_id});
            """
            cur.execute(insert_query)
        print("Data loaded to DB")
        self.conn.commit()
        cur.close()
        self.conn.close()

    def process_files_state(self, directory):
        stateFiles = glob.glob(f"{directory}/*.xlsx")

        for stateFile in stateFiles:
            df = pd.read_excel(stateFile)  # Read Excel file

            # Drop rows with missing codes
            df = df.dropna(subset=["Indicador"])

            # Clean up strings
            df = self._clean_strings(df)

            # Save cleaned data to a new Excel file
            cleaned_filename = f"data/cleaned_{stateFile}"
            df.to_excel(cleaned_filename, index=False)
            self._insert_state_data(df=df)
            print("State Loaded to DB")

    def process_files_flights(self, directory):
        flightsFiles = []

        # Recursively search for CSV files in the specified directory and its subdirectories
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(".csv"):
                    flightsFiles.append(os.path.join(root, file))

        for csv_file in flightsFiles:
            print(f"Processing file: {csv_file}")
            df = pd.read_csv(csv_file, sep=';',encoding='latin1')            

            # Create a new column "Year_Month" by concatenating "Year" and "Month"
            try:
                df["DATA"] = df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-1"
            except:
                df = df.rename(columns={'Ano de Referência' : 'ANO',
                                        'Mês de Referência' : 'MES',
                                        'ICAO Empresa Aérea' : 'EMPRESA',
                                        'ICAO Aeródromo Origem' : 'ORIGEM',
                                        'ICAO Aeródromo Destino' : 'DESTINO',
                                        'Tarifa-N' : 'TARIFA',
                                        'Assentos Comercializados' : 'ASSENTOS'
                                        }
                                )
                df["DATA"] = df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-1"
            
            # Extract the directory path where the cleaned file will be saved
            cleaned_directory = os.path.join("data", "cleaned_data", "flight", os.path.dirname(os.path.relpath(csv_file, start=directory)))

            # Create the directory if it doesn't exist
            os.makedirs(cleaned_directory, exist_ok=True)

            # Construct the path for the cleaned file
            cleaned_filename = os.path.join(cleaned_directory, os.path.basename(csv_file))

            # Save cleaned data to a new CSV file
            df.to_csv(cleaned_filename, index=False, sep=';')
            self.insert_flight_data(df=df)
            print(f"Processed file saved: {cleaned_filename}")

