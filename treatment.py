import pandas as pd
import psycopg2
import glob
import unidecode, unicodedata
import os


class DataProcessor:
    def __init__(self, dbName, userName, pwd, host):
        self.dbName = dbName
        self.userName = userName
        self.pwd = pwd
        self.host = host

    def _clean_strings(self, df):
        def remove_accents(input_str):
            nfkd_form = unicodedata.normalize('NFKD', input_str)
            return unidecode.unidecode(nfkd_form)

        # Remove spaces and special characters
        df["Município"] = df["Município"].apply(remove_accents).str.replace(r'[^a-zA-Z0-9]', '', regex=True)

        return df

    def _insert_state_data(self, df):
        conn = psycopg2.connect(
            dbname=self.dbName,
            user=self.userName,
            password=self.pwd,
            host=self.host
        )

        cur = conn.cursor()

        schemaName = "brazil"
        stateCodesTableName = "stateCodes"

        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {schemaName}.{stateCodesTableName} (code, state, city)
                VALUES ('{row['Indicador']}', '{row['UF']}', '{row['Município']}');
            """
            cur.execute(insert_query)

        conn.commit()
        cur.close()
        conn.close()

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
            # Clean up strings
            #df = self._clean_strings(df)

            # Extract the directory path where the cleaned file will be saved
            cleaned_directory = os.path.join("data", "cleaned_data", "flight", os.path.dirname(os.path.relpath(csv_file, start=directory)))

            # Create the directory if it doesn't exist
            os.makedirs(cleaned_directory, exist_ok=True)

            # Construct the path for the cleaned file
            cleaned_filename = os.path.join(cleaned_directory, os.path.basename(csv_file))

            # Save cleaned data to a new CSV file
            df.to_csv(cleaned_filename, index=False, sep=';')
            print(f"Processed file saved: {cleaned_filename}")


    def load_state_data(self, directory):
        state_data_files = glob.glob(f"{directory}/*.csv")

        state_data = pd.DataFrame()  # Create an empty DataFrame to hold the state data

        for csv_file in state_data_files:
            df = pd.read_csv(csv_file)  # Read state data CSV file

            # Append the state data to the DataFrame
            state_data = state_data.append(df, ignore_index=True)

        return state_data


if __name__ == "__main__":
    pathState = 'data/state'
    pathFlight = 'data/flights'

    dbName = "your_db_name"
    userName = "your_username"
    pwd = "your_password"
    host = "your_host"

    data_processor = DataProcessor(dbName, userName, pwd, host)

    data_processor.process_files_state(directory=pathState)
    data_processor.process_files_flights(directory=pathFlight)
