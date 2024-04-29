import luigi
import pandas as pd
import datetime
from pymongo import MongoClient
from luigi.contrib.postgres import CopyToTable

class ExtractFromMongoDB(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        """ Saving it locally to see what data I got form MongoDB """
        return luigi.LocalTarget('data_from_mongodb.csv')

    def run(self):
        # Connecting to MongoDB
        client = MongoClient(
            'mongodb+srv://Hamza_x23115033:hamzapaskingakhtar@clusterncidap1.gbew9wh.mongodb.net/?retryWrites=true&w=majority',
            ssl=True
        )
        db = client['DAP_PROJECT_2024']
        collection = db['tmdb_rated_movie']

        # Fetching data and converting to dataframe
        data = pd.DataFrame(list(collection.find()))

        # Saving data to CSV
        data.to_csv(self.output().path, index=False)

class TransformData(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ExtractFromMongoDB(self.date)

    def output(self):
        return luigi.LocalTarget('transformed_data.csv')

    def run(self):
        df = pd.read_csv(self.input().path)

        # Dropping the '_id' field here as it is not needed
        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        # Converting 'release_date' from string to datetime format and extracting the year as part of the transformation
        df['release_date'] = pd.to_datetime(df['release_date'])
        df['release_year'] = df['release_date'].dt.year

        # Saving the transformed data
        df.to_csv(self.output().path, index=False)

class LoadToPostgres(CopyToTable):
    date = luigi.DateParameter(default=datetime.date.today())
    host = 'database-1.cjm246e0wkky.eu-north-1.rds.amazonaws.com' #localhost
    database = 'dab-final-database' # taba1
    user = 'postgres'
    password = 'hamzapaskingakhtar'
    port = "5432"
    table = 'tmdb_rated_movies'
    columns = [
        ("id", "INTEGER"),
        ("title", "TEXT"),
        ("overview", "TEXT"),
        ("release_date", "DATE"),
        ("popularity", "FLOAT"),
        ("vote_average", "FLOAT"),
        ("vote_count", "INTEGER"),
        ("release_year", "INTEGER")
    ]

    def requires(self):
        return TransformData(self.date)

    def rows(self):
        with open(self.input().path, 'r', encoding='utf-8') as f:
            df = pd.read_csv(f)
            for _, row in df.iterrows():
                yield row.tolist()


if __name__ == '__main__':
    luigi.build([LoadToPostgres()], local_scheduler=True)
