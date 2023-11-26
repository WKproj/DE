from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("amazon_sales").getOrCreate()

df = spark.read.csv('C:\\Users\\Admin\\Documents\\GitHub\\DE-1\\raw_data\\Amazon Sale Report.csv', header=True)
df = df.toDF(*[c.lower() for c in df.columns])
df = df.drop('unnamed: 22')


from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@localhost:5432/dbname')

df.to_sql("my_table_name", engine)


import configparser

config = configparser.ConfigParser()
config.sections()
config.read("C:\\Users\\Admin\\Documents\\GitHub\\DE-1\\passwords\\config.ini")

# Pobierz dane do połączenia z bazą danych
database_config = config['database']
jdbc_url = f"jdbc:postgresql://{database_config['host']}:{database_config['port']}/{database_config['dbname']}"
properties = {"user": database_config['user'], "password": database_config['password'], "driver": "org.postgresql.Driver"}


# Zapis danych do tabeli ecom.sales w PostgreSQL
df.write.jdbc(url=jdbc_url, table="ecom.sales", mode="overwrite", properties=properties)


spark.stop()