import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from functools import reduce

def initialize_spark_session():
    return SparkSession.builder.appName("HDFSFileRead").getOrCreate()

def download_csv_files(csv_files, local_folder):
    subprocess.run(["mkdir", "-p", local_folder])
    
    for csv_file in csv_files:
        subprocess.run(["../../hadoop-2.7.4/bin/hdfs", "dfs", "-get", csv_file, local_folder])

def process_csv_file(file_name, df, spark):
    if file_name == "vehicles_districte.csv":
        filtered_df = df.filter(df["Tipus_Servei"] == "Privat")
        aggregated_df = filtered_df.groupBy("Codi_Districte").agg(F.sum("Total").alias("Vehicles"))
        return aggregated_df

    if file_name == "Adreces_per_secció_censal.csv":
        filtered_df = df.select(col("NOM_CARRER"), col("Codi_Districte")).distinct()
        return filtered_df

    if file_name == "Taula_mapa_districte.csv":
        aggregated_df = df.groupBy("Codi_Districte", "Sexe").agg(F.sum("Nombre").alias("Personas"))
        return aggregated_df

    if file_name == "renda_neta_mitjana_per_persona.csv":
        promedio_distrito = df.groupBy("Codi_Districte").agg(F.avg("Import_Euros").alias("Promedio_Import_Euros"))
        return promedio_distrito

def main():
    # Lista de archivos CSV en HDFS
    csv_files = [
        "webScraping/Adreces_per_secció_censal.csv",
        "webScraping/Infraestructures_Inventari_Reserves.csv",
        "webScraping/Taula_mapa_districte.csv",
        "webScraping/renda_neta_mitjana_per_persona.csv",
        "webScraping/vehicles_districte.csv"
    ]

    # Carpeta local donde se guardarán los archivos descargados
    local_folder = "../csv_from_hdfs/"
    
    # Inicializar SparkSession
    spark = initialize_spark_session()

    # Descargar los archivos CSV desde HDFS
    download_csv_files(csv_files, local_folder)

    # Procesamiento de los archivos CSV
    modified_dfs = {}

    for csv_file in csv_files:
        file_name = csv_file.split("/")[-1]
        df = spark.read.csv(f"{local_folder}/{file_name}", header=True, inferSchema=True)
        processed_df = process_csv_file(file_name, df, spark)
        modified_dfs[file_name] = processed_df

    # Combinar DataFrames
    combined_df = reduce(lambda df1, df2: df1.join(df2, on="Codi_Districte", how="inner"), modified_dfs.values())

    # Cambiar el orden de las columnas
    column_order = ['Codi_Districte', 'Nom_Districte', 'NOM_CARRER', 'Personas', 'Promedio_Import_Euros', 'Vehicles']
    combined_df = combined_df[column_order]

    # Mostrar los primeros 100 registros
    combined_df.show(100, truncate=False)

    # Guardar el DataFrame como CSV local
    standalone_csv_path = "../csv_data/combined_df.csv"
    combined_df.coalesce(1).write.csv(standalone_csv_path, header=True, mode="overwrite")

    # Subir el archivo CSV a HDFS
    hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
    put_command = [hadoop_bin, "dfs", "-put", "-f", standalone_csv_path, "webScraping/combined_df.csv"]
    subprocess.run(put_command, check=True)

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
