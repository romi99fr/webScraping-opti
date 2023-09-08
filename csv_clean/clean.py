import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,avg,when,round
from functools import reduce

def initialize_spark_session():
    return SparkSession.builder.appName("HDFSFileRead").getOrCreate()

def download_csv_files(csv_files, local_folder):
    subprocess.run(["mkdir", "-p", local_folder])
    
    for csv_file in csv_files:
        subprocess.run(["../../hadoop-2.7.4/bin/hdfs", "dfs", "-get", csv_file, local_folder])

def process_csv_file(file_name, df, spark):
    if file_name == "allotjament_turistic.csv":
        df = df.withColumnRenamed("Dto", "Codi_Districte")
        df = df.withColumnRenamed("Habitatges d'ús turístic", "Habitatges_dus_turístic")
        selected_columns = ["Codi_Districte", "Habitatges_dus_turístic", "Hotel", "Residències estudiants"]
        selected_df = df.select(*selected_columns)
        
        return selected_df 
    

    if file_name == "promedio_ventas.csv":
        filtered_df = df.select("Codi_Districte", "Promedio €/m2", "Promedio_Vivienda")
        return filtered_df

    if file_name == "seguretat.csv":
        df = df.withColumn("Nom_Districte", F.split(col("Districte"), "\\.").getItem(1))
        selected_columns = ["Nom_Districte", "Denúncies per infracció de l’ordenança de convivència ciutadana (% habitants)","Incidents per degradació de l'espai públic (‰ habitants)"]
        df = df.select(selected_columns)
        # Renombrar las columnas para que coincidan con los otros DataFrames
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        df = df.withColumnRenamed("Denúncies per infracció de l’ordenança de convivència ciutadana (% habitants)", "Denuncias_Convivencia")
        df = df.withColumnRenamed("Incidents per degradació de l'espai públic (‰ habitants)", "Incidents_Degradacion")
        return df

    if file_name == "m2.csv":
        df = df.withColumnRenamed("Dto", "Codi_Districte")
        #  Calcular la media por distrito para el año 2021
        aggregated_df = df.groupBy("Codi_Districte").agg(round(avg("2021"),3).alias("€/m2"))
        return aggregated_df.dropDuplicates()

    if file_name == "atur.csv":
        df = df.withColumnRenamed("Dte", "Codi_Districte")
        #  Calcular la media por distrito para el año 2021
        aggregated_df = df.groupBy("Codi_Districte").agg(round(avg("2021"), 2).alias("Atur"))
        return aggregated_df.dropDuplicates()
     
    if file_name == "vehicles_districte.csv":
        filtered_df = df.filter(df["Tipus_Servei"] == "Privat")
        aggregated_df = filtered_df.groupBy("Nom_Districte").agg(F.sum("Total").alias("Vehicles"))
        return aggregated_df.dropDuplicates()

    if file_name == "Taula_mapa_districte.csv":
        aggregated_df = df.groupBy("Nom_Districte", "Codi_Districte").agg(F.sum("Nombre").alias("Personas"))
        
        return aggregated_df.dropDuplicates()

    if file_name == "renda_neta_mitjana_per_persona.csv":
        df = df.withColumn("Nom_Districte", when(col("Nom_Districte") == "L'Eixample", "Eixample").otherwise(col("Nom_Districte")))
        promedio_distrito = df.groupBy("Nom_Districte").agg(F.avg("Import_Euros").alias("Promedio_Import_Euros"))
        promedio_distrito = promedio_distrito.withColumn("Promedio_Import_Euros", round(col("Promedio_Import_Euros"), 2))
        return promedio_distrito.dropDuplicates()

def main():
    # Lista de archivos CSV en HDFS
    csv_files = [
        "webScraping/Taula_mapa_districte.csv",
        "webScraping/renda_neta_mitjana_per_persona.csv",
        "webScraping/vehicles_districte.csv",
        "webScraping/atur.csv",
        "webScraping/m2.csv",
        "webScraping/allotjament_turistic.csv",
        "webScraping/seguretat.csv",
        "webScraping/promedio_ventas.csv",
    ]

    # Carpeta local donde se guardarán los archivos descargados
    local_folder = "../csv_data/"
    
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
        if processed_df is not None:
            modified_dfs[file_name] = processed_df
    
      # Realizar el primer join de tres DataFrames por Nom_Districte
    vehicles_df = modified_dfs["vehicles_districte.csv"]
    taula_mapa_df = modified_dfs["Taula_mapa_districte.csv"]
    renda_neta_df = modified_dfs["renda_neta_mitjana_per_persona.csv"]
    atur =  modified_dfs["atur.csv"]
    m2 =  modified_dfs["m2.csv"]
    seguretat = modified_dfs["seguretat.csv"]
    promedio_vivienda = modified_dfs["promedio_ventas.csv"]
    allotjament_turistic = modified_dfs["allotjament_turistic.csv"]

    final_df = vehicles_df.join(taula_mapa_df, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(renda_neta_df, on="Nom_Districte", how="inner")

    final_df = final_df.join(seguretat, on="Nom_Districte", how="inner")

    final_df = final_df.join(atur, on="Codi_Districte", how="inner")

    final_df = final_df.join(m2, on="Codi_Districte", how="inner")

    final_df = final_df.join(promedio_vivienda, on="Codi_Districte", how="inner")

    final_df = final_df.join(allotjament_turistic, on="Codi_Districte", how="inner")


    # Cambiar el orden de las columnas
    column_order = ['Codi_Districte', 'Nom_Districte', 'Personas', 'Promedio_Import_Euros', 'Vehicles', 'Atur', '€/m2', 'Denuncias_Convivencia', 'Incidents_Degradacion', 'Promedio_Vivienda', 'Habitatges_dus_turístic', 'Hotel', 'Residències estudiants']
    final_df = final_df.distinct()
    final_df = final_df[column_order]
    final_df = final_df.orderBy("Codi_Districte")

    # Mostrar los primeros 100 registros
    final_df.show()

    # Guardar el DataFrame como CSV local
    standalone_csv_path = "../csv_data/combined_df.csv"
    combined_df_single_partition = final_df.coalesce(1)
    combined_pandas_df = combined_df_single_partition.toPandas()

    # Save the Pandas DataFrame as a single CSV file
    combined_pandas_df.to_csv(standalone_csv_path, index=False)

    # Subir el archivo CSV a HDFS
    hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
    put_command = [hadoop_bin, "dfs", "-put", "-f", standalone_csv_path, "webScraping-opti/combined_df.csv"]
    subprocess.run(put_command, check=True)

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()

