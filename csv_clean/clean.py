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
    if file_name == "centresCivics.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        df = df.withColumn("Nom_Districte", when(col("Nom_Districte") == "Sarrià-St.Gervasi", "Sarrià-Sant Gervasi").otherwise(col("Nom_Districte")))
        result = df.groupBy("Nom_Districte").agg(F.count("Centre cívic").alias("Nombre de Centres Civics"))

        return result.dropDuplicates()
    
    if file_name == "aparcaments.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        df = df.withColumnRenamed("Nombre de places d'aparcament Total", "Nombre de places aparcament Total")
        df = df.withColumnRenamed("Nombre de places d'aparcament Residencies", "Nombre de places aparcament Residencies")
        df = df.withColumnRenamed("Nombre de places d'aparcament Altres edificis", "Nombre de places aparcament Altres edificis")
        selected_columns = ["Nom_Districte", "Nombre de places aparcament Total","Nombre de places aparcament Residencies", "Nombre de places aparcament Altres edificis"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "us_del_sol.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte", "Residencial","Equipaments","Indústria i infraestructures","Xarxa viària","Parcs forestals"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "poblacio.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte", "PoblacióTotal", "Densitat de població (hab/km²)","Edat mitjana","Domicilis"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "nacionalitats.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte", "Nacionalitat espanyola","Nacionalitat estrangera"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "mobilitat_feina.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte", "A peu + bicicleta","Transport privat", "Transport públic"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "guals.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        df = df.withColumnRenamed("Total", "Total Guals")
        selected_columns = ["Nom_Districte", "Total Guals"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "densitat_turismes.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte","Turismes","Superfície (km²)","Habitants/Turisme","Turismes/km²"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()
    
    if file_name == "arbres_parcs.csv":
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        selected_columns = ["Nom_Districte", "Parcs urbans", "Arbres al carrer"]
        selected_df = df.select(*selected_columns)

        return selected_df.dropDuplicates()

    if file_name == "allotjament_turistic.csv":
        df = df.withColumnRenamed("Dto", "Codi_Districte")
        df = df.withColumnRenamed("Habitatges d'ús turístic", "Habitatges_dus_turístic")
        selected_columns = ["Codi_Districte", "Habitatges_dus_turístic", "Hotel", "Residències estudiants"]
        selected_df = df.select(*selected_columns)
        
        return selected_df.dropDuplicates()
    
    if file_name == "promedio_ventas.csv":
        filtered_df = df.select("Codi_Districte", "Promedio €/m2", "Promedio_Vivienda")
        return filtered_df.dropDuplicates()

    if file_name == "seguretat.csv":
        df = df.withColumn("Nom_Districte", F.split(col("Districte"), "\\.").getItem(1))
        selected_columns = ["Nom_Districte", "Denúncies per infracció de l’ordenança de convivència ciutadana (% habitants)","Incidents per degradació de l'espai públic (‰ habitants)"]
        df = df.select(selected_columns)
        # Renombrar las columnas para que coincidan con los otros DataFrames
        df = df.withColumnRenamed("Districte", "Nom_Districte")
        df = df.withColumnRenamed("Denúncies per infracció de l’ordenança de convivència ciutadana (% habitants)", "Denuncias_Convivencia")
        df = df.withColumnRenamed("Incidents per degradació de l'espai públic (‰ habitants)", "Incidents_Degradacion")
        return df.dropDuplicates()
    
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

    if file_name == "renda_neta_mitjana_per_persona.csv":
        promedio_distrito = df.groupBy("Nom_Districte","Codi_Districte").agg(F.avg("Import_Euros").alias("Promedio_Import_Euros"))
        promedio_distrito = promedio_distrito.withColumn("Promedio_Import_Euros", round(col("Promedio_Import_Euros"), 2))
        return promedio_distrito.dropDuplicates()

def main():
    # Lista de archivos CSV en HDFS
    csv_files = [
        "webScraping/renda_neta_mitjana_per_persona.csv",
        "webScraping/atur.csv",
        "webScraping/m2.csv",
        "webScraping/allotjament_turistic.csv",
        "webScraping/seguretat.csv",
        "webScraping/promedio_ventas.csv",
        "webScraping/aparcaments.csv",
        "webScraping/arbres_parcs.csv",
        "webScraping/centresCivics.csv",
        "webScraping/densitat_turismes.csv",
        "webScraping/guals.csv",
        "webScraping/mobilitat_feina.csv",
        "webScraping/nacionalitats.csv",
        "webScraping/poblacio.csv",
        "webScraping/us_del_sol.csv"
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
    renda_neta_df = modified_dfs["renda_neta_mitjana_per_persona.csv"]
    atur =  modified_dfs["atur.csv"]
    m2 =  modified_dfs["m2.csv"]
    seguretat = modified_dfs["seguretat.csv"]
    promedio_vivienda = modified_dfs["promedio_ventas.csv"]
    allotjament_turistic = modified_dfs["allotjament_turistic.csv"]
    aparcaments = modified_dfs["aparcaments.csv"]
    arbres_parcs = modified_dfs["arbres_parcs.csv"]
    centresCivics = modified_dfs["centresCivics.csv"]
    densitat_turismes = modified_dfs["densitat_turismes.csv"]
    guals = modified_dfs["guals.csv"]
    mobilitat_feina = modified_dfs["mobilitat_feina.csv"]
    nacionalitats = modified_dfs["nacionalitats.csv"]
    poblacio = modified_dfs["poblacio.csv"]
    us_del_sol = modified_dfs["us_del_sol.csv"]

    
    final_df = renda_neta_df.join(seguretat, on="Nom_Districte", how="inner")

    final_df = final_df.join(aparcaments, on="Nom_Districte", how="inner")
 
    final_df = final_df.join(arbres_parcs, on="Nom_Districte", how="inner")
     
    final_df = final_df.join(centresCivics, on="Nom_Districte", how="inner")

    final_df = final_df.join(densitat_turismes, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(guals, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(mobilitat_feina, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(nacionalitats, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(poblacio, on="Nom_Districte", how="inner")

    final_df = final_df.join(us_del_sol, on="Nom_Districte", how="inner")
    
    final_df = final_df.join(atur, on="Codi_Districte", how="inner")

    final_df = final_df.join(m2, on="Codi_Districte", how="inner")
    
    final_df = final_df.join(promedio_vivienda, on="Codi_Districte", how="inner")
    
    final_df = final_df.join(allotjament_turistic, on="Codi_Districte", how="inner")


    # Cambiar el orden de las columnas
    column_order = ['Codi_Districte','Nom_Districte','Promedio_Import_Euros','Atur','€/m2','Denuncias_Convivencia','Incidents_Degradacion', 
                    'Promedio_Vivienda', 'Habitatges_dus_turístic', 'Hotel', 'Residències estudiants', 'Parcs urbans', 'Arbres al carrer','Turismes','Superfície (km²)','Habitants/Turisme','Turismes/km²','Total Guals', 'A peu + bicicleta','Transport privat', 'Transport públic', 'Nacionalitat espanyola','Nacionalitat estrangera',
                    'PoblacióTotal', 'Densitat de població (hab/km²)','Edat mitjana','Domicilis', 'Residencial','Equipaments','Indústria i infraestructures','Xarxa viària','Parcs forestals','Nombre de places aparcament Total','Nombre de places aparcament Residencies','Nombre de places aparcament Altres edificis','Nombre de Centres Civics']
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

