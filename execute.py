import subprocess

# Ejecutar script1.py
print("Ejecutando scraper...")
subprocess.run(["python", "scraper/scrape_yaencontre.py"])
subprocess.run(["python", "scraper/add_info.py"])
subprocess.run(["python", "scraper/to_cluster.py"])

# Ejecutar script2.py
print("Ejecutando srcaper_csvs...")
subprocess.run(["python", "csv_scraper/csv_data_ingestion.py"])

# Ejecutar script3.py
print("Ejecutando clean...")
subprocess.run(["python", "csv_clean/clean.py"])

# Ejecutar script3.py
print("Ejecutando join...")
subprocess.run(["python", "join_data/json_join_csv.py"])

# Ejecutar script3.py
print("Enviando informacion a Mongo...")
subprocess.run(["python", "csv_toMongo/toMongo.py"])

print("Todos los scripts se han ejecutado.")
