import os
import requests
import subprocess

# Define the URL and file information for each CSV file
csv_info = [
    {
        "url": "https://opendata-ajuntament.barcelona.cat/data/dataset/5411c8e1-1ede-47d6-92ce-2035141d8721/resource/f791aeba-2570-4e37-b957-c6036a0c28f7/download",
        "local_filename": "renda_neta_mitjana_per_persona.csv"
    },
    {
        "url": "https://opendata-ajuntament.barcelona.cat/data/dataset/de5c4f45-24eb-4672-9bc4-f12508a0060a/resource/cd3125e4-f7d3-4217-8f9d-6d7abdad6ab0/download",
        "local_filename": "Taula_mapa_districte.csv"
    },
    {
        "url": "https://opendata-ajuntament.barcelona.cat/data/dataset/802f9370-9c24-489a-9397-215220959afb/resource/f1c7e6f6-d975-4b7d-b4ce-82dd810300b1/download",
        "local_filename": "vehicles_districte.csv"
    },
    {
        "local_path": "../csv_data/atur.csv",
        "local_filename": "atur.csv"
    },
    {
        "local_path": "../csv_data/m2.csv",
        "local_filename": "m2.csv"
    },
    {
        "local_path": "../csv_data/allotjament_turistic.csv",
        "local_filename": "allotjament_turistic.csv"
    },
    {
        "local_path": "../csv_data/promedio_ventas.csv",
        "local_filename": "promedio_ventas.csv"
    },
    {
        "local_path": "../csv_data/seguretat.csv",
        "local_filename": "seguretat.csv"
    }
]

local_directory = "../csv_data"
os.makedirs(local_directory, exist_ok=True)

def download_csv(url, local_file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            file.write(response.content)
        print(f"CSV file '{local_file_path}' downloaded successfully.")
    else:
        print(f"Failed to download CSV file from '{url}'. Status code: {response.status_code}")

def upload_to_hdfs(local_file_path, hdfs_directory):
    hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
    put_command = [hadoop_bin, "dfs", "-put", "-f", local_file_path, hdfs_directory]
    subprocess.run(put_command, check=True)
    print(f"Uploaded '{local_file_path}' to HDFS directory '{hdfs_directory}'.")

for csv in csv_info:
    if "url" in csv:
        url = csv["url"]
        local_file_name = csv["local_filename"]
        local_file_path = os.path.join(local_directory, local_file_name)
        hdfs_directory = f"webScraping-opti/{local_file_name}"

        download_csv(url, local_file_path)
        upload_to_hdfs(local_file_path, hdfs_directory)
    elif "local_path" in csv:
        local_file_path = csv["local_path"]
        local_file_name = csv["local_filename"]
        hdfs_directory = f"webScraping-opti/{local_file_name}"

        upload_to_hdfs(local_file_path, hdfs_directory)


