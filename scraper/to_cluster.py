import subprocess

def create_hdfs_directory(directory_name):
    hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
    mkdir_command = [hadoop_bin, "dfs", "-mkdir", "-p", directory_name]
    subprocess.run(mkdir_command, check=True)

def transfer_data_to_hdfs(local_path, hdfs_path):
    hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
    transfer_command = [hadoop_bin, "dfs", "-put", local_path, hdfs_path]
    subprocess.run(transfer_command, check=True)

def main():
    hdfs_directory = "webScraping-opti"
    local_data_path = "../data/data.json"
    hdfs_data_path = f"{hdfs_directory}/data.json"
    
    # Crear el directorio en HDFS si no existe
    create_hdfs_directory(hdfs_directory)
    
    # Transferir datos a HDFS
    transfer_data_to_hdfs(local_data_path, hdfs_data_path)

if __name__ == "__main__":
    main()
