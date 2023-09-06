import pymongo
import json

def connect_to_mongodb(database_name, collection_name):
    # Establecer la conexión con el servidor MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[database_name]
    collection = db[collection_name]
    return collection

def read_json_file(json_file_path):
    json_list = []

    with open(json_file_path, "r") as json_file:
        for line in json_file:
            try:
                json_data = json.loads(line)
                json_list.append(json_data)

            except json.JSONDecodeError:
                print("Error de decodificación JSON en la línea:", line)

    return json_list

def save_json_to_mongo(collection, json_list):
    if json_list:
        collection.insert_many(json_list)

def main():
    database_name = "db"
    collection_name = "webScraping"
    json_file_path = "../data/resultado_join.json"

    collection = connect_to_mongodb(database_name, collection_name)
    json_list = read_json_file(json_file_path)
    save_json_to_mongo(collection, json_list)

    print("Datos almacenados en MongoDB con éxito.")

if __name__ == "__main__":
    main()
