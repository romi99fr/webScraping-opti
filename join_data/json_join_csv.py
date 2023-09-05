import pandas as pd
import json

def read_csv(csv_file):
    # Leer el archivo CSV en un DataFrame
    return pd.read_csv(csv_file)

def load_json(json_file):
    with open(json_file) as file:
        return json.load(file)

def merge_data(csv_df, json_data):
    json_data_cleaned = []
    
    for item in json_data:
        address = item['Address']
        distrito_parts = address['Distrito'].split(', ')
        nom_carrer = address['Calle']
        matching_row = csv_df[csv_df['Nom_Carrer'] == nom_carrer]
        
        if not matching_row.empty:
            merged_data = {
                'Codi_Districte': distrito_parts[1],
                'Nom_Districte': distrito_parts[0],
                'Nom_Carrer': nom_carrer,
                'title': item['title'],
                'size': item['size'],
                'price': item['price'],
                'Vigilancia': item['Vigilancia'],
                'Columnas': item['Columnas'],
                'Tipo plaza': item['Tipo plaza'],
                'Puerta': item['Puerta'],
                'Planta': item['Planta'],
                'Ciudad': address['Ciudad'],
                'Personas': matching_row['Personas'].values[0],
                'Promedio_Euros': matching_row['Promedio_Import_Euros'].values[0],
                'Vehicles': matching_row['Vehicles'].values[0],
            }
            json_data_cleaned.append(merged_data)
    
    return pd.DataFrame(json_data_cleaned)

def main():
    json_file = "../data/data.json"
    
    csv_df = read_csv(csv_file)
    json_data = load_json(json_file)
    
    merged_df = merge_data(csv_df, json_data)
    
    # Guardar el resultado en un nuevo archivo JSON
    merged_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False)
    

if __name__ == "__main__":
    main()
