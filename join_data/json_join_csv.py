import pandas as pd
import json
import re

def read_csv(csv_file):
    # Leer el archivo CSV en un DataFrame
    return pd.read_csv(csv_file)

def load_json(json_file):
    with open(json_file) as file:
        return json.load(file)

def clean_price_and_size(value):
    # Utilizar una expresión regular para eliminar el símbolo € y m²
    cleaned_value = re.sub(r'[€m²]', '', value)
    cleaned_value = re.sub(r'\s+', '', cleaned_value)  # Eliminar espacios en blanco
    return cleaned_value

def clean_district_name(name):
    return name.strip().replace(' - ', '-')

def merge_data(csv_df, json_data):
    json_data_cleaned = []
    
    for item in json_data:
        address = item['Address']
        distrito_parts = clean_district_name(address['Distrito']).split(',')
        nom_carrer = address['Calle']
        matching_row = csv_df[csv_df['Nom_Districte'].str.strip() == distrito_parts[0]]

        if not matching_row.empty:
            price = clean_price_and_size(item['price'])
            size = clean_price_and_size(item['size'])
            merged_data = {
                'Codi_Districte': distrito_parts[1],
                'Nom_Districte': distrito_parts[0],
                'Nom_Carrer': nom_carrer,
                'title': item['title'],
                'size': size,
                'price': price,
                'Vigilancia': item['Vigilancia'],
                'Columnas': item['Columnas'],
                'Tipo plaza': item['Tipo plaza'],
                'Puerta': item['Puerta'],
                'Planta': item['Planta'],
                'Ciudad': address['Ciudad'],                
                'Personas': matching_row['Personas'].values[0],
                'Promedio_Euros': matching_row['Promedio_Import_Euros'].values[0],
                'Vehicles': matching_row['Vehicles'].values[0],
                'Atur': matching_row['Atur'].values[0],
                "€_m2": matching_row["€/m2"].values[0],
                "Denuncias_Convivencia": matching_row["Denuncias_Convivencia"].values[0],
                "Incidents_Degradacion": matching_row["Incidents_Degradacion"].values[0],
                "Promedio_Vivienda": matching_row["Promedio_Vivienda"].values[0],
                "Habitatges turístics": matching_row["Habitatges_dus_turístic"].values[0],
                "Hotels": matching_row["Hotel"].values[0],
                "Residències estudiants": matching_row["Residències estudiants"].values[0],
                "Parcs urbans": matching_row["Parcs urbans"].values[0],
                "Arbres al carrer": matching_row["Arbres al carrer"].values[0],
                "Turismes": matching_row["Turismes"].values[0],
                "Superfície (km²)": matching_row["Superfície (km²)"].values[0],
                "Habitants/Turisme": matching_row["Habitants/Turisme"].values[0],
                "Turismes/km²": matching_row["Turismes/km²"].values[0],
                "Total Guals": matching_row["Total Guals"].values[0],
                "A peu + bicicleta a treballar": matching_row["A peu + bicicleta"].values[0],
                "Transport privat a treballar": matching_row["Transport privat"].values[0],
                "Transport públic a treballar": matching_row["Transport públic"].values[0],
                "Nacionalitats espanyoles": matching_row["Nacionalitat espanyola"].values[0],
                "Nacionalitats estrangeres": matching_row["Nacionalitat estrangera"].values[0],
                "Població Total": matching_row["PoblacióTotal"].values[0],
                "Densitat de població (hab/km²)": matching_row["Densitat de població (hab/km²)"].values[0],
                "Edat mitjana": matching_row["Edat mitjana"].values[0],
                "Domicilis": matching_row["Domicilis"].values[0],
                "Superficie Residencial": matching_row["Residencial"].values[0],
                "Superficie Equipaments": matching_row["Equipaments"].values[0],
                "Superficie Indústria i infraestructures": matching_row["Indústria i infraestructures"].values[0],
                "Superficie Xarxa viària": matching_row["Xarxa viària"].values[0],
                "Superficie Parcs forestals": matching_row["Parcs forestals"].values[0],
                "Aparcaments Total": matching_row["Nombre de places aparcament Total"].values[0],
                "Aparcaments Residencies": matching_row["Nombre de places aparcament Residencies"].values[0],
                "Aparcament Altres edificis": matching_row["Nombre de places aparcament Altres edificis"].values[0],
                "Centres Civics": matching_row["Nombre de Centres Civics"].values[0],



            }
            json_data_cleaned.append(merged_data)
    
    return pd.DataFrame(json_data_cleaned)

def main():
    csv_file = "../csv_data/combined_df.csv"
    json_file = "../data/data.json"

    json_data = load_json(json_file)
    csv_df = read_csv(csv_file)

    merged_df = merge_data(csv_df, json_data)
    
    # Guardar el resultado en un nuevo archivo JSON
    merged_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False, date_format='iso')
    merged_df.to_csv('../data/resultado_join.csv')    


if __name__ == "__main__":
    main()
