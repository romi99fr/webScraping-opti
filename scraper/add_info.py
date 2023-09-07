import time
import json
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def configure_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def load_data_from_json(filename):
    with open(filename, mode='r', encoding='utf-8') as file:
        return json.load(file)

def extract_info_from_description(product_data, desc):
    # Patrones para buscar información en la descripción
    pattern_street = r"(?i)(?<!\w)(Garaje en(?: calle)?)(?:\s+(?:de\s+)?(?:\s+([^,]+))?"
    pattern_door = r"(?i)(?<!\w)(?:[.,])?\s*(?:puerta(?:\s+a\s+distancia)?|mando|puerta\s+de\s+acceso\s+es\s+automática|motor|motorizada)(?:\s+(?:basculante|de acceso|mecanizada|automática|a distancia))?\b"
    pattern_floor = r"(?i)\b(?:planta\s*-?\s*(?:primera|segunda|tercera|cuarta|quinta|\d+)|primera\s*planta|segunda\s*planta|tercera\s*planta|cuarta\s*planta|quinta\s*planta|semisótano|sotano|sótano\s*-?\s*\d+|sótano\s*uno|sótano\s*dos|sótano|única\s*planta\s*sótano)\b"
    pattern_coche = r"(?i)\bcoche\s*(pequeño|mediano|grande)\b"
    pattern_columnas = r"(?i)\b(?:columnas\s*laterales|entre\s*columnas|dos\s*columnas\s*laterales|dos\s*columnas|tres\s*columnas|protegidas\s*por\s*columnas|protegidas\s*entre\s*columnas|pocas\s*columnas|columna)\b"
    pattern_no_columnas = r"(?i)\b(?:no\s*tiene\s*columnas|sin\s*columnas|sin\s*columna)\b"
    pattern_ascensor = r"(?i)\bascensores?\b"
    pattern_sin_ascensor = r"(?i)\bsin\s+ascensor\b"
    pattern_vigilancia = r"(?i)\b(vigilante|vigilancia|vigilado|videovigilado|videovigilancia)\b"

    # Buscar todas las coincidencias de vigilancia
    matches_vigilancia = re.findall(pattern_vigilancia, desc)
    if matches_vigilancia:
        product_data["Vigilancia"] = "Si"
    else:
        product_data["Vigilancia"] = "No"

    # Buscar si hay ascensor
    matches_ascensor = re.findall(pattern_ascensor, desc)
    matches_noascensor = re.findall(pattern_sin_ascensor, desc)
    atributo_ascensor = "Si" if matches_ascensor else "No" if matches_noascensor else "None"
    if matches_ascensor:
        product_data["Ascensor"] = atributo_ascensor

    # Buscar si hay o no columnas
    matches = re.findall(pattern_columnas, desc)
    matches_no_columnas = re.findall(pattern_no_columnas, desc)
    atributo_columnas = "Si" if matches else "No" if matches_no_columnas else "None"
    if atributo_columnas:
        product_data["Columnas"] = atributo_columnas   

    # Buscar el tamaño del coche
    size_car = re.findall(pattern_coche, desc)
    size_low = [tamano.lower() for tamano in size_car]
    if size_low:
        size = size_low[0]
        product_data["Tipo plaza"] = size
    else:
        product_data["Tipo plaza"] = "None"

    # Buscar y capturar el tipo de puerta
    description = re.findall(pattern_door, desc)
    if description:
        door = description[0]
        product_data["Puerta"] = "automática"
    else:
        product_data["Puerta"] = "None"

    # Buscar y capturar la planta
    floor = re.findall(pattern_floor, desc)
    if floor:
        floor_info = floor[0]
        product_data["Planta"] = floor_info
    else:
        product_data["Planta"] = "None"

    # Buscar coincidencias de la calle en el título
    matches_street = re.findall(pattern_street, product_data["title"])
    if matches_street:
        street_name = matches_street[0][1]
    else:
        street_name = "None"
    return street_name

def main():
    driver = configure_webdriver()
    all_product_data = []
    distritos_barcelona = {
        "Ciutat Vella": 1,
        "Eixample": 2,
        "Sants - Montjuïc": 3,
        "Les Corts": 4,
        "Sarrià-Sant Gervasi": 5,
        "Gràcia": 6,
        "Horta - Guinardó": 7,
        "Nou Barris": 8,
        "Sant Andreu": 9,
        "Sant Martí": 10
    }
    filename = "./data/data.json"
    data = load_data_from_json(filename)

    for product_data in data:
        product_link = product_data['link']
        driver.get(product_link)
        time.sleep(3)  # Esperar para que se cargue la página completa

        try:
            address_element = driver.find_element(By.CSS_SELECTOR, 'div.details-address.icon-placeholder-1.mb-lg.pointer')
            address = address_element.text.strip()
        except Exception as e:
            address = ""

        try:
            desc_element = driver.find_element(By.CSS_SELECTOR, 'div.raw-format.description.readMoreText')
            desc = desc_element.text.strip()
            desc = desc.replace('\n', '')
        except Exception as e:
            desc = ""

        components = address.split(',')
        if len(components) >= 3:
            barri = components[0]
            districte = components[1]
            ciutat = components[2]

        # Clean up the 'districte' variable and convert it to lowercase for case-insensitive matching
        districte_cleaned = districte.replace("-", "").replace(" ", "").strip().lower()

        # Clean up the keys in the distritos_barcelona dictionary to match the cleaned and lowercase version
        distritos_barcelona_cleaned = {k.replace("-", "").replace(" ", "").lower(): v for k, v in distritos_barcelona.items()}

        # Obtener el número de distrito correspondiente al nombre del distrito (case-insensitive)
        districte_num = distritos_barcelona_cleaned.get(districte_cleaned, "None")


        # Agregar barri, districte y ciutat dentro de la variable 'address'
        address = {
            "Calle": extract_info_from_description(product_data, desc),
            "Barrio": barri,
            "Distrito": f"{districte}, {districte_num}",
            "Ciudad": ciutat
        }

        # Actualizar la dirección en el producto_data
        product_data['Address'] = address
        product_data.pop('link', None)

        # Agregar los datos de los productos de la página actual a la lista general
        all_product_data.append(product_data)
        
    driver.quit()

    with open(filename, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False)

    print(f"Los datos se han actualizado correctamente en el archivo {filename}.")

if __name__ == "__main__":
    main()
