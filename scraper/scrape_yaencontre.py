import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def configure_and_open_browser():
    # Configurar Selenium y abrir el navegador
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def scrape_product_data(driver, total_pages):
    visited_products = set()
    all_product_data = []

    for page in range(1, total_pages + 1):
        page_url = f"https://www.yaencontre.com/venta/garajes/barcelona/pag-{page}"
        driver.get(page_url)
        time.sleep(6)

        scroll_pause_time = 1
        screen_height = driver.execute_script("return window.innerHeight")
        i = 1

        while True:
            driver.execute_script(f"window.scrollTo(0, {screen_height * i});")
            i += 5
            time.sleep(scroll_pause_time)
            html_content = driver.page_source
            product_containers = driver.find_elements(By.CSS_SELECTOR, 'div.content')

            product_data = []
            for container in product_containers:
                product_link = container.find_element(By.CSS_SELECTOR, 'a.d-ellipsis').get_attribute('href')
                if product_link in visited_products:
                    continue

                visited_products.add(product_link)
                title = container.find_element(By.CSS_SELECTOR, 'a.d-ellipsis').text.strip()
                size = container.find_element(By.CSS_SELECTOR, 'div.iconGroup').text.strip()
                price = container.find_element(By.CSS_SELECTOR, 'span.price').text.strip()

                product_data.append({
                    'title': title,
                    'size': size,
                    'price': price,
                    'link': product_link
                })

            all_product_data.extend(product_data)
            if i >= 25:
                break

    return all_product_data

def save_data_to_json(data, filename):
    with open(filename, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False)

def main():
    total_pages = 62
    driver = configure_and_open_browser()
    product_data = scrape_product_data(driver, total_pages)
    filename = "./data/data.json"
    save_data_to_json(product_data, filename)
    print(f"Los datos se han guardado correctamente en el archivo {filename}.")
    driver.quit()

if __name__ == "__main__":
    main()
