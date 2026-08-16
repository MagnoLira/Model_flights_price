from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import tempfile



class Browser_latam:
    def __init__(self, url: str):
        options = Options()

        user_data_dir = tempfile.mkdtemp()
        options.add_argument(f"--user-data-dir={user_data_dir}")

        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

        self.driver = webdriver.Chrome(options=options)
        self.url = url
        self.wait = WebDriverWait(self.driver, 30)

    def load_page(self):
        self.driver.get(self.url)

        self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        self.accept_cookies()
        self.remove_overlay()

    def accept_cookies(self):
        try:
            aceitar_cookies = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div/div/div/div/section/div/button[1]"))
            )
            aceitar_cookies.click()
            print("Cookies aceitos.")
        except Exception:
            print("Botão de cookies não encontrado ou já aceito.")

    def remove_overlay(self):
        ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
        print("Overlay fechado com ESC.")

    def get_flight_info_latam(self):
        voos_raw = []
        index = 1
        while True:
            try:
                xpath = f"//ol/li[{index}]//span[contains(@class, 'sc-hmdomO') and contains(@class, 'etZFYD')]"
                span_element = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                texto = span_element.text.upper()

                voos_raw.append({
                "raw_text": texto,
                "link_emissao": self.url,
                "site": "latam",
                "data_busca": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })

                print(f"[{index}] Voo adicionado.")

                index += 1

            except:
                print(f"Fim da lista de voos. Total encontrados: {index - 1}")
                break
        return voos_raw
    def quit(self):
        self.driver.quit()
