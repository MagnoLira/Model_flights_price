from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import tempfile



class Browser_skiplagged:
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
        self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="FlightSearchResults.TripRow"]')))

    
    def get_flights_info_skipplagged(self):
        self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="FlightSearchResults.TripRow"]')))
        voos = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="FlightSearchResults.TripRow"]')

        voos_raw = []

        for voo in voos:
            try:
                try:
                        companhia = voo.find_element(By.CLASS_NAME, "airlines").text
                except: companhia = None
                try:
                        preco = voo.find_element(By.CLASS_NAME, "trip-cost-new").text
                except: preco = None
                try:
                        hora_saida = voo.find_element(By.CSS_SELECTOR, ".trip-path-point-first .trip-path-point-time").text
                except: hora_saida = None
                try:
                        hora_chegada = voo.find_element(By.CSS_SELECTOR, ".trip-path-point-last .trip-path-point-time").text
                except: hora_chegada = None

                voos_raw.append({
                        "raw_text": voo.text.strip(),
                        "companhia_bruta": companhia,
                        "preco_bruto": preco,
                        "hora_saida_bruta": hora_saida,
                        "hora_chegada_bruta": hora_chegada,
                        "data_busca": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "link_emissao": self.url,
                        "site": "skiplagged"
                    })

            except Exception as e:
                print(f"Erro ao capturar um card: {e}")

        return voos_raw
    def quit(self):
        self.driver.quit()
