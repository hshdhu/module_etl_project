import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException

download_folder = "D:/Year 3/IT4930/project ETL/extract_data/data_fao/data"
os.makedirs(download_folder, exist_ok=True)
url = "https://www.fao.org/fishery/statistics-query/en/aquaculture/aquaculture_quantity"
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_folder.replace("/", "\\"),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("--window-size=1920,1080")
#chrome_options.add_argument("--headless")  

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 30)

def scroll_and_click(by, value, retries=3):  
    for i in range(retries):
        try:
            element = wait.until(EC.element_to_be_clickable((by, value)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            element.click()
            return
        except ElementClickInterceptedException:
            print(f"Click bị chặn, thử lại lần {i+1}")
            time.sleep(1)
    

# Truy cập trang và thực hiện thao tác tải
try:
    driver.get(url)
    time.sleep(5)  
    print("Click nút Download đầu tiên")
    scroll_and_click(By.XPATH, "//a[.//span[text()='Download']]")

    print("Chọn 'Yes' cho Include Null Values")
    scroll_and_click(By.XPATH, "//label[contains(., 'Yes')]")

    print("Click nút Download cuối cùng")
    buttons = driver.find_elements(By.XPATH, "//a[.//span[text()='Download']]")
    buttons[-1].click()  # click nút cuối
    time.sleep(5)  
    
    timeout = 60
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not any(f.endswith(".crdownload") for f in os.listdir(download_folder)):
            break
        time.sleep(1)
    print("Tải xuống hoàn tất.")

except TimeoutException:
    print("Quá thời gian chờ.")
except Exception as e:
    print(f"Lỗi không mong muốn: {e}")
finally:
    driver.quit()
    
