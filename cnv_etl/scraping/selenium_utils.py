import time

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DEFAULT_TIMEOUT = 10

def wait_clickable(driver, by, value, timeout=DEFAULT_TIMEOUT):
    web_element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, value))
    )

    time.sleep(0.1)
    return web_element

def wait_present(driver, by, value, timeout=DEFAULT_TIMEOUT):
    web_element = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )
    
    time.sleep(0.1)
    return web_element