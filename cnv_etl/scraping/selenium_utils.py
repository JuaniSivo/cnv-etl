import time

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnv_etl.config import CLICK_DELAY

DEFAULT_TIMEOUT = 10


def wait_clickable(driver, by, value, timeout=DEFAULT_TIMEOUT):
    """Wait until an element is clickable, then pause briefly before returning."""
    element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, value))
    )
    time.sleep(CLICK_DELAY)
    return element


def wait_present(driver, by, value, timeout=DEFAULT_TIMEOUT):
    """Wait until an element is present in the DOM, then pause briefly before returning."""
    element = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )
    time.sleep(CLICK_DELAY)
    return element