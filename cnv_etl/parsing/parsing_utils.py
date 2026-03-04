from typing import List

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException

from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


def get_value(driver, elements_id: List[str] | str) -> str:
    if isinstance(elements_id, str):
        elements_id = [elements_id]

    value = ""
    for element_id in elements_id:
        try:
            el = driver.find_element(By.ID, element_id)
            value = str(el.get_attribute("value")).strip()
        except NoSuchElementException:
            logger.debug(f"No element with id '{element_id}'. Trying next.")
        except Exception as e:
            logger.warning(f"Could not get value from element '{element_id}': {e}")
            value = ""

        if value:
            return value

    return value


def get_select_text(driver, element_id: str) -> str:
    try:
        element = driver.find_element(By.ID, element_id)
        value = str(Select(element).first_selected_option.text).strip()
    except Exception as e:
        logger.warning(f"Could not get select text from element '{element_id}': {e}")
        value = ""

    return value