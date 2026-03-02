from typing import Optional
from datetime import date

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select


def get_value(driver, element_id: str) -> str:
    try:
        el = driver.find_element(By.ID, element_id)
        value = str(el.get_attribute("value"))
        value = value.strip()
    except Exception as e:
        print(f"Exception occured when getting value from element id {element_id}. Asigned empty string. Exception: {e}")
        value = ""

    return value


def get_select_text(driver, element_id: str) -> str:
    try:
        element = driver.find_element(By.ID, element_id)
        value = str(Select(element).first_selected_option.text)
        value = value.strip()
    except Exception as e:
        print(f"Exception occured when getting value from element id {element_id}. Asigned empty string. Exception: {e}")
        value = ""

    return value


def safe_int(value) -> Optional[int]:
    if  value is None:
        return None
    
    if isinstance(value, str):
        cleaned_value = value.replace(".", "").replace(",", ".")
        try:
            return int(cleaned_value)
        except Exception as e:
            print(f"Exception occured when converting {value} to int. Asigned None. Exception: {e}")
            return 
    
    if isinstance(value, float | int):
        return int(value)
    
    print(f"{value} was not converted to int because it is not None | str | int | float. Returning None")
    return None


def safe_float(value) -> Optional[float]:
    if  value is None:
        return None
    
    if isinstance(value, str):
        cleaned_value = value.replace(".", "").replace(",", ".")
        try:
            return float(cleaned_value)
        except Exception as e:
            print(f"Exception occured when converting {value} to float. Asigned None. Exception: {e}")
            return 
    
    if isinstance(value, float | int):
        return float(value)
    
    print(f"{value} was not converted to float because it is not None | str | int | float. Returning None")
    return None


def safe_date(value, sep: str = "/", day_first: bool = True) -> Optional[date]:
    date_str = str(value)
    date_list = date_str.split(sep)

    date_list = [safe_int(x) for x in date_list]
    date_list = [x for x in date_list if x is not None]

    if len(date_list) == 3:
        if day_first:
            day, month, year = date_list
        else:
            year, month, day = date_list
        
        return date(year, month, day)
    
    print(f"{value} was not converted to date. Returning None")
    return None