"""
Selenium WebDriver session utilities.

Provides a single factory function to create and configure Chrome
WebDriver instances in a consistent way across the project.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver


def create_driver(headless: bool = False) -> WebDriver:
    """
    Create and configure a Selenium Chrome WebDriver.

    Parameters
    ----------
    headless : bool
        Whether to run Chrome in headless mode.

    Returns
    -------
    WebDriver
        Configured Chrome WebDriver instance.
    """
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    return webdriver.Chrome(options=options)