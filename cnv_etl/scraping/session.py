"""
Selenium WebDriver session utilities.

Provides a factory function and a context manager to create and configure
Chrome WebDriver instances in a consistent way across the project.

Usage
-----
# Plain driver (caller is responsible for quit)
driver = create_driver()

# Context manager (quit is guaranteed)
with driver_session() as driver:
    driver.get("https://...")
"""

from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver


def create_driver(headless: bool) -> WebDriver:
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


@contextmanager
def driver_session(headless: bool = True):
    """
    Context manager that creates a Chrome WebDriver and guarantees
    driver.quit() is called on exit, even if an exception is raised.

    Parameters
    ----------
    headless : bool
        Whether to run Chrome in headless mode.

    Yields
    ------
    WebDriver
        Configured Chrome WebDriver instance.

    Example
    -------
    with driver_session() as driver:
        driver.get("https://...")
    """
    driver = create_driver(headless=headless)
    try:
        yield driver
    finally:
        driver.quit()