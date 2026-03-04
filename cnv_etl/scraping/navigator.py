"""
Selenium-based navigation layer for the CNV website.

This module is responsible for:
- URL construction
- Page navigation
- Menu interactions

It returns raw Selenium WebElements and does not perform any parsing or
domain logic.
"""

import time
from datetime import date

from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from cnv_etl.config import BASE_URL, XPATHS
from cnv_etl.scraping.selenium_utils import wait_clickable, wait_present


class CNVNavigator:
    """
    Selenium-based navigator for CNV web pages.

    Responsibilities:
    - Build CNV URLs
    - Open pages
    - Click menus
    - Locate and return raw WebElements

    This class does not parse data and does not
    interact with domain objects.
    """

    def __init__(self, driver: WebDriver):
        self.driver = driver

    def get_documents_url(
        self,
        company_id: str,
        date_from: date,
        date_to: date
    ) -> str:
        company   = f"Empresa/{company_id}?"
        formtype  = "formType=INFOFI&"
        from_str  = f"fdesde={date_from.strftime('%d/%m/%Y')}&"
        to_str    = f"fhasta={date_to.strftime('%d/%m/%Y')}"

        return BASE_URL + company + formtype + from_str + to_str

    def open_documents_table(
        self,
        company_id: str,
        date_from: date,
        date_to: date
    ) -> tuple[list[WebElement], list[WebElement]]:

        url = self.get_documents_url(company_id, date_from, date_to)

        self.driver.get(url)
        wait_present(self.driver, By.TAG_NAME, "body")

        wait_clickable(self.driver, By.XPATH, XPATHS["financial_info_menu"]).click()
        wait_clickable(self.driver, By.XPATH, XPATHS["financial_states_menu"]).click()

        panel = wait_present(self.driver, By.XPATH, XPATHS["panel_heading"])

        table = wait_present(
            panel,
            By.XPATH,
            'ancestor::div[contains(@id,"heading")]/following-sibling::div//table[@class="table tabla-hechos-relevantes"]'
        )

        header = table.find_elements(By.CSS_SELECTOR, "thead th")
        rows   = table.find_elements(By.TAG_NAME, "tr")

        return header, rows

    def open_statement(self, link: str) -> None:
        """
        Open a financial statement page and ensure the main content
        is interactable (CNV modal released).
        """
        self.driver.get(link)

        MAX_RETRIES = 200
        RETRY_DELAY = 0.1

        for _ in range(MAX_RETRIES):
            try:
                wait_clickable(
                    self.driver, By.XPATH, XPATHS["statement_main_tab"]
                ).click()
                return
            except ElementClickInterceptedException:
                time.sleep(RETRY_DELAY)

        raise TimeoutError("CNV modal never released the click")

    def open_statement_metadata_tab(self) -> None:
        wait_clickable(self.driver, By.XPATH, XPATHS["statement_metadata_tab"]).click()

    def open_company_metadata_tab(self) -> None:
        wait_clickable(self.driver, By.XPATH, XPATHS["company_metadata_tab"]).click()

    def open_statement_values_tab(self) -> None:
        wait_clickable(self.driver, By.XPATH, XPATHS["statement_main_tab"]).click()