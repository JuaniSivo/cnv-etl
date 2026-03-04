import time
from typing import List

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnv_etl.models.document import RawFinancialStatement, RawConceptValue
from cnv_etl.config import PAGE_DELAY
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


class StatementValuesParser:

    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        wait = WebDriverWait(driver, 10)

        try:
            pagination = driver.find_element(By.XPATH, '//ul[@class="pagination" and @entidad="10013"]')
        except Exception as e:
            logger.warning(
                f"Could not find pagination for statement '{fs.document_description}'. "
                f"Returning unchanged. {type(e).__name__}: {e}"
            )
            return fs

        start_page = int(pagination.get_attribute("data-paginainicio"))
        end_page   = int(pagination.get_attribute("data-paginafin"))

        concepts: List[RawConceptValue] = []

        for i in range(start_page, end_page + 1):
            if i != 1:
                enlace = driver.find_element(
                    By.XPATH,
                    f'//ul[@entidad="10013"]/li/a[text()="{i}"]'
                )
                driver.execute_script("arguments[0].click();", enlace)

            wait.until(EC.presence_of_element_located(
                (By.XPATH, f'//ul[@entidad="10013"]/li[@class="active"]/a[text()="{i}"]')
            ))
            wait.until(EC.presence_of_all_elements_located(
                (By.XPATH, '//table[@entidad="10013"]/tbody/tr')
            ))
            wait.until(lambda d: any(
                r.find_elements(By.TAG_NAME, "td")[0].text.strip() != ""
                for r in d.find_elements(By.XPATH, '//table[@entidad="10013"]/tbody/tr')
            ))

            time.sleep(PAGE_DELAY)

            rows = driver.find_elements(By.XPATH, '//table[@entidad="10013"]/tbody/tr')
            logger.debug(f"Page {i}/{end_page}: found {len(rows)} rows")

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                concepts.append(RawConceptValue(
                    label=str(cols[1].text),
                    value=str(cols[2].text),
                    id=str(cols[0].text)
                ))

        return fs.transform(statement=concepts)