import time
from typing import List

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnv_etl.models.document import RawFinancialStatement, RawConceptValue


class StatementValuesParser:

    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        wait = WebDriverWait(driver, 10)

        # pagination container
        try:
            pagination = driver.find_element(By.XPATH, '//ul[@class="pagination" and @entidad="10013"]')
        except Exception as e:
            print(f"Exception occured when accessing the statements table at CNV webpage. Returning the original FinancialStatement instance without change")
            return fs

        start_page = int(pagination.get_attribute("data-paginainicio"))
        end_page   = int(pagination.get_attribute("data-paginafin"))

        concepts: List[RawConceptValue] = list()
        for i in range(start_page, end_page + 1):
            if i != 1:
                # click page
                enlace = driver.find_element(
                    By.XPATH,
                    f'//ul[@entidad="10013"]/li/a[text()="{i}"]'
                )
                driver.execute_script("arguments[0].click();", enlace)

            # wait until page becomes active
            wait.until(EC.presence_of_element_located(
                (By.XPATH, f'//ul[@entidad="10013"]/li[@class="active"]/a[text()="{i}"]')
            ))

            # wait table rows to be present
            wait.until(EC.presence_of_all_elements_located(
                (By.XPATH, '//table[@entidad="10013"]/tbody/tr')
            ))

            # wait until at least one row has non-empty first column
            wait.until(lambda d: any(
                r.find_elements(By.TAG_NAME, "td")[0].text.strip() != ""
                for r in d.find_elements(By.XPATH, '//table[@entidad="10013"]/tbody/tr')
            ))

            time.sleep(0.5)

            rows = driver.find_elements(By.XPATH, '//table[@entidad="10013"]/tbody/tr')

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")

                concept = RawConceptValue(
                    label=str(cols[1].text),
                    value=str(cols[2].text),
                    id=str(cols[0].text)
                )

                concepts.append(concept)

        parsed_fs = fs.transform(statement=concepts)

        return parsed_fs