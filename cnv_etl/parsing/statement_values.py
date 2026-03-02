"""
Parser for financial statement numerical values.

Extracts balance sheet and income statement values from paginated CNV
tables and returns a clean pandas DataFrame with numeric types.
"""

import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnv_scraper.domain.document import FinancialStatement, ConceptValue
from cnv_scraper.parsing.parsing_utils import safe_float, safe_int


class StatementValuesParser:
    """
    Parser for financial statement value tables.

    Extracts all paginated rows of balance data and returns a normalized
    DataFrame with numeric values.

    Handles Spanish numeric formats and pagination.
    """

    def parse(self, driver, fs: FinancialStatement) -> FinancialStatement:

        cnv_to_xbrl = {
            "EFECTIVO Y EQUIVALENTES A EFECTIVO": "CashAndCashEquivalents",
            "ACTIVOS FINANCIEROS CORRIENTES": "CurrentFinancialAssets",
            "CUENTAS POR COBRAR CORRIENTES": "CurrentTradeAndOtherReceivables",
            "OTROS ACTIVOS NO FINANCIEROS CORRIENTES": "OtherCurrentNonFinancialAssets",
            "INVENTARIOS CORRIENTES": "Inventories",
            "ACTIVO CORRIENTE": "CurrentAssets",
            "ACTIVOS FINANCIEROS NO CORRIENTES": "NonCurrentFinancialAssets",
            "CUENTAS POR COBRAR NO CORRIENTES": "NonCurrentTradeAndOtherReceivables",
            "PROPIEDADES PLANTAS Y EQUIPOS": "PropertyPlantAndEquipment",
            "OTROS ACTIVOS NO FINANCIEROS NO CORRIENTES": "OtherNonCurrentNonFinancialAssets",
            "ACTIVOS INTANGIBLES": "IntangibleAssets",
            "ACTIVO NO CORRIENTE": "NonCurrentAssets",
            "TOTAL DEL ACTIVO": "Assets",
            "CAPITAL": "IssuedCapital",
            "AJUSTE DE CAPITAL": "CapitalAdjustment",
            "APORTES NO CAPITALIZADOS O CONTRIBUCIONES DE CAPITAL": "ContributionsReceivedForFutureShareIssues",
            "OTROS CONCEPTOS DEL CAPITAL": "OtherEquityInterests",
            "RESERVA LEGAL": "LegalReserve",
            "OTRAS RESERVAS": "OtherReserves",
            "GANANCIAS RESERVADAS": "RetainedEarnings",
            "INTERESES NO CONTROLANTES": "NonControllingInterests",
            "RESULTADOS NO ASIGNADOS": "AccumulatedResults",
            "RESULTADOS INTEGRALES": "ComprehensiveIncome",
            "TOTAL PATRIMONIO NETO": "Equity",
            "PASIVOS FINANCIEROS CORRIENTES": "CurrentFinancialLiabilities",
            "CUENTAS POR PAGAR CORRIENTES": "CurrentTradeAndOtherPayables",
            "PASIVOS POR IMPUESTOS CORRIENTES": "CurrentTaxLiabilities",
            "OTROS PASIVOS NO FINANCIEROS CORRIENTES": "OtherCurrentNonFinancialLiabilities",
            "PASIVO CORRIENTE": "CurrentLiabilities",
            "PASIVOS FINANCIEROS NO CORRIENTES": "NonCurrentFinancialLiabilities",
            "CUENTAS POR PAGAR NO CORRIENTES": "NonCurrentTradeAndOtherPayables",
            "PASIVOS POR IMPUESTOS NO CORRIENTES": "NonCurrentTaxLiabilities",
            "OTROS PASIVOS NO FINANCIEROS NO CORRIENTES": "OtherNonCurrentNonFinancialLiabilities",
            "PASIVO NO CORRIENTE": "NonCurrentLiabilities",
            "TOTAL DEL PASIVO": "Liabilities",
            "TOTAL DEL PASIVO Y PATRIMONIO NETO": "LiabilitiesAndEquity",
            "INGRESOS DE ACTIVIDADES ORDINARIAS": "Revenue",
            "COSTO DE VENTAS Y/O SERVICIOS": "CostOfSales",
            "GANANCIA (PERDIDA) BRUTA": "GrossProfit",
            "HONORARIOS A DIRECTORES Y SINDICOS": "DirectorsAndStatutoryAuditorsFees",
            "OTROS GASTOS DE ADMINISTRACION": "AdministrativeExpenses",
            "GASTOS DE COMERCIALIZACION Y/O DISTRIBUCION": "SellingAndDistributionExpenses",
            "OTROS GASTOS OPERATIVOS": "OtherOperatingExpenses",
            "OTROS INGRESOS": "OtherIncome",
            "DEPRECIACIONES Y AMORTIZACIONES": "DepreciationAndAmortisation",
            "GANANCIA (PERDIDA) DE ACTIVIDADES OPERATIVAS": "OperatingProfitLoss",
            "INGRESOS FINANCIEROS": "FinanceIncome",
            "COSTOS FINANCIEROS": "FinanceCosts",
            "RECPAM": "MonetaryPositionGainLoss",
            "OTROS RESULTADOS": "OtherGainsLosses",
            "GANANCIA (PERDIDA) ANTES DE IMPUESTOS": "ProfitLossBeforeTax",
            "INGRESO (GASTO) POR IMPUESTOS A LAS GANANCIAS": "IncomeTaxExpense",
            "GANANCIA (PERDIDA) DEL PERIODO / EJERCICIO OPERACIONES CONTINUADAS": "ProfitLossFromContinuingOperations",
            "GANANCIA (PERDIDA) DEL PERIODO / EJERCICIO OPERACIONES DISCONTINUADAS": "ProfitLossFromDiscontinuedOperations",
            "GANANCIA (PERDIDA) DEL PERIODO / EJERCICIO": "ProfitLoss",
            "OTRO RESULTADO INTEGRAL DEL EJERCICIO / PERIODO": "OtherComprehensiveIncome",
            "RESULTADO INTEGRAL TOTAL DEL EJERCICIO / PERIODO": "TotalComprehensiveIncome",
            "TOTAL CAMBIOS EN ACTIVOS Y PASIVOS OPERATIVOS": "CashFlowsFromOperatingActivities",
            "TOTAL DE ACTIVIDADES DE INVERSION": "CashFlowsFromInvestingActivities",
            "TOTAL DE ACTIVIDADES DE FINANCIACION": "CashFlowsFromFinancingActivities",
            "INCREMENTO (DISMINUCION) NETA DE EFECTIVO Y EQUIVALENTES": "IncreaseDecreaseInCashAndCashEquivalents",
            "GANANCIA (PERDIDA) BASICA POR ACCION": "BasicEarningsLossPerShare",
            "GANANCIAS (PERDIDA) DILUIDA POR ACCION": "DilutedEarningsLossPerShare",
            "TOTAL PATRIMONIO NETO DEL EJERCICIO ANTERIOR": "EquityPreviousPeriod",
            "EBIT": "EBIT",
            "EBITDA": "EBITDA",
            "CAPITAL DE TRABAJO": "WorkingCapital",
            "LIQUIDEZ": "CurrentRatio",
            "SOLVENCIA": "SolvencyRatio",
            "INMOVILIZACION DEL CAPITAL": "CapitalEmployed",
            "RENTABILIDAD PATRIMONIO NETO": "ReturnOnEquity",
            "RENTABILIDAD DEL ACTIVO": "ReturnOnAssets",
            "ENDEUDAMIENTO": "DebtRatio",
            "ENDEUDAMIENTO A CORTO PLAZO": "ShortTermDebtRatio",
            "APALANCAMIENTO": "LeverageRatio",
            "MARGEN NETO / VENTAS": "NetProfitMargin",
            "DEUDA FINANCIERA / EBITDA": "NetDebtToEBITDA",
            "EBITDA / COSTOS FINANCIEROS": "EBITDAToFinanceCosts",
            "ANALISIS DU-PONT": "DuPontAnalysis",
            "PRUEBA ACIDA": "QuickRatio",
            "COBERTURA DE GASTOS FINANCIEROS": "InterestCoverageRatio",
            "ROTACION DE ACTIVOS": "AssetTurnover",
        }

        wait = WebDriverWait(driver, 10)

        # pagination container
        try:
            pagination = driver.find_element(By.XPATH, '//ul[@class="pagination" and @entidad="10013"]')
        except Exception as e:
            print(f"Exception occured when accessing the statements table at CNV webpage. Returning the original FinancialStatement instance without change")
            return fs

        start_page = int(pagination.get_attribute("data-paginainicio"))
        end_page   = int(pagination.get_attribute("data-paginafin"))

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

            time.sleep(1)

            rows = driver.find_elements(By.XPATH, '//table[@entidad="10013"]/tbody/tr')

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")

                concept_id = str(cols[0].text)
                cnv_label = str(cols[1].text)
                xbrl_label = cnv_to_xbrl.get(cnv_label)
                value = safe_float(cols[2].text)

                # Skip if we couldn't map to XBRL or parse value
                if xbrl_label is None:
                    print(f"Warning: Could not map CNV concept '{cnv_label}' (ID: {concept_id}) to XBRL")
                    continue

                if value is None:
                    continue

                # Convert concept_id to integer for ordering
                try:
                    display_order = int(concept_id)
                except (ValueError, TypeError):
                    display_order = 999999  # Put unmappable IDs at the end

                concept = ConceptValue(
                    xbrl_label=xbrl_label,
                    value=value,
                    display_order=display_order,
                    cnv_label=cnv_label
                )

                fs.add_concept(concept)

        return fs