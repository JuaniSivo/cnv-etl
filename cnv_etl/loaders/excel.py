from pathlib import Path
from datetime import date, datetime
from typing import Any, Optional, Dict

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from cnv_etl.models.document import CleanFinancialStatement
from cnv_etl.models.company import Company
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


class ExcelExporter:
    """Export financial statements to Excel format."""

    HEADER_FILL = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    HEADER_FONT = Font(bold=True, size=11)
    BORDER = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    def __init__(self):
        self.wb = Workbook()

    def export_company(
        self,
        company: Company,
        output_path: Path
    ) -> Path:

        if not company.statements:
            raise ValueError("Cannot export empty statements list")

        if output_path.suffix != '.xlsx':
            output_path = output_path.with_suffix('.xlsx')

        self.wb.remove(self.wb.active)

        self._write_company_metadata_sheet(company)
        self._write_statement_metadata_sheet(company)
        self._write_statement_values_sheet(company.statements)

        self.wb.save(output_path)
        logger.info(f"Exported {len(company.statements)} statements to {output_path}")

        return output_path

    def _write_company_metadata_sheet(self, company: Company) -> None:
        ws = self.wb.create_sheet("company_metadata")

        rows = [
            ("id", company.id),
            ("name", company.name),
            ("ticker", company.ticker),
            ("description", company.description),
            ("sector", company.sector),
            ("industry_group", company.industry_group),
            ("industry", company.industry),
            ("sub_industry", company.sub_industry),
            ("sub_industry_description", company.sub_industry_description)
        ]

        for row_idx, row_data in enumerate(rows, start=1):
            self._write_data_row(ws, row_idx, row_data)

    def _write_statement_metadata_sheet(self, company: Company) -> None:
        ws = self.wb.create_sheet("statement_metadata")

        columns = [
            ("document_id", 15),
            ("document_description", 100),
            ("submission_date", 20),
            ("period_end_date", 15),
            ("reporting_period", 15),
            ("financial_statements_type", 25),
            ("currency", 15),
            ("accounting_standards", 25),
            ("merger_or_demerger_in_process", 20),
            ("treasury_shares", 20),
            ("insolvency_proceedings", 20),
            ("public_offering_of_equity_instruments", 20),
            ("subscribed_shares", 20),
            ("share_price", 15),
            ("free_float_percentage", 15),
            ("number_of_employees", 20)
        ]

        headers = [col[0] for col in columns]
        widths  = [col[1] for col in columns]

        self._write_header_row(ws, headers, widths)

        for row_idx, stmt in enumerate(company.statements.values(), start=2):
            row_data = [
                stmt.document_id,
                stmt.document_description,
                stmt.submission_date,
                stmt.period_end_date,
                stmt.reporting_period,
                stmt.financial_statements_type,
                stmt.presentation_currency,
                stmt.accounting_standards_applied,
                stmt.merger_or_demerger_in_process,
                stmt.treasury_shares,
                stmt.insolvency_proceedings,
                stmt.public_offering_of_equity_instruments,
                stmt.subscribed_shares_at_period_end,
                stmt.equity_share_price_at_period_end,
                stmt.free_float_percentage,
                stmt.number_of_employees
            ]
            self._write_data_row(ws, row_idx, row_data)

        ws.freeze_panes = 'A2'

    def _write_statement_values_sheet(
        self,
        statements: Dict[int, CleanFinancialStatement]
    ) -> None:
        ws = self.wb.create_sheet("statement_values")

        columns = [
            ("document_id", 15),
            ("order", 12),
            ("concept", 40),
            ("value", 20)
        ]

        headers = [col[0] for col in columns]
        widths  = [col[1] for col in columns]

        self._write_header_row(ws, headers, widths)

        row_idx = 2
        for stmt in statements.values():
            for line in stmt.statement.values():
                self._write_data_row(ws, row_idx, [
                    stmt.document_id,
                    line.order,
                    line.label,
                    line.value
                ])
                row_idx += 1

        ws.freeze_panes = 'A2'

    def _write_header_row(self, ws, headers: list[str], widths: list[int]) -> None:
        for col_idx, (header, width) in enumerate(zip(headers, widths), start=1):
            cell = ws.cell(row=1, column=col_idx)
            cell.value = header
            cell.font = self.HEADER_FONT
            cell.fill = self.HEADER_FILL
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.BORDER
            ws.column_dimensions[get_column_letter(col_idx)].width = width

    def _write_data_row(self, ws, row_idx: int, data: list[Any]) -> None:
        for col_idx, value in enumerate(data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)

            if isinstance(value, datetime):
                cell.value = value
                cell.number_format = 'YYYY-MM-DD HH:MM'
            elif isinstance(value, date):
                cell.value = value
                cell.number_format = 'YYYY-MM-DD'
            elif isinstance(value, int):
                cell.value = value
                cell.number_format = '#,##0'
            elif isinstance(value, float):
                cell.value = value
                cell.number_format = '#,##0.00'
            else:
                cell.value = value

            cell.border = self.BORDER


def export_company_to_excel(company: Company, output_path: Path) -> Path:
    """Convenience wrapper around ExcelExporter."""
    exporter = ExcelExporter()
    return exporter.export_company(company, output_path)