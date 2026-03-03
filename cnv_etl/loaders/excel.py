from pathlib import Path
from datetime import date, datetime
from typing import Any, Optional, Dict

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from cnv_etl.models.document import CleanFinancialStatement
from cnv_etl.models.company import Company


class ExcelExporter:
    """Export financial statements to Excel format."""
    
    # Style definitions
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
        
        # Ensure .xlsx extension
        if output_path.suffix != '.xlsx':
            output_path = output_path.with_suffix('.xlsx')
        
        # Create workbook
        self.wb.remove(self.wb.active)  # Remove default sheet
        
        # Write sheets
        self._write_metadata_sheet(company)
        self._write_values_sheet(company.statements)
        
        # Save
        self.wb.save(output_path)
        print(f"✓ Exported {len(company.statements)} statements to {output_path}")
        
        return output_path
    
    def _write_metadata_sheet(
        self,
        company: Company
    ) -> None:
        """Write metadata sheet with one row per statement."""
        ws = self.wb.create_sheet("metadata")
        
        # Define columns
        columns = [
            ("company_id", 15),
            ("company_name", 30),
            ("company_ticker", 15),
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
        widths = [col[1] for col in columns]
        
        # Write and format header
        self._write_header_row(ws, headers, widths)
        
        # Write data rows
        for row_idx, stmt in enumerate(company.statements.values(), start=2):
            row_data = [
                company.company_id,
                company.company_name,
                company.company_ticker,
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
        
        # Freeze header row
        ws.freeze_panes = 'A2'
    
    def _write_values_sheet(
        self,
        statements: Dict[int, CleanFinancialStatement]
    ) -> None:
        """Write values sheet with all statement lines."""
        ws = self.wb.create_sheet("values")
        
        # Define columns
        columns = [
            ("document_id", 15),
            ("order", 12),
            ("concept", 40),
            ("value", 20)
        ]
        
        headers = [col[0] for col in columns]
        widths = [col[1] for col in columns]
        
        # Write and format header
        self._write_header_row(ws, headers, widths)
        
        # Write all statement lines
        row_idx = 2
        for stmt in statements.values():
            for line in stmt.statement.values():
                row_data = [
                    stmt.document_id,
                    line.order,
                    line.label,
                    line.value
                ]
                self._write_data_row(ws, row_idx, row_data)
                row_idx += 1
        
        # Freeze header row
        ws.freeze_panes = 'A2'
    
    def _write_header_row(
        self,
        ws,
        headers: list[str],
        widths: list[int]
    ) -> None:
        """Write and format header row."""
        for col_idx, (header, width) in enumerate(zip(headers, widths), start=1):
            cell = ws.cell(row=1, column=col_idx)
            cell.value = header
            cell.font = self.HEADER_FONT
            cell.fill = self.HEADER_FILL
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.BORDER
            
            # Set column width
            column_letter = get_column_letter(col_idx)
            ws.column_dimensions[column_letter].width = width
    
    def _write_data_row(
        self,
        ws,
        row_idx: int,
        data: list[Any]
    ) -> None:
        """Write a data row with appropriate formatting."""
        for col_idx, value in enumerate(data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            
            # Set value and format based on type
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


# Convenience function
def export_company_to_excel(
    company: Company,
    output_path: Path
) -> Path:
    """
    Export company statements to Excel.
    
    Convenience wrapper around ExcelExporter class.
    """
    exporter = ExcelExporter()
    return exporter.export_company(company, output_path)