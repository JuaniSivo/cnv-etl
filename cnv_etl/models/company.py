from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass, field

from openpyxl import load_workbook

from cnv_etl.models.document import CleanFinancialStatement


@dataclass
class Company:
    id: int
    name: str
    ticker: str
    description: Optional[str] = None
    sector: Optional[str] = None
    industry_group: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    sub_industry_description: Optional[str] = None
    statements: Dict[int, CleanFinancialStatement] = field(default_factory=dict)
    
    def add_statement(self, statement: CleanFinancialStatement) -> None:
        self.statements[statement.document_id] = statement


class Companies:
    def __init__(self):
        self.by_ticker: Dict[str, Company] = dict()

    def add(self, company: Company) -> None:
        self.by_ticker[company.ticker] = company

    def get_by_ticker(self, ticker: str) -> Company:
        """Get company by ticker symbol."""
        company = self.by_ticker.get(ticker)
        if company is not None:
            return company
        else:
            raise ValueError(
                f"Ticker '{ticker}' not found. Available tickers: {self.by_ticker.keys()}"
            )


    def load_from_excel(
        self,
        file_path: Path | str,
        sheet_name: str = "companies",
        column_mapping: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Load companies from an Excel file.
        
        Parameters
        ----------
        file_path : Path | str
            Path to Excel file
        sheet_name : str, default "companies"
            Name of the sheet containing company data
        column_mapping : dict, optional
            Mapping from Excel column names to Company attribute names.
            If None, uses default mapping.
            
            Example:
            {
                "cuit": "id",
                "company_name": "name",
                "ticker": "ticker",
                "company_description": "description",
                "sector": "sector",
                "industry_group": "industry_group",
                "industry": "industry",
                "subindustry": "sub_industry",
                "subindustry_definition": "sub_industry_description"
            }
            
        Raises
        ------
        FileNotFoundError
            If file doesn't exist
        ValueError
            If sheet doesn't exist or required columns are missing
            
        Examples
        --------
        >>> companies = Companies()
        >>> companies.load_from_excel("data/companies.xlsx")
        >>> companies.load_from_excel(
        ...     "data/companies.xlsx",
        ...     column_mapping={"cuit": "id", "name": "name", "symbol": "ticker"}
        ... )
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Default column mapping
        if column_mapping is None:
            column_mapping = {
                "cuit": "id",
                "company_name": "name",
                "ticker": "ticker",
                "company_description": "description",
                "sector": "sector",
                "industry_group": "industry_group",
                "industry": "industry",
                "subindustry": "sub_industry",
                "subindustry_definition": "sub_industry_description"
            }
        
        # Load workbook
        wb = load_workbook(file_path, read_only=True, data_only=True)
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(
                f"Sheet '{sheet_name}' not found. Available sheets: {wb.sheetnames}"
            )
        
        ws = wb[sheet_name]
        
        # Read header row
        headers = [str(cell.value) for cell in ws[1]]
        
        # Validate that mapped columns exist
        missing_columns = set(column_mapping.keys()) - set(headers)
        if missing_columns:
            raise ValueError(
                f"Missing required columns in Excel: {missing_columns}\n"
                f"Available columns: {headers}"
            )
        
        # Create column index mapping
        col_indices = {
            excel_col: headers.index(excel_col)
            for excel_col in column_mapping.keys()
            if excel_col in headers
        }
        
        # Read data rows
        companies_loaded = 0
        for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            # Skip empty rows
            if not any(row):
                continue
            
            # Build company data
            company_data = {}
            for excel_col, company_attr in column_mapping.items():
                if excel_col in col_indices:
                    col_idx = col_indices[excel_col]
                    value = row[col_idx] if col_idx < len(row) else None
                    
                    # Convert to string and strip whitespace
                    if value is not None:
                        value = str(value).strip()
                        # Convert empty strings to None
                        if value == "":
                            value = None
                    
                    company_data[company_attr] = value
            
            # Skip if missing required fields
            if not company_data.get("id") or not company_data.get("name"):
                print(f"⚠ Skipping row {row_idx}: missing id or name")
                continue
            
            # Create and add company
            try:
                company = Company(**company_data)
                self.add(company)
                companies_loaded += 1
            except Exception as e:
                print(f"⚠ Error loading company at row {row_idx}: {e}")
                continue
        
        wb.close()
        
        print(f"✓ Loaded {companies_loaded} companies from {file_path}")
    
    def __len__(self) -> int:
        """Return number of companies in registry."""
        return len(self.by_ticker)
    
    def __iter__(self):
        """Iterate over all companies."""
        return iter(self.by_ticker.values())
