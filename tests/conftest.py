"""
Shared pytest fixtures for cnv_etl tests.
"""

import pytest
from datetime import date, datetime
from pathlib import Path

from cnv_etl.models.company import Company
from cnv_etl.models.document import (
    CleanFinancialStatement,
    CleanConceptValue,
)


# ---------------------------------------------------------------------------
# Company fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_company() -> Company:
    return Company(
        id=30123456789,
        name="Test Company SA",
        ticker="TEST",
        sector="Energy",
        industry="Oil & Gas",
    )


# ---------------------------------------------------------------------------
# Statement fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_statement() -> CleanFinancialStatement:
    stmt = CleanFinancialStatement(
        document_id=100001,
        document_description=(
            "PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO - "
            "FECHA CIERRE: 2024-12-31 - NORMA CONTABLE: NIIF"
        ),
        document_link="https://example.com/doc/100001",
        submission_date=datetime(2025, 3, 15, 10, 30),
        reporting_period="Annual",
        period_end_date=date(2024, 12, 31),
        financial_statements_type="Consolidated",
        presentation_currency="ARS",
        accounting_standards_applied="IFRS",
        merger_or_demerger_in_process=False,
        treasury_shares=False,
        insolvency_proceedings=False,
        public_offering_of_equity_instruments=True,
        subscribed_shares_at_period_end=1_000_000,
        equity_share_price_at_period_end=150.50,
        free_float_percentage=35.0,
        number_of_employees=500,
    )
    stmt.add_concept(CleanConceptValue(label="Assets", value=1_000_000.0, order=1))
    stmt.add_concept(CleanConceptValue(label="Liabilities", value=600_000.0, order=2))
    stmt.add_concept(CleanConceptValue(label="Equity", value=400_000.0, order=3))
    return stmt


# ---------------------------------------------------------------------------
# SQLite fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Return a path to a temporary SQLite DB file that doesn't exist yet."""
    return tmp_path / "test_cnv.db"