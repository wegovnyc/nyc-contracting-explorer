#!/usr/bin/env python3
"""
OCE MCP Server - NYC Contract Explorer for LLM clients.

Exposes NYC procurement data (vendors, contracts, solicitations, spending)
via the Model Context Protocol (MCP) for use with Claude Desktop and other
MCP-compatible clients.

Usage:
    # Run with MCP inspector
    mcp dev mcp_server.py
    
    # Run directly
    python mcp_server.py
"""

import os
import sqlite3
from typing import Optional

import duckdb
from mcp.server.fastmcp import FastMCP

# Initialize MCP server
mcp = FastMCP("oce")

# Database configuration
DB_FILE = os.path.join(os.path.dirname(__file__), "databook.db")


# ============================================================================
# Database Helpers
# ============================================================================

def get_db():
    """Get SQLite database connection with row factory."""
    db = sqlite3.connect(DB_FILE)
    db.row_factory = sqlite3.Row
    return db


def query_db(query: str, args: tuple = (), one: bool = False):
    """Execute a SQLite query and return results."""
    db = get_db()
    cur = db.execute(query, args)
    rv = cur.fetchall()
    db.close()
    return (rv[0] if rv else None) if one else rv


def get_spending_connection():
    """Get DuckDB connection configured for HTTPS access to public S3 bucket."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


# S3 bucket base URL (public, no credentials needed)
S3_HTTPS_BASE = "https://nyc-databook-spending.s3.amazonaws.com"

# Fiscal years available in the spending data
SPENDING_FISCAL_YEARS = list(range(2010, 2026))  # FY2010-FY2025


def get_spending_files(fiscal_year: int = None) -> str:
    """
    Generate a list of parquet file URLs for spending data.
    
    Args:
        fiscal_year: Specific year, or None for recent years
        
    Returns:
        SQL-ready list of file URLs
    """
    # Known chunk counts per fiscal year (based on actual S3 contents)
    CHUNKS_PER_YEAR = {
        2024: 17, 2023: 18, 2022: 17, 2021: 15, 2020: 14,
        2019: 13, 2018: 12, 2017: 11, 2016: 10, 2015: 10,
        2014: 9, 2013: 9, 2012: 8, 2011: 7, 2010: 6
    }
    
    if fiscal_year:
        years = [fiscal_year]
    else:
        # Default to recent 3 years for performance
        years = [2024, 2023, 2022]
    
    urls = []
    for fy in years:
        chunks = CHUNKS_PER_YEAR.get(fy, 10)  # Default to 10 if unknown
        for chunk in range(1, chunks + 1):
            url = f"{S3_HTTPS_BASE}/fiscal_year={fy}/chunk_{chunk:04d}.parquet"
            urls.append(f"'{url}'")
    return "[" + ", ".join(urls) + "]"


def get_contracts_files(fiscal_year: int = None) -> str:
    """Generate parquet file URLs for contract data."""
    if fiscal_year:
        years = [fiscal_year]
    else:
        years = list(range(2020, 2026))
    
    urls = []
    for fy in years:
        url = f"{S3_HTTPS_BASE}/contracts/fiscal_year={fy}/fy{fy}_active_chunks_chunk_0001.parquet"
        urls.append(f"'{url}'")
    
    return "[" + ", ".join(urls) + "]"


def format_currency(amount) -> str:
    """Format a number as currency."""
    if amount is None:
        return "N/A"
    try:
        return f"${float(amount):,.2f}"
    except (ValueError, TypeError):
        return "N/A"


# ============================================================================
# Vendor Tools
# ============================================================================

@mcp.tool()
def search_vendors(query: str, limit: int = 20) -> str:
    """
    Search NYC vendors by name.
    
    Args:
        query: Vendor name to search for (partial match supported)
        limit: Maximum number of results (default 20, max 100)
    
    Returns:
        List of matching vendors with basic info
    """
    limit = min(limit, 100)
    
    rows = query_db(
        """
        SELECT passport_supplier_id, name, certification_type, ethnicity, business_category
        FROM vendors 
        WHERE name LIKE ? 
        ORDER BY name 
        LIMIT ?
        """,
        (f"%{query}%", limit)
    )
    
    if not rows:
        return f"No vendors found matching '{query}'"
    
    results = [f"**Found {len(rows)} vendor(s) matching '{query}':**\n"]
    for row in rows:
        cert = row['certification_type'] or 'None'
        results.append(
            f"- **{row['name']}** (ID: {row['passport_supplier_id']})\n"
            f"  - Certification: {cert} | Category: {row['business_category'] or 'N/A'}"
        )
    
    return "\n".join(results)


@mcp.tool()
def get_vendor_profile(vendor_id: str) -> str:
    """
    Get detailed profile for a specific vendor.
    
    Args:
        vendor_id: The PASSPort Supplier ID (e.g., "123456")
    
    Returns:
        Vendor profile with contract statistics
    """
    vendor = query_db(
        "SELECT * FROM vendors WHERE passport_supplier_id = ?",
        (vendor_id,),
        one=True
    )
    
    if not vendor:
        return f"Vendor with ID '{vendor_id}' not found"
    
    # Get contract stats
    contracts = query_db(
        "SELECT * FROM contracts WHERE vendor_name = ?",
        (vendor['name'],)
    )
    
    total_awarded = sum(c['award_amount'] for c in contracts if c['award_amount'])
    
    profile = f"""
**{vendor['name']}**

**Basic Info:**
- Supplier ID: {vendor['passport_supplier_id']}
- Certification: {vendor['certification_type'] or 'None'}
- Ethnicity: {vendor['ethnicity'] or 'N/A'}
- Business Category: {vendor['business_category'] or 'N/A'}
- Website: {vendor['website_url'] or 'N/A'}

**Contract Statistics:**
- Total Contracts: {len(contracts)}
- Total Awarded: {format_currency(total_awarded)}
"""
    
    # Recent contracts
    if contracts:
        profile += "\n**Recent Contracts:**\n"
        for c in sorted(contracts, key=lambda x: x['start_date'] or '', reverse=True)[:5]:
            profile += f"- {c['contract_id']}: {c['contract_title'][:50]}... ({format_currency(c['award_amount'])})\n"
    
    return profile.strip()


# ============================================================================
# Contract Tools
# ============================================================================

@mcp.tool()
def search_contracts(
    query: Optional[str] = None,
    vendor: Optional[str] = None,
    agency: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20
) -> str:
    """
    Search NYC contracts with filters.
    
    Args:
        query: Search in contract ID or title
        vendor: Filter by vendor name (partial match)
        agency: Filter by agency name (partial match)
        status: Filter by status (e.g., "Registered", "Active")
        limit: Maximum results (default 20, max 100)
    
    Returns:
        List of matching contracts
    """
    limit = min(limit, 100)
    
    where_clauses = []
    params = []
    
    if query:
        where_clauses.append("(contract_id LIKE ? OR contract_title LIKE ?)")
        params.extend([f"%{query}%", f"%{query}%"])
    
    if vendor:
        where_clauses.append("vendor_name LIKE ?")
        params.append(f"%{vendor}%")
    
    if agency:
        where_clauses.append("agency LIKE ?")
        params.append(f"%{agency}%")
    
    if status:
        where_clauses.append("status = ?")
        params.append(status)
    
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    rows = query_db(
        f"""
        SELECT contract_id, contract_title, vendor_name, agency, 
               award_amount, status, start_date
        FROM contracts 
        {where_str}
        ORDER BY start_date DESC 
        LIMIT ?
        """,
        tuple(params) + (limit,)
    )
    
    if not rows:
        return "No contracts found matching the criteria"
    
    results = [f"**Found {len(rows)} contract(s):**\n"]
    for row in rows:
        title = (row['contract_title'] or 'Untitled')[:50]
        results.append(
            f"- **{row['contract_id']}**: {title}...\n"
            f"  - Vendor: {row['vendor_name']}\n"
            f"  - Agency: {row['agency'] or 'N/A'}\n"
            f"  - Amount: {format_currency(row['award_amount'])} | Status: {row['status'] or 'N/A'}"
        )
    
    return "\n".join(results)


@mcp.tool()
def get_contract_details(contract_id: str) -> str:
    """
    Get detailed information for a specific contract.
    
    Args:
        contract_id: The contract ID (e.g., "CT1-856-20251234567")
    
    Returns:
        Full contract details including linked solicitation
    """
    contract = query_db(
        "SELECT * FROM contracts WHERE ctr_id = ? OR contract_id = ?",
        (contract_id, contract_id),
        one=True
    )
    
    if not contract:
        return f"Contract '{contract_id}' not found"
    
    details = f"""
**Contract: {contract['contract_id']}**

**Title:** {contract['contract_title'] or 'N/A'}

**Parties:**
- Vendor: {contract['vendor_name']}
- Agency: {contract['agency'] or 'N/A'}

**Financials:**
- Award Amount: {format_currency(contract['award_amount'])}
- Current Amount: {format_currency(contract['current_amount'])}

**Timeline:**
- Start Date: {contract['start_date'] or 'N/A'}
- End Date: {contract['end_date'] or 'N/A'}
- Status: {contract['status'] or 'N/A'}

**Procurement:**
- Method: {contract['procurement_method'] or 'N/A'}
- Industry: {contract['industry'] or 'N/A'}
"""
    
    # Check for linked solicitation
    if contract.get('normalized_epin'):
        solicitation = query_db(
            "SELECT * FROM solicitations WHERE normalized_epin = ?",
            (contract['normalized_epin'],),
            one=True
        )
        if solicitation:
            details += f"""
**Linked Solicitation:**
- EPIN: {solicitation['epin']}
- Title: {solicitation['procurement_name'][:60]}...
"""
    
    return details.strip()


@mcp.tool()
def get_contract_stats(
    agency: Optional[str] = None,
    fiscal_year: Optional[int] = None
) -> str:
    """
    Get contract count and statistics for an agency and/or fiscal year.
    
    Use this to answer questions like "How many contracts did Parks have in 2023?"
    
    Args:
        agency: Agency name to filter by (partial match, e.g., "Parks", "Education")
        fiscal_year: Fiscal year to filter by (e.g., 2023, 2024)
    
    Returns:
        Contract counts and total award amounts
    """
    where_clauses = []
    params = []
    
    if agency:
        where_clauses.append("agency LIKE ?")
        params.append(f"%{agency}%")
    
    if fiscal_year:
        # Contract IDs contain the fiscal year (e.g., CT1-846-20238xxxxx for FY2023)
        where_clauses.append("contract_id LIKE ?")
        params.append(f"%{fiscal_year}%")
    
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    # Get aggregate stats
    stats = query_db(
        f"""
        SELECT 
            COUNT(*) as contract_count,
            SUM(award_amount) as total_amount,
            AVG(award_amount) as avg_amount,
            MIN(award_amount) as min_amount,
            MAX(award_amount) as max_amount
        FROM contracts
        {where_str}
        """,
        tuple(params),
        one=True
    )
    
    if not stats or stats['contract_count'] == 0:
        return f"No contracts found for the specified criteria"
    
    # Get breakdown by status
    status_breakdown = query_db(
        f"""
        SELECT status, COUNT(*) as count, SUM(award_amount) as total
        FROM contracts
        {where_str}
        GROUP BY status
        ORDER BY count DESC
        """,
        tuple(params)
    )
    
    # Get top agencies if no agency filter
    agency_info = ""
    if not agency:
        top_agencies = query_db(
            f"""
            SELECT agency, COUNT(*) as count, SUM(award_amount) as total
            FROM contracts
            {where_str}
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
            """,
            tuple(params)
        )
        if top_agencies:
            agency_info = "\n**Top Agencies by Contract Count:**\n"
            for a in top_agencies:
                agency_info += f"- {a['agency'] or 'Unknown'}: {a['count']} contracts ({format_currency(a['total'])})\n"
    
    title_parts = []
    if agency:
        title_parts.append(agency)
    if fiscal_year:
        title_parts.append(f"FY{fiscal_year}")
    title = " - ".join(title_parts) if title_parts else "All NYC Contracts"
    
    result = f"""**Contract Statistics: {title}**

**Summary:**
- Total Contracts: **{stats['contract_count']:,}**
- Total Award Amount: **{format_currency(stats['total_amount'])}**
- Average Award: {format_currency(stats['avg_amount'])}
- Range: {format_currency(stats['min_amount'])} - {format_currency(stats['max_amount'])}

**By Status:**
"""
    
    for s in status_breakdown:
        result += f"- {s['status'] or 'Unknown'}: {s['count']} contracts ({format_currency(s['total'])})\n"
    
    result += agency_info
    
    return result.strip()


@mcp.tool()
def get_agency_contracts(agency: str, limit: int = 10) -> str:
    """
    Get contract summary for a specific NYC agency.
    
    Use this to get an overview of an agency's contracts.
    
    Args:
        agency: Agency name (partial match, e.g., "Parks", "Education", "DOT")
        limit: Number of recent contracts to show (default 10, max 50)
    
    Returns:
        Agency contract summary with counts, totals, and recent contracts
    """
    limit = min(limit, 50)
    
    # Get aggregate stats for the agency
    stats = query_db(
        """
        SELECT 
            COUNT(*) as contract_count,
            SUM(award_amount) as total_amount,
            COUNT(DISTINCT vendor_name) as unique_vendors
        FROM contracts
        WHERE agency LIKE ?
        """,
        (f"%{agency}%",),
        one=True
    )
    
    if not stats or stats['contract_count'] == 0:
        return f"No contracts found for agency matching '{agency}'"
    
    # Get recent contracts
    recent = query_db(
        """
        SELECT contract_id, contract_title, vendor_name, award_amount, start_date, status
        FROM contracts
        WHERE agency LIKE ?
        ORDER BY start_date DESC
        LIMIT ?
        """,
        (f"%{agency}%", limit)
    )
    
    # Get top vendors for this agency
    top_vendors = query_db(
        """
        SELECT vendor_name, COUNT(*) as count, SUM(award_amount) as total
        FROM contracts
        WHERE agency LIKE ?
        GROUP BY vendor_name
        ORDER BY total DESC
        LIMIT 5
        """,
        (f"%{agency}%",)
    )
    
    result = f"""**Agency Contracts: {agency}**

**Summary:**
- Total Contracts: **{stats['contract_count']:,}**
- Total Award Amount: **{format_currency(stats['total_amount'])}**
- Unique Vendors: {stats['unique_vendors']}

**Top Vendors by Award Amount:**
"""
    
    for v in top_vendors:
        result += f"- {v['vendor_name']}: {v['count']} contracts ({format_currency(v['total'])})\n"
    
    result += f"\n**Recent Contracts ({limit} shown):**\n"
    for row in recent:
        title = (row['contract_title'] or 'Untitled')[:40]
        result += f"- **{row['contract_id']}**: {title}...\n"
        result += f"  Vendor: {row['vendor_name']} | {format_currency(row['award_amount'])}\n"
    
    return result.strip()


# ============================================================================
# Solicitation Tools
# ============================================================================

@mcp.tool()
def search_solicitations(
    query: Optional[str] = None,
    status: Optional[str] = None,
    agency: Optional[str] = None,
    limit: int = 20
) -> str:
    """
    Search NYC solicitations (RFPs, bids, etc.).
    
    Args:
        query: Search in EPIN or procurement name
        status: Filter by status (e.g., "Released", "Closed")
        agency: Filter by agency name
        limit: Maximum results (default 20, max 100)
    
    Returns:
        List of matching solicitations
    """
    limit = min(limit, 100)
    
    where_clauses = []
    params = []
    
    if query:
        where_clauses.append("(epin LIKE ? OR procurement_name LIKE ?)")
        params.extend([f"%{query}%", f"%{query}%"])
    
    if status:
        where_clauses.append("rfx_status = ?")
        params.append(status)
    
    if agency:
        where_clauses.append("agency LIKE ?")
        params.append(f"%{agency}%")
    
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    rows = query_db(
        f"""
        SELECT epin, procurement_name, agency, rfx_status, 
               release_date, due_date, procurement_method
        FROM solicitations 
        {where_str}
        ORDER BY release_date DESC 
        LIMIT ?
        """,
        tuple(params) + (limit,)
    )
    
    if not rows:
        return "No solicitations found matching the criteria"
    
    results = [f"**Found {len(rows)} solicitation(s):**\n"]
    for row in rows:
        name = (row['procurement_name'] or 'Untitled')[:50]
        results.append(
            f"- **{row['epin']}**: {name}...\n"
            f"  - Agency: {row['agency'] or 'N/A'}\n"
            f"  - Status: {row['rfx_status'] or 'N/A'} | Due: {row['due_date'] or 'N/A'}"
        )
    
    return "\n".join(results)


@mcp.tool()
def get_solicitation_details(epin: str) -> str:
    """
    Get detailed information for a specific solicitation.
    
    Args:
        epin: The EPIN (solicitation ID)
    
    Returns:
        Full solicitation details with resulting contracts
    """
    solicitation = query_db(
        "SELECT * FROM solicitations WHERE epin = ?",
        (epin,),
        one=True
    )
    
    if not solicitation:
        return f"Solicitation '{epin}' not found"
    
    details = f"""
**Solicitation: {solicitation['epin']}**

**Title:** {solicitation['procurement_name'] or 'N/A'}

**Agency:** {solicitation['agency'] or 'N/A'}

**Timeline:**
- Release Date: {solicitation['release_date'] or 'N/A'}
- Due Date: {solicitation['due_date'] or 'N/A'}
- Status: {solicitation['rfx_status'] or 'N/A'}

**Procurement:**
- Method: {solicitation['procurement_method'] or 'N/A'}
- Industry: {solicitation['industry'] or 'N/A'}
"""
    
    # Find resulting contracts
    contracts = query_db(
        "SELECT * FROM contracts WHERE normalized_epin LIKE ?",
        (solicitation['normalized_epin'] + '%',)
    )
    
    if contracts:
        total = sum(c['award_amount'] for c in contracts if c['award_amount'])
        details += f"""
**Resulting Contracts:** {len(contracts)}
- Total Awarded: {format_currency(total)}
"""
        for c in contracts[:5]:
            details += f"- {c['contract_id']}: {c['vendor_name']} ({format_currency(c['award_amount'])})\n"
    
    return details.strip()


# ============================================================================
# Spending Tools (DuckDB/S3)
# ============================================================================

@mcp.tool()
def search_transactions(
    query: str,
    limit: int = 50
) -> str:
    """
    Search NYC spending transactions by payee, agency, or contract.
    
    Queries 147M+ transactions from Checkbook NYC via S3.
    
    Args:
        query: Search term (payee name, agency, or contract ID)
        limit: Maximum results (default 50, max 200)
    
    Returns:
        List of matching transactions
    """
    limit = min(limit, 200)
    
    try:
        con = get_spending_connection()
        
        safe_q = query.replace("'", "''")
        
        files = get_spending_files()  # Recent 5 years
        
        result = con.execute(f"""
            SELECT 
                issue_date,
                agency,
                payee_name,
                contract_id,
                expense_category,
                TRY_CAST(check_amount AS DOUBLE) as check_amount
            FROM read_parquet({files}, union_by_name=true)
            WHERE payee_name ILIKE '%{safe_q}%' 
               OR agency ILIKE '%{safe_q}%'
               OR contract_id ILIKE '%{safe_q}%'
            ORDER BY issue_date DESC
            LIMIT {limit}
        """).fetchall()
        
        con.close()
        
        if not result:
            return f"No transactions found matching '{query}'"
        
        output = [f"**Found {len(result)} transaction(s) matching '{query}':**\n"]
        total = sum(r[5] or 0 for r in result)
        output.append(f"*Total shown: {format_currency(total)}*\n")
        
        for r in result[:20]:  # Show first 20 in detail
            output.append(
                f"- **{r[0]}**: {r[2]} ({r[1]})\n"
                f"  - Amount: {format_currency(r[5])} | Contract: {r[3] or 'N/A'}"
            )
        
        if len(result) > 20:
            output.append(f"\n*... and {len(result) - 20} more transactions*")
        
        return "\n".join(output)
        
    except Exception as e:
        return f"Error querying spending data: {str(e)}"


@mcp.tool()
def get_vendor_spending(vendor_name: str, fiscal_year: Optional[int] = None) -> str:
    """
    Get spending history for a vendor from Checkbook NYC.
    
    Args:
        vendor_name: Vendor/payee name to search
        fiscal_year: Optional fiscal year filter (e.g., 2024)
    
    Returns:
        Spending summary and recent transactions
    """
    try:
        con = get_spending_connection()
        
        safe_name = vendor_name.replace("'", "''")
        
        files = get_spending_files(fiscal_year)
        
        # Get summary
        summary = con.execute(f"""
            SELECT 
                COUNT(*) as tx_count,
                SUM(TRY_CAST(check_amount AS DOUBLE)) as total,
                MIN(issue_date) as first_tx,
                MAX(issue_date) as last_tx
            FROM read_parquet({files}, union_by_name=true)
            WHERE payee_name ILIKE '%{safe_name}%'
        """).fetchone()
        
        # Get by fiscal year (query each year separately for accurate breakdown)
        by_year = []
        for fy in range(2024, 2019, -1):  # Last 5 years
            try:
                fy_files = get_spending_files(fy)
                fy_total = con.execute(f"""
                    SELECT SUM(TRY_CAST(check_amount AS DOUBLE)) as total
                    FROM read_parquet({fy_files}, union_by_name=true)
                    WHERE payee_name ILIKE '%{safe_name}%'
                """).fetchone()
                if fy_total and fy_total[0]:
                    by_year.append((fy, fy_total[0]))
            except:
                pass
        
        con.close()
        
        if not summary or summary[0] == 0:
            return f"No spending found for vendor '{vendor_name}'"
        
        output = f"""
**Spending Summary for '{vendor_name}'**
{f'(Fiscal Year {fiscal_year})' if fiscal_year else '(All Time)'}

- Total Transactions: {summary[0]:,}
- Total Amount: {format_currency(summary[1])}
- First Transaction: {summary[2]}
- Last Transaction: {summary[3]}

**By Fiscal Year:**
"""
        for row in by_year:
            output += f"- FY{row[0]}: {format_currency(row[1])}\n"
        
        return output.strip()
        
    except Exception as e:
        return f"Error querying vendor spending: {str(e)}"


@mcp.tool()
def get_spending_by_year(fiscal_year: int) -> str:
    """
    Get aggregate spending statistics for a fiscal year.
    
    Args:
        fiscal_year: Fiscal year (e.g., 2024)
    
    Returns:
        Spending breakdown by agency and category
    """
    try:
        con = get_spending_connection()
        
        files = get_spending_files(fiscal_year)
        
        # Overall stats
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as tx_count,
                SUM(TRY_CAST(check_amount AS DOUBLE)) as total
            FROM read_parquet({files}, union_by_name=true)
        """).fetchone()
        
        # Top agencies
        by_agency = con.execute(f"""
            SELECT 
                agency,
                SUM(TRY_CAST(check_amount AS DOUBLE)) as total
            FROM read_parquet({files}, union_by_name=true)
            GROUP BY agency
            ORDER BY total DESC
            LIMIT 10
        """).fetchall()
        
        con.close()
        
        if not stats or stats[0] == 0:
            return f"No spending data found for fiscal year {fiscal_year}"
        
        output = f"""
**NYC Spending - Fiscal Year {fiscal_year}**

- Total Transactions: {stats[0]:,}
- Total Amount: {format_currency(stats[1])}

**Top 10 Agencies by Spending:**
"""
        for i, row in enumerate(by_agency, 1):
            output += f"{i}. {row[0]}: {format_currency(row[1])}\n"
        
        return output.strip()
        
    except Exception as e:
        return f"Error querying fiscal year spending: {str(e)}"


@mcp.tool()
def get_datasets_info() -> str:
    """
    Get information about available datasets and their freshness.
    
    Returns:
        List of datasets with record counts and last updated dates
    """
    import glob
    import json
    from datetime import datetime
    
    base_dir = os.path.dirname(__file__)
    
    datasets = []
    
    # Local CSV files
    csv_files = [
        ("Vendors", "vendor_data.csv"),
        ("Contracts", "contracts_data.csv"),
        ("Solicitations", "solicitations_data.csv"),
    ]
    
    for label, filename in csv_files:
        filepath = os.path.join(base_dir, filename)
        if os.path.exists(filepath):
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            try:
                with open(filepath, 'rb') as f:
                    count = sum(1 for _ in f) - 1
            except:
                count = "Unknown"
            datasets.append(f"- **{label}**: {count:,} records (updated {mtime.strftime('%Y-%m-%d')})")
        else:
            datasets.append(f"- **{label}**: Not found locally")
    
    # S3 spending data
    datasets.append(f"- **NYC Spending (S3)**: 147M+ transactions (public bucket)")
    datasets.append(f"- **Checkbook Contracts (S3)**: Contract metadata (public bucket)")
    
    output = """
**Available Datasets**

""" + "\n".join(datasets) + """

**Data Sources:**
- PASSPort (MOCS): Vendors, contracts, solicitations
- Checkbook NYC: Spending transactions
- City Record Online: Public notices

**S3 Bucket:** s3://nyc-databook-spending (PUBLIC - no credentials needed)
"""
    
    return output.strip()


# ============================================================================
# Statistics & Overview Tools
# ============================================================================

@mcp.tool()
def get_database_overview() -> str:
    """
    Get a complete overview of all NYC procurement data available.
    
    Use this as the first call to understand what data is available.
    
    Returns:
        Summary counts and statistics for all data tables
    """
    # Get counts for each table
    vendors_count = query_db("SELECT COUNT(*) as c FROM vendors", one=True)['c']
    contracts_count = query_db("SELECT COUNT(*) as c FROM contracts", one=True)['c']
    solicitations_count = query_db("SELECT COUNT(*) as c FROM solicitations", one=True)['c']
    agencies_count = query_db("SELECT COUNT(*) as c FROM agencies", one=True)['c']
    
    # Contract stats
    contract_stats = query_db("""
        SELECT 
            SUM(award_amount) as total_value,
            AVG(award_amount) as avg_value,
            COUNT(DISTINCT agency) as agency_count,
            COUNT(DISTINCT vendor_name) as vendor_count
        FROM contracts
    """, one=True)
    
    # Status breakdown
    contract_status = query_db("""
        SELECT status, COUNT(*) as count 
        FROM contracts 
        GROUP BY status 
        ORDER BY count DESC 
        LIMIT 5
    """)
    
    # Solicitation status
    sol_status = query_db("""
        SELECT rfx_status, COUNT(*) as count 
        FROM solicitations 
        GROUP BY rfx_status 
        ORDER BY count DESC 
        LIMIT 5
    """)
    
    result = f"""**NYC Procurement Database Overview**

**Total Records:**
- Vendors: **{vendors_count:,}**
- Contracts: **{contracts_count:,}**
- Solicitations: **{solicitations_count:,}**
- Agencies: **{agencies_count:,}**

**Contract Summary:**
- Total Award Value: **{format_currency(contract_stats['total_value'])}**
- Average Contract: {format_currency(contract_stats['avg_value'])}
- Unique Agencies: {contract_stats['agency_count']}
- Unique Vendors: {contract_stats['vendor_count']}

**Contract Status Breakdown:**
"""
    for s in contract_status:
        result += f"- {s['status'] or 'Unknown'}: {s['count']:,}\n"
    
    result += "\n**Solicitation Status Breakdown:**\n"
    for s in sol_status:
        result += f"- {s['rfx_status'] or 'Unknown'}: {s['count']:,}\n"
    
    result += """
**Available Tools:**
- `get_contract_stats(agency, fiscal_year)` - Count contracts by agency/year
- `get_agency_contracts(agency)` - Agency contract summary
- `get_vendor_stats()` - Vendor certification breakdown
- `get_solicitation_stats()` - Solicitation trends
- `search_contracts/vendors/solicitations` - Search individual records
- `get_spending_by_year(year)` - Checkbook NYC spending data
"""
    
    return result.strip()


@mcp.tool()
def get_vendor_stats() -> str:
    """
    Get statistics about NYC vendors including certification types and categories.
    
    Returns:
        Vendor breakdown by certification type, ethnicity, and business category
    """
    total = query_db("SELECT COUNT(*) as c FROM vendors", one=True)['c']
    
    # By certification type
    cert_stats = query_db("""
        SELECT certification_type, COUNT(*) as count
        FROM vendors
        WHERE certification_type IS NOT NULL AND certification_type != ''
        GROUP BY certification_type
        ORDER BY count DESC
        LIMIT 10
    """)
    
    # By ethnicity
    ethnicity_stats = query_db("""
        SELECT ethnicity, COUNT(*) as count
        FROM vendors
        WHERE ethnicity IS NOT NULL AND ethnicity != ''
        GROUP BY ethnicity
        ORDER BY count DESC
        LIMIT 10
    """)
    
    # By business category
    category_stats = query_db("""
        SELECT business_category, COUNT(*) as count
        FROM vendors
        WHERE business_category IS NOT NULL AND business_category != ''
        GROUP BY business_category
        ORDER BY count DESC
        LIMIT 10
    """)
    
    result = f"""**NYC Vendor Statistics**

**Total Registered Vendors: {total:,}**

**By Certification Type:**
"""
    for c in cert_stats:
        pct = (c['count'] / total) * 100
        result += f"- {c['certification_type']}: {c['count']:,} ({pct:.1f}%)\n"
    
    result += "\n**By Ethnicity:**\n"
    for e in ethnicity_stats:
        pct = (e['count'] / total) * 100
        result += f"- {e['ethnicity']}: {e['count']:,} ({pct:.1f}%)\n"
    
    result += "\n**By Business Category:**\n"
    for b in category_stats:
        result += f"- {b['business_category']}: {b['count']:,}\n"
    
    return result.strip()


@mcp.tool()
def get_solicitation_stats(agency: Optional[str] = None) -> str:
    """
    Get solicitation statistics - counts by status, agency, and method.
    
    Args:
        agency: Optional agency name filter (partial match)
    
    Returns:
        Solicitation breakdown by status, method, and top agencies
    """
    where = ""
    params = ()
    if agency:
        where = "WHERE agency LIKE ?"
        params = (f"%{agency}%",)
    
    total = query_db(f"SELECT COUNT(*) as c FROM solicitations {where}", params, one=True)['c']
    
    if total == 0:
        return f"No solicitations found{' for ' + agency if agency else ''}"
    
    # By status
    status_stats = query_db(f"""
        SELECT rfx_status, COUNT(*) as count
        FROM solicitations
        {where}
        GROUP BY rfx_status
        ORDER BY count DESC
    """, params)
    
    # By procurement method
    method_stats = query_db(f"""
        SELECT procurement_method, COUNT(*) as count
        FROM solicitations
        {where}
        GROUP BY procurement_method
        ORDER BY count DESC
        LIMIT 10
    """, params)
    
    # Top agencies (if no agency filter)
    agency_info = ""
    if not agency:
        top_agencies = query_db("""
            SELECT agency, COUNT(*) as count
            FROM solicitations
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
        """)
        agency_info = "\n**Top Agencies by Solicitation Count:**\n"
        for a in top_agencies:
            agency_info += f"- {a['agency'] or 'Unknown'}: {a['count']:,}\n"
    
    title = f"Solicitation Statistics{' - ' + agency if agency else ''}"
    result = f"""**{title}**

**Total Solicitations: {total:,}**

**By Status:**
"""
    for s in status_stats:
        pct = (s['count'] / total) * 100
        result += f"- {s['rfx_status'] or 'Unknown'}: {s['count']:,} ({pct:.1f}%)\n"
    
    result += "\n**By Procurement Method:**\n"
    for m in method_stats:
        result += f"- {m['procurement_method'] or 'Unknown'}: {m['count']:,}\n"
    
    result += agency_info
    
    return result.strip()


@mcp.tool()
def get_yearly_trends() -> str:
    """
    Get contract and solicitation trends by year.
    
    Returns:
        Year-over-year counts and values for contracts
    """
    # Extract year from contract IDs (format: CT1-846-20238xxxxx)
    yearly_contracts = query_db("""
        SELECT 
            SUBSTR(contract_id, INSTR(contract_id, '-202') + 1, 4) as year,
            COUNT(*) as count,
            SUM(award_amount) as total_value
        FROM contracts
        WHERE contract_id LIKE '%202%'
        GROUP BY year
        ORDER BY year DESC
        LIMIT 10
    """)
    
    # Also try by start_date
    yearly_by_date = query_db("""
        SELECT 
            SUBSTR(start_date, 1, 4) as year,
            COUNT(*) as count,
            SUM(award_amount) as total_value
        FROM contracts
        WHERE start_date IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 10
    """)
    
    # Solicitations by year
    yearly_sols = query_db("""
        SELECT 
            SUBSTR(release_date, 1, 4) as year,
            COUNT(*) as count
        FROM solicitations
        WHERE release_date IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 10
    """)
    
    result = """**NYC Procurement Yearly Trends**

**Contracts by Start Year:**
"""
    for y in yearly_by_date:
        if y['year'] and y['year'].isdigit():
            result += f"- {y['year']}: {y['count']:,} contracts ({format_currency(y['total_value'])})\n"
    
    result += "\n**Solicitations by Release Year:**\n"
    for y in yearly_sols:
        if y['year'] and y['year'].isdigit():
            result += f"- {y['year']}: {y['count']:,} solicitations\n"
    
    return result.strip()


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Run the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
