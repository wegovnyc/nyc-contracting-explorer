#!/usr/bin/env python3
"""
Load contracts and solicitations locally and test CROL matching.
"""

import sqlite3
import csv
import re
import os

DB_FILE = "databook.db"


def normalize_epin(epin):
    """Normalize EPIN for matching."""
    if not epin:
        return None
    # Remove hyphens, spaces, and make uppercase
    return re.sub(r'[^A-Z0-9]', '', epin.upper())


def load_solicitations(conn):
    """Load solicitations from CSV."""
    if not os.path.exists("solicitations_data.csv"):
        print("solicitations_data.csv not found")
        return
    
    print("Loading Solicitations...")
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS solicitations (
        rfp_id TEXT,
        bpm_id TEXT,
        program TEXT,
        industry TEXT,
        epin TEXT PRIMARY KEY,
        procurement_name TEXT,
        agency TEXT,
        agency_id TEXT,
        rfx_status TEXT,
        release_date TEXT,
        due_date TEXT,
        main_commodity TEXT,
        procurement_method TEXT,
        normalized_epin TEXT
    )
    """)
    
    # Clear existing
    cursor.execute("DELETE FROM solicitations")
    
    with open("solicitations_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            epin = row.get("EPIN")
            to_db.append((
                row.get("RFP-ID"),
                row.get("BPM-ID"),
                row.get("Program"),
                row.get("Industry"),
                epin,
                row.get("Procurement Name"),
                row.get("Agency"),
                row.get("wegov-org-id", ""),
                row.get("RFx Status"),
                row.get("Release Date"),
                row.get("Due Date"),
                row.get("Main Commodity"),
                row.get("Procurement Method"),
                normalize_epin(epin)
            ))
        
        cursor.executemany("""
        INSERT OR IGNORE INTO solicitations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, to_db)
        conn.commit()
    print(f"Loaded {len(to_db)} solicitations.")


def load_contracts(conn):
    """Load contracts from CSV."""
    if not os.path.exists("contracts_data.csv"):
        print("contracts_data.csv not found")
        return
    
    print("Loading Contracts...")
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS contracts (
        ctr_id TEXT,
        epin TEXT,
        contract_id TEXT,
        contract_title TEXT,
        agency TEXT,
        agency_id TEXT,
        vendor_name TEXT,
        program TEXT,
        procurement_method TEXT,
        contract_type TEXT,
        status TEXT,
        award_amount REAL,
        current_amount REAL,
        start_date TEXT,
        end_date TEXT,
        industry TEXT,
        normalized_contract_id TEXT,
        normalized_epin TEXT
    )
    """)
    
    # Clear existing
    cursor.execute("DELETE FROM contracts")
    
    def clean_money(val):
        if not val: return 0.0
        cleaned = re.sub(r'[^0-9.]', '', val)
        if not cleaned: return 0.0
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    
    def normalize_contract_id(cid):
        if not cid: return None
        return re.sub(r'[^A-Z0-9]', '', cid.upper())
    
    with open("contracts_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            cid = row.get("Contract ID")
            epin = row.get("EPIN")
            to_db.append((
                row.get("CTR-ID"),
                epin,
                cid,
                row.get("Contract Title"),
                row.get("Agency"),
                row.get("wegov-org-id", ""),
                row.get("Vendor"),
                row.get("Program"),
                row.get("Procurement Method"),
                row.get("Contract Type"),
                row.get("Status"),
                clean_money(row.get("Award Amount")),
                clean_money(row.get("Current Contract Amount")),
                row.get("Contract Start Date"),
                row.get("Contract End Date"),
                row.get("Industry"),
                normalize_contract_id(cid),
                normalize_epin(epin)
            ))
        
        cursor.executemany("""
        INSERT OR IGNORE INTO contracts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, to_db)
        conn.commit()
    print(f"Loaded {len(to_db)} contracts.")


def test_matching(conn):
    """Test CROL matching with solicitations and contracts."""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("CROL MATCHING ANALYSIS")
    print("="*60)
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) FROM solicitations")
    solicit_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM contracts")
    contract_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM crol")
    crol_count = cursor.fetchone()[0]
    
    print(f"\nData Counts:")
    print(f"  Solicitations: {solicit_count:,}")
    print(f"  Contracts: {contract_count:,}")
    print(f"  CROL Records: {crol_count:,}")
    
    # Direct EPIN match with solicitations
    cursor.execute("""
        SELECT COUNT(DISTINCT s.epin) 
        FROM solicitations s 
        INNER JOIN crol c ON c.PIN = s.epin
    """)
    exact_solicit_match = cursor.fetchone()[0]
    
    # Normalized EPIN match with solicitations
    cursor.execute("""
        SELECT COUNT(DISTINCT s.epin) 
        FROM solicitations s 
        INNER JOIN crol c ON UPPER(REPLACE(c.PIN, '-', '')) = s.normalized_epin
    """)
    norm_solicit_match = cursor.fetchone()[0]
    
    print(f"\nSolicitation Matches:")
    print(f"  Exact PIN match: {exact_solicit_match}")
    print(f"  Normalized match: {norm_solicit_match}")
    
    # Contract EPIN match
    cursor.execute("""
        SELECT COUNT(DISTINCT c.contract_id) 
        FROM contracts c 
        INNER JOIN crol cr ON cr.PIN = c.epin
    """)
    exact_contract_epin = cursor.fetchone()[0]
    
    # Contract normalized EPIN match
    cursor.execute("""
        SELECT COUNT(DISTINCT c.contract_id) 
        FROM contracts c 
        INNER JOIN crol cr ON UPPER(REPLACE(cr.PIN, '-', '')) = c.normalized_epin
    """)
    norm_contract_match = cursor.fetchone()[0]
    
    print(f"\nContract Matches (by EPIN):")
    print(f"  Exact EPIN match: {exact_contract_epin}")
    print(f"  Normalized match: {norm_contract_match}")
    
    # Sample matching PINs
    print(f"\nSample Matching Solicitations:")
    cursor.execute("""
        SELECT s.epin, s.procurement_name, c.ShortTitle, c.TypeOfNoticeDescription
        FROM solicitations s 
        INNER JOIN crol c ON UPPER(REPLACE(c.PIN, '-', '')) = s.normalized_epin
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  EPIN: {row[0]}")
        print(f"    Solicitation: {row[1][:50]}...")
        print(f"    CROL: {row[2]} ({row[3]})")
    
    # Sample CROL PINs
    print(f"\nSample CROL PINs:")
    cursor.execute("SELECT DISTINCT PIN FROM crol WHERE PIN != '' LIMIT 10")
    pins = [r[0] for r in cursor.fetchall()]
    print(f"  {pins}")
    
    # Sample Solicitation EPINs
    print(f"\nSample Solicitation EPINs:")
    cursor.execute("SELECT epin FROM solicitations LIMIT 10")
    epins = [r[0] for r in cursor.fetchall()]
    print(f"  {epins}")
    
    # Check for partial matches
    print(f"\nPartial Match Analysis:")
    cursor.execute("""
        SELECT COUNT(*) FROM crol 
        WHERE EXISTS (
            SELECT 1 FROM solicitations s 
            WHERE s.normalized_epin LIKE '%' || SUBSTR(UPPER(REPLACE(PIN, '-', '')), 1, 8) || '%'
        )
    """)
    partial = cursor.fetchone()[0]
    print(f"  CROL PINs with partial solicitation match: {partial}")


if __name__ == "__main__":
    conn = sqlite3.connect(DB_FILE)
    load_solicitations(conn)
    load_contracts(conn)
    test_matching(conn)
    conn.close()
