#!/usr/bin/env python3
"""
Add CROL table to existing databook.db without full rebuild.
Downloads from S3 and imports into SQLite.
"""

import sqlite3
import csv
import os

DB_FILE = "databook.db"
CROL_CSV = "crol_data.csv"
CROL_URL = "https://wegov-research-api.s3.amazonaws.com/crol"


def download_crol():
    """Download CROL data from S3 if not present."""
    if os.path.exists(CROL_CSV):
        print(f"{CROL_CSV} already exists, skipping download.")
        return
    
    import urllib.request
    print(f"Downloading CROL data from {CROL_URL}...")
    urllib.request.urlretrieve(CROL_URL, CROL_CSV)
    print("Download complete.")


def create_crol_table(conn):
    """Create CROL table and indexes."""
    cursor = conn.cursor()
    
    # Drop existing table if present
    cursor.execute("DROP TABLE IF EXISTS crol")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crol (
        RequestID TEXT PRIMARY KEY,
        StartDate TEXT,
        EndDate TEXT,
        AgencyName TEXT,
        TypeOfNoticeDescription TEXT,
        CategoryDescription TEXT,
        ShortTitle TEXT,
        SelectionMethodDescription TEXT,
        SectionName TEXT,
        SpecialCaseReasonDescription TEXT,
        PIN TEXT,
        DueDate TEXT,
        AddressToRequest TEXT,
        ContactName TEXT,
        ContactPhone TEXT,
        Email TEXT,
        ContractAmount TEXT,
        ContactFax TEXT,
        AdditionalDescription1 TEXT,
        AdditionalDescription2 TEXT,
        AdditionalDescription3 TEXT,
        OtherInfo1 TEXT,
        OtherInfo2 TEXT,
        OtherInfo3 TEXT,
        VendorName TEXT,
        VendorAddress TEXT,
        Printout1 TEXT,
        Printout2 TEXT,
        Printout3 TEXT,
        DocumentLinks TEXT,
        EventDate TEXT,
        EventBuildingName TEXT,
        EventStreetAddress1 TEXT,
        EventStreetAddress2 TEXT,
        EventCity TEXT,
        EventStateCode TEXT,
        EventZipCode TEXT,
        wegov_org_name TEXT,
        wegov_org_id TEXT
    )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crol_pin ON crol(PIN)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crol_agency ON crol(AgencyName)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crol_vendor ON crol(VendorName)")
    conn.commit()
    print("CROL table created.")


def load_crol(conn):
    """Load CROL data from CSV."""
    print("Loading CROL data...")
    cursor = conn.cursor()
    
    with open(CROL_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        count = 0
        
        for row in reader:
            # Skip records without PIN - we can't match them to solicitations/contracts
            pin = row.get('PIN', '').strip()
            if not pin:
                continue
            
            # Skip invalid PINs (text like "SEE BELOW", "NoPINFound", etc.)
            # Valid PINs should be alphanumeric and at least 8 characters
            if len(pin) < 8:
                continue
            if pin.upper() in ('NOPINFOUND', 'SEE BELOW', 'LINE 17 BELOW', 'SEE LINE 17 BELOW', 'LINE 17'):
                continue
            # Skip PINs that are all zeros or mostly zeros
            if pin.replace('0', '').replace('.', '') == '':
                continue
            # Skip PINs that start with common invalid patterns
            if pin.lower().startswith('see ') or pin.lower().startswith('line '):
                continue
                
            to_db.append((
                row.get('RequestID'),
                row.get('StartDate'),
                row.get('EndDate'),
                row.get('AgencyName'),
                row.get('TypeOfNoticeDescription'),
                row.get('CategoryDescription'),
                row.get('ShortTitle'),
                row.get('SelectionMethodDescription'),
                row.get('SectionName'),
                row.get('SpecialCaseReasonDescription'),
                row.get('PIN'),
                row.get('DueDate'),
                row.get('AddressToRequest'),
                row.get('ContactName'),
                row.get('ContactPhone'),
                row.get('Email'),
                row.get('ContractAmount'),
                row.get('ContactFax'),
                row.get('AdditionalDescription1'),
                row.get('AdditionalDesctription2'),  # Note: typo in source
                row.get('AdditionalDescription3'),
                row.get('OtherInfo1'),
                row.get('OtherInfo2'),
                row.get('OtherInfo3'),
                row.get('VendorName'),
                row.get('VendorAddress'),
                row.get('Printout1'),
                row.get('Printout2'),
                row.get('Printout3'),
                row.get('DocumentLinks'),
                row.get('EventDate'),
                row.get('EventBuildingName'),
                row.get('EventStreetAddress1'),
                row.get('EventStreetAddress2'),
                row.get('EventCity'),
                row.get('EventStateCode'),
                row.get('EventZipCode'),
                row.get('wegov-org-name'),
                row.get('wegov-org-id')
            ))
            
            count += 1
            if count % 100000 == 0:
                print(f"  Processed {count:,} records...")
        
        # Batch insert
        cursor.executemany("""
        INSERT OR IGNORE INTO crol VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, to_db)
        conn.commit()
        
    print(f"Loaded {len(to_db):,} CROL records.")


def verify(conn):
    """Verify CROL data was loaded."""
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM crol")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM crol WHERE PIN IS NOT NULL AND PIN != ''")
    with_pin = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT PIN) FROM crol WHERE PIN IS NOT NULL AND PIN != ''")
    unique_pins = cursor.fetchone()[0]
    
    print(f"\nVerification:")
    print(f"  Total CROL records: {total:,}")
    print(f"  Records with PIN: {with_pin:,}")
    print(f"  Unique PINs: {unique_pins:,}")
    
    # Check for matches with solicitations
    cursor.execute("""
        SELECT COUNT(DISTINCT s.epin) 
        FROM solicitations s 
        INNER JOIN crol c ON c.PIN = s.epin
    """)
    matched_solicitations = cursor.fetchone()[0]
    print(f"  Solicitations with CROL match: {matched_solicitations:,}")


if __name__ == "__main__":
    download_crol()
    
    conn = sqlite3.connect(DB_FILE)
    create_crol_table(conn)
    load_crol(conn)
    verify(conn)
    conn.close()
    
    print("\nCROL import complete!")
