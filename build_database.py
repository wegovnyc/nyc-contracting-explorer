import sqlite3
import csv
import re
import os
import time

DB_FILE = "databook.db"

def init_db(conn):
    cursor = conn.cursor()
    
    # 1. Vendors
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agencies (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    # 1. Vendors
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendors (
        passport_supplier_id TEXT PRIMARY KEY,
        name TEXT,
        fms_vendor_code TEXT,
        duns_number TEXT,
        certification_type TEXT,
        ethnicity TEXT,
        business_category TEXT,
        corporate_structure TEXT
    )
    """)
    
    # 2. Solicitations
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
    
    # 3. Contracts
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
    
    # 4. Doing Business Entities (formerly mocs_entities)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mocs_entities (
        organization_name TEXT,
        ownership_structure_code TEXT,
        organization_phone TEXT,
        start_date TEXT,
        normalized_name TEXT,
        matched_vendor_id TEXT,
        match_score REAL
    )
    """)
    
    # 5. Doing Business People (formerly mocs_people)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mocs_people (
        mocs_peopleid TEXT,
        organization_name TEXT,
        first_name TEXT,
        last_name TEXT,
        relationship_code TEXT,
        normalized_org_name TEXT
    )
    """)
    
    # 6. Entity Summary
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_entity_summary (
        vendor_name TEXT,
        address1 TEXT,
        address2 TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        country TEXT,
        telephone TEXT,
        symbol TEXT,
        for_profit TEXT,
        duns TEXT,
        revenue TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ves_name ON vendor_entity_summary(vendor_name)")
    
    # 7. Other Names
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_other_names (
        vendor_name TEXT,
        type TEXT,
        other_name TEXT,
        from_date TEXT,
        to_date TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_von_name ON vendor_other_names(vendor_name)")
    
    # 8. Evaluations
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_evaluations (
        vendor_name TEXT,
        agency TEXT,
        contract_id TEXT,
        purpose TEXT,
        eval_date TEXT,
        start_date TEXT,
        end_date TEXT,
        rating TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ve_name ON vendor_evaluations(vendor_name)")
    
    # 9. Principals
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_principals (
        vendor_name TEXT,
        principal_name TEXT,
        title TEXT,
        ownership_type TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_name ON vendor_principals(vendor_name)")
    
    # 10. Related Entities
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_related_entities (
        vendor_name TEXT,
        related_entity_name TEXT,
        address1 TEXT,
        address2 TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        country TEXT,
        telephone TEXT,
        relationship TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vre_name ON vendor_related_entities(vendor_name)")
    
    # 11. OpenCorporates Matches
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS opencorporates_matches (
        passport_vendor_id TEXT PRIMARY KEY,
        vendor_name TEXT,
        opencorporates_url TEXT,
        company_number TEXT,
        jurisdiction_code TEXT,
        incorporation_date TEXT,
        registered_address TEXT,
        status TEXT,
        match_type TEXT,
        last_updated_at TEXT,
        raw_data TEXT,
        FOREIGN KEY (passport_vendor_id) REFERENCES vendors (passport_supplier_id)
    )
    """)

    # Opencorporates Failures
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS opencorporates_failures (
            passport_vendor_id TEXT PRIMARY KEY,
            vendor_name TEXT,
            attempted_at TEXT,
            failure_reason TEXT
        )
    """)
    
    # 12. City Record Online (CROL)
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

def normalize_contract_id(cid):
    if not cid: return None
    # Remove hyphens, spaces, and make uppercase
    return re.sub(r'[^A-Z0-9]', '', cid.upper())

def normalize_epin(epin):
    if not epin: return None
    return re.sub(r'[^A-Z0-9]', '', epin.upper())

def clean_money(val):
    if not val: return 0.0
    cleaned = re.sub(r'[^0-9.]', '', val)
    if not cleaned: return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def clean_name(name):
    if not name: return ""
    # Uppercase, remove special chars, extra spaces
    return re.sub(r'[^A-Z0-9]', '', name.upper())

def load_vendors(conn):
    print("Loading Vendors...")
    with open("vendor_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            to_db.append((
                row.get("PASSPort Supplier-ID"),
                row.get("Vendor Name"),
                row.get("FMS Vendor Code"),
                row.get("DUNS Number"),
                row.get("Certification Type"),
                row.get("Ethnicity"),
                row.get("Business Category"),
                row.get("Corporate Structure")
            ))
        
        cursor = conn.cursor()
        cursor.executemany("""
        INSERT OR IGNORE INTO vendors 
        (passport_supplier_id, name, fms_vendor_code, duns_number, certification_type, ethnicity, business_category, corporate_structure)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, to_db)
        conn.commit()
    print(f"Loaded {len(to_db)} vendors.")

def load_solicitations(conn):
    print("Loading Solicitations...")
    with open("solicitations_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            epin = row.get("EPIN")
            agency_id = row.get("wegov-org-id")
            agency_name = row.get("Agency")
            
            # Populate Agencies on the fly
            if agency_id and agency_name:
                cursor = conn.cursor()
                cursor.execute("INSERT OR IGNORE INTO agencies (id, name) VALUES (?, ?)", (agency_id, agency_name))
            
            to_db.append((
                row.get("RFP-ID"),
                row.get("BPM-ID"),
                row.get("Program"),
                row.get("Industry"),
                epin,
                row.get("Procurement Name"),
                agency_name,
                agency_id,
                row.get("RFx Status"),
                row.get("Release Date"),
                row.get("Due Date"),
                row.get("Main Commodity"),
                row.get("Procurement Method"),
                normalize_epin(epin)
            ))
            
        cursor = conn.cursor()
        cursor.executemany("""
        INSERT OR IGNORE INTO solicitations
        (rfp_id, bpm_id, program, industry, epin, procurement_name, agency, agency_id, rfx_status, release_date, due_date, main_commodity, procurement_method, normalized_epin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, to_db)
        conn.commit()
    print(f"Loaded {len(to_db)} solicitations.")

def load_contracts(conn):
    print("Loading Contracts...")
    with open("contracts_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            cid = row.get("Contract ID")
            epin = row.get("EPIN")
            agency_id = row.get("wegov-org-id")
            agency_name = row.get("Agency")
            
            # Populate Agencies on the fly
            if agency_id and agency_name:
                cursor = conn.cursor()
                cursor.execute("INSERT OR IGNORE INTO agencies (id, name) VALUES (?, ?)", (agency_id, agency_name))
            
            to_db.append((
                row.get("CTR-ID"),
                epin,
                cid,
                row.get("Contract Title"),
                agency_name,
                agency_id,
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
            
        cursor = conn.cursor()
        cursor.executemany("""
        INSERT OR IGNORE INTO contracts
        (ctr_id, epin, contract_id, contract_title, agency, agency_id, vendor_name, program, procurement_method, contract_type, status, award_amount, current_amount, start_date, end_date, industry, normalized_contract_id, normalized_epin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, to_db)
        conn.commit()
    print(f"Loaded {len(to_db)} contracts.")

def load_doing_business(conn):
    print("Loading Doing Business Entities...")
    cursor = conn.cursor()
    with open("doing_business_entities.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            name = row.get("organization_name")
            to_db.append((
                name,
                row.get("ownership_structure_code"),
                row.get("organization_phone"),
                row.get("doing_business_start_date"),
                clean_name(name)
            ))
        
        cursor.executemany("""
        INSERT INTO mocs_entities (organization_name, ownership_structure_code, organization_phone, start_date, normalized_name)
        VALUES (?, ?, ?, ?, ?)
        """, to_db)
    print(f"Loaded {len(to_db)} entities.")

    print("Loading Doing Business People...")
    with open("doing_business_people.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        for row in reader:
            org_name = row.get("organization_name")
            to_db.append((
                row.get("mocs_peopleid"),
                org_name,
                row.get("person_name_first"),
                row.get("person_name_last"),
                row.get("relationship_type_code"),
                clean_name(org_name)
            ))
            
        cursor.executemany("""
        INSERT INTO mocs_people (mocs_peopleid, organization_name, first_name, last_name, relationship_code, normalized_org_name)
        VALUES (?, ?, ?, ?, ?, ?)
        """, to_db)
    print(f"Loaded {len(to_db)} people.")
    conn.commit()

def load_new_vendor_data(conn):
    cursor = conn.cursor()
    
    # 1. Entity Summary (passport_entity_summary.csv)
    if os.path.exists("passport_entity_summary.csv"):
        print("Loading Entity Summary...")
        with open("passport_entity_summary.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            to_db = []
            for row in reader:
                 to_db.append((
                     row.get('Vendor Name'), row.get('Address Line 1'), row.get('Address  Line 2'),
                     row.get('City'), row.get('State'), row.get('Zip Code'), row.get('Country'),
                     row.get('Telephone'), row.get('Stock Exchange Symbol'), row.get('For Profit'),
                     row.get('DUNS number'), row.get('Gross Revenue')
                 ))
            cursor.executemany("INSERT INTO vendor_entity_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", to_db)
            print(f"Loaded {len(to_db)} entity summary records.")

    # 2. Other Names (passport_other_names.csv)
    if os.path.exists("passport_other_names.csv"):
        print("Loading Other Names...")
        with open("passport_other_names.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            to_db = []
            for row in reader:
                 to_db.append((
                     row.get('Vendor Name'), row.get('Other Name Type'), row.get('Other Name'),
                     row.get('From Date'), row.get('To Date ')
                 ))
            cursor.executemany("INSERT INTO vendor_other_names VALUES (?,?,?,?,?)", to_db)
            print(f"Loaded {len(to_db)} other names.")
            
    # 3. Evaluations (passport_performance_evaluation.csv)
    if os.path.exists("passport_performance_evaluation.csv"):
        print("Loading Evaluations...")
        with open("passport_performance_evaluation.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            to_db = []
            for row in reader:
                 to_db.append((
                     row.get('Vendor Name'), row.get('Agency'), row.get('Contract  ID'),
                     row.get('Purpose'), row.get('Evaluation Date'), row.get('Evaluation Period Start Date'),
                     row.get('Evaluation Period End Date'), row.get('Overall Rating')
                 ))
            cursor.executemany("INSERT INTO vendor_evaluations VALUES (?,?,?,?,?,?,?,?)", to_db)
            print(f"Loaded {len(to_db)} evaluation records.")
            
    # 4. Principals (passport_principals.csv)
    if os.path.exists("passport_principals.csv"):
        print("Loading Principals...")
        with open("passport_principals.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            to_db = []
            for row in reader:
                 to_db.append((
                     row.get('Vendor Name'), row.get('Principal Name'), row.get('Current Title'),
                     row.get('Principal Ownership Type')
                 ))
            cursor.executemany("INSERT INTO vendor_principals VALUES (?,?,?,?)", to_db)
            print(f"Loaded {len(to_db)} principal records.")

    # 5. Related Entities (passport_related_entities.csv)
    if os.path.exists("passport_related_entities.csv"):
        print("Loading Related Entities...")
        with open("passport_related_entities.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            to_db = []
            for row in reader:
                 to_db.append((
                     row.get('Vendor Name'), row.get('Related Entity Name'), row.get('Address Line 1'),
                     row.get('Address Line 2'), row.get('City'), row.get('State'), row.get('Zip Code'),
                     row.get('Country'), row.get('Telephone'), row.get('Relationship to Vendor')
                 ))
            cursor.executemany("INSERT INTO vendor_related_entities VALUES (?,?,?,?,?,?,?,?,?,?)", to_db)
            print(f"Loaded {len(to_db)} related entities.")
    conn.commit()

def match_entities_to_vendors(conn):
    print("Matching Entities to Vendors...")
    cursor = conn.cursor()
    
    # Fetch all Vendors
    cursor.execute("SELECT passport_supplier_id, name FROM vendors")
    vendors = cursor.fetchall()
    # Map normalized name -> ID
    vendor_map = {clean_name(v[1]): v[0] for v in vendors}
    
    # Fetch MOCS Entities that need matching
    cursor.execute("SELECT rowid, organization_name, normalized_name FROM mocs_entities")
    mocs_rows = cursor.fetchall()
    
    updates = []
    
    count = 0
    for row in mocs_rows:
        rid, org_name, norm_name = row
        
        # Exact Normalized Match
        if norm_name in vendor_map:
            updates.append((vendor_map[norm_name], 1.0, rid))
            
        count += 1
        if count % 10000 == 0:
            print(f"Processed match for {count}...")
            
    print(f"Updating {len(updates)} matches...")
    cursor.executemany("UPDATE mocs_entities SET matched_vendor_id = ?, match_score = ? WHERE rowid = ?", updates)
    conn.commit()

def load_crol(conn):
    """Load City Record Online (CROL) data from CSV."""
    if not os.path.exists("crol_data.csv"):
        print("CROL data file not found, skipping...")
        return
        
    print("Loading CROL data...")
    cursor = conn.cursor()
    
    with open("crol_data.csv", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        to_db = []
        count = 0
        
        for row in reader:
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
                row.get('AdditionalDesctription2'),  # Note: typo in source data
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
                print(f"  Processed {count:,} CROL records...")
                
        cursor.executemany("""
        INSERT OR IGNORE INTO crol VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, to_db)
        conn.commit()
        
    print(f"Loaded {len(to_db):,} CROL records.")


def create_indices(conn):
    print("Creating indices...")
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contracts_norm_id ON contracts(normalized_contract_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contracts_norm_epin ON contracts(normalized_epin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_solicit_norm_epin ON solicitations(normalized_epin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contracts_vendor_name ON contracts(vendor_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name)")
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mocs_ent_match ON mocs_entities(matched_vendor_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mocs_ppl_norm ON mocs_people(normalized_org_name)")
    conn.commit()

if __name__ == "__main__":
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    
    load_vendors(conn)
    load_solicitations(conn)
    load_contracts(conn)
    load_doing_business(conn)
    load_new_vendor_data(conn)
    load_crol(conn)
    match_entities_to_vendors(conn)
    
    create_indices(conn)
    
    # Simple Verification
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM mocs_entities WHERE matched_vendor_id IS NOT NULL")
    matches = cursor.fetchone()[0]
    print(f"Verification: Found {matches} matched doing business entities.")
    
    conn.close()
    print(f"Database built: {DB_FILE}")
