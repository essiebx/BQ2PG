# src/schema_mapper.py
"""
Schema mapping and query generation for patents data.
Date fields are in YYYYMMDD integer format.
"""

def generate_extraction_query(limit: int = None, year: int = None, recent_days: int = None):
    """
    Generate BigQuery SQL for extracting patents data.
    
    Args:
        limit: Maximum rows to extract
        year: Filter by filing year
        recent_days: Get patents from last N days
    """
    query = """
    SELECT
        -- Basic identifiers
        publication_number,
        application_number,
        country_code,
        kind_code,
        family_id,
        
        -- Extract English title
        COALESCE(
            (SELECT text 
             FROM UNNEST(title_localized) 
             WHERE language = 'en' 
             LIMIT 1),
            (SELECT text 
             FROM UNNEST(title_localized) 
             LIMIT 1),
            ''
        ) as title_english,
        
        -- Parse dates from YYYYMMDD format
        CASE 
            WHEN filing_date IS NOT NULL AND filing_date > 18000101 
            THEN PARSE_DATE('%Y%m%d', CAST(filing_date AS STRING))
            ELSE NULL 
        END as filing_date,
        
        CASE 
            WHEN publication_date IS NOT NULL AND publication_date > 18000101 
            THEN PARSE_DATE('%Y%m%d', CAST(publication_date AS STRING))
            ELSE NULL 
        END as publication_date,
        
        -- Inventor information
        ARRAY_LENGTH(inventor_harmonized) as inventor_count,
        ARRAY(
            SELECT name 
            FROM UNNEST(inventor_harmonized)
            WHERE name IS NOT NULL
        ) as inventor_names,
        
        -- Assignee information
        ARRAY_LENGTH(assignee_harmonized) as assignee_count,
        ARRAY(
            SELECT name 
            FROM UNNEST(assignee_harmonized)
            WHERE name IS NOT NULL
        ) as assignee_names,
        
        -- Classification codes
        ARRAY(
            SELECT DISTINCT code 
            FROM UNNEST(cpc) 
            WHERE code IS NOT NULL
            LIMIT 5
        ) as cpc_codes,
        
        -- Citation count
        ARRAY_LENGTH(citation) as citation_count,
        
        -- Other fields
        entity_status,
        art_unit
        
    FROM `patents-public-data.patents.publications`
    WHERE filing_date IS NOT NULL AND filing_date > 18000101
    """
    
    # Add filters
    if year:
        query += f" AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', CAST(filing_date AS STRING))) = {year}"
    
    if recent_days:
        query += f" AND PARSE_DATE('%Y%m%d', CAST(filing_date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {recent_days} DAY)"
    
    # Order and limit
    query += "\nORDER BY filing_date DESC"
    
    if limit:
        query += f"\nLIMIT {limit}"
    
    return query

def generate_create_table_sql(table_name: str = 'patents'):
    """Generate SQL to create patents table"""
    return f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    
    CREATE TABLE {table_name} (
        publication_number TEXT PRIMARY KEY,
        application_number TEXT,
        country_code VARCHAR(10),
        kind_code VARCHAR(10),
        family_id TEXT,
        title_english TEXT,
        filing_date DATE,
        publication_date DATE,
        inventor_count INTEGER,
        inventor_names TEXT[],
        assignee_count INTEGER,
        assignee_names TEXT[],
        cpc_codes TEXT[],
        citation_count INTEGER,
        entity_status TEXT,
        art_unit TEXT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes
    CREATE INDEX idx_{table_name}_filing_date ON {table_name}(filing_date);
    CREATE INDEX idx_{table_name}_country ON {table_name}(country_code);
    CREATE INDEX idx_{table_name}_inventor_names ON {table_name} USING GIN(inventor_names);
    CREATE INDEX idx_{table_name}_assignee_names ON {table_name} USING GIN(assignee_names);
    """