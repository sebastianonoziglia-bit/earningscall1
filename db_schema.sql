CREATE TABLE transcripts (
        id INTEGER PRIMARY KEY,
        company TEXT NOT NULL,
        year INTEGER NOT NULL,
        quarter TEXT NOT NULL,
        full_text TEXT,
        word_count INTEGER,
        indexed_date TIMESTAMP,
        UNIQUE(company, year, quarter)
    );
CREATE TABLE transcript_topics (
        id INTEGER PRIMARY KEY,
        transcript_id INTEGER,
        topic TEXT NOT NULL,
        keyword TEXT,
        mention_count INTEGER,
        context_snippet TEXT,
        speaker TEXT,
        FOREIGN KEY (transcript_id) REFERENCES transcripts(id)
    );
CREATE TABLE transcript_kpis (
        id INTEGER PRIMARY KEY,
        transcript_id INTEGER,
        kpi_type TEXT NOT NULL,
        value_text TEXT,
        value_numeric REAL,
        unit TEXT,
        context_sentence TEXT,
        confidence REAL,
        FOREIGN KEY (transcript_id) REFERENCES transcripts(id)
    );
CREATE TABLE transcript_highlights (
        id INTEGER PRIMARY KEY,
        transcript_id INTEGER,
        highlight_type TEXT,
        speaker TEXT,
        text TEXT,
        relevance_score REAL,
        FOREIGN KEY (transcript_id) REFERENCES transcripts(id)
    );
CREATE TABLE company_metrics (
        id INTEGER PRIMARY KEY,
        company TEXT NOT NULL,
        ticker TEXT,
        year INTEGER,
        quarter TEXT,
        revenue REAL,
        cost_of_revenue REAL,
        operating_income REAL,
        net_income REAL,
        capex REAL,
        r_and_d REAL,
        total_assets REAL,
        market_cap REAL,
        cash_balance REAL,
        debt REAL, employee_count REAL, advertising_revenue REAL,
        UNIQUE(company, year, quarter)
    );
CREATE INDEX idx_topics_company ON transcript_topics(topic);
CREATE INDEX idx_kpis_type ON transcript_kpis(kpi_type);
CREATE INDEX idx_transcripts_company_year ON transcripts(company, year);
