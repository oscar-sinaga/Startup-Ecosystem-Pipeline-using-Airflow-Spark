
-- =========================
-- DIMENSION TABLES
-- =========================

-- Extension for UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE dim_date (
    date_id int4 NOT NULL,
    date_actual date NOT NULL,
    day_suffix varchar(4) NOT NULL,
    day_name varchar(9) NOT NULL,
    day_of_year int4 NOT NULL,
    week_of_month int4 NOT NULL,
    week_of_year int4 NOT NULL,
    week_of_year_iso bpchar(10) NOT NULL,
    month_actual int4 NOT NULL,
    month_name varchar(9) NOT NULL,
    month_name_abbreviated bpchar(3) NOT NULL,
    quarter_actual int4 NOT NULL,
    quarter_name varchar(9) NOT NULL,
    year_actual int4 NOT NULL,
    first_day_of_week date NOT NULL,
    last_day_of_week date NOT NULL,
    first_day_of_month date NOT NULL,
    last_day_of_month date NOT NULL,
    first_day_of_quarter date NOT NULL,
    last_day_of_quarter date NOT NULL,
    first_day_of_year date NOT NULL,
    last_day_of_year date NOT NULL,
    mmyyyy bpchar(6) NOT NULL,
    mmddyyyy bpchar(10) NOT NULL,
    weekend_indr varchar(20) NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);


CREATE TABLE dim_company (
    company_id BIGSERIAL PRIMARY KEY ,
    company_nk VARCHAR(255) UNIQUE,
    description TEXT,
    region VARCHAR(255),
    city VARCHAR(255),
    state_code VARCHAR(255),
    country_code VARCHAR(255),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE dim_people (
    people_id BIGSERIAL PRIMARY KEY ,
    people_nk VARCHAR(255) UNIQUE,
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    affiliation VARCHAR(255),
    birthplace VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE fact_investment_round_participation (
    investment_round_participation_id BIGSERIAL PRIMARY KEY,
    investment_nk BIGINT,
    funding_round_nk BIGINT,
    investee_company_id BIGINT,
    investor_company_id BIGINT,
    funded_at INTEGER,
    funding_round_type VARCHAR(255),
    funding_round_code VARCHAR(255),
    raised_amount NUMERIC(15,2),
    pre_money_valuation NUMERIC(15,2),
    post_money_valuation NUMERIC(15,2),
    number_of_participants NUMERIC(15,2),
    round_position_desc VARCHAR(50),
    round_stage_desc VARCHAR(50),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_investment_funding UNIQUE (investment_nk, funding_round_nk),    -- ✅ UNIQUE constraint gabungan dari investment_nk dan funding_round_nk
    FOREIGN KEY (funded_at) REFERENCES dim_date(date_id),
    FOREIGN KEY (investor_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (investee_company_id) REFERENCES dim_company(company_id)
);

CREATE TABLE fact_acquisition (
    acquisition_id BIGSERIAL PRIMARY KEY,
    acquisition_nk BIGINT UNIQUE,
    acquiring_company_id BIGINT,
    acquired_company_id BIGINT,
    acquired_at INTEGER,
    price_amount NUMERIC,
    price_currency_code VARCHAR(255),
    term_code VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (acquiring_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (acquired_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (acquired_at) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_ipos (
    ipo_id BIGSERIAL PRIMARY KEY,
    ipo_nk BIGINT UNIQUE,
    company_id BIGINT,
    public_at INTEGER,
    valuation_amount NUMERIC,
    raised_amount NUMERIC,
    valuation_currency_code VARCHAR(255),
    raised_currency_code VARCHAR(255),
    stock_symbol VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,    
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (public_at) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_funds (
    fund_id BIGSERIAL PRIMARY KEY,
    fund_nk BIGINT UNIQUE,
    company_id BIGINT,
    funded_at INTEGER,
    fund_name VARCHAR(255),
    raised_amount NUMERIC,
    raised_currency_code VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (funded_at) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_milestones (
    milestone_id BIGSERIAL PRIMARY KEY,
    milestone_nk BIGINT UNIQUE,
    company_id BIGINT,
    milestone_at INTEGER,
    description TEXT,
    milestone_code VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (milestone_at) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_relationships (
    relationship_id BIGSERIAL PRIMARY KEY ,
    relationship_nk BIGINT UNIQUE,
    people_id BIGINT,
    company_id BIGINT,
    title TEXT,
    start_at INTEGER,
    end_at INTEGER,
    relationship_status VARCHAR(255),
    relationship_order INT,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (people_id) REFERENCES dim_people(people_id),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (start_at) REFERENCES dim_date(date_id),
    FOREIGN KEY (end_at) REFERENCES dim_date(date_id)

);

INSERT INTO dim_date
SELECT
    EXTRACT(YEAR FROM d)::INT * 10000 + EXTRACT(MONTH FROM d)::INT * 100 + EXTRACT(DAY FROM d)::INT AS date_id,
    d AS date_actual,
    TO_CHAR(d, 'FMDDth') AS day_suffix,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    CEIL(EXTRACT(DAY FROM d) / 7.0)::INT AS week_of_month,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    TO_CHAR(d, 'IYYY-IW') AS week_of_year_iso,
    EXTRACT(MONTH FROM d)::INT AS month_actual,
    TO_CHAR(d, 'Month') AS month_name,
    TO_CHAR(d, 'Mon') AS month_name_abbreviated,
    EXTRACT(QUARTER FROM d)::INT AS quarter_actual,
    CASE EXTRACT(QUARTER FROM d)
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
        WHEN 4 THEN 'Fourth'
    END AS quarter_name,
    EXTRACT(YEAR FROM d)::INT AS year_actual,
    date_trunc('week', d)::DATE AS first_day_of_week,
    (date_trunc('week', d)::DATE + INTERVAL '6 day')::DATE AS last_day_of_week,
    date_trunc('month', d)::DATE AS first_day_of_month,
    (date_trunc('month', d) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
    date_trunc('quarter', d)::DATE AS first_day_of_quarter,
    (date_trunc('quarter', d) + INTERVAL '3 month - 1 day')::DATE AS last_day_of_quarter,
    date_trunc('year', d)::DATE AS first_day_of_year,
    (date_trunc('year', d) + INTERVAL '1 year - 1 day')::DATE AS last_day_of_year,
    TO_CHAR(d, 'MMYYYY')::bpchar AS mmyyyy,
    TO_CHAR(d, 'MM/DD/YYYY')::bpchar AS mmddyyyy,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekend_indr
FROM generate_series(
    DATE '1900-01-01',
    DATE '2100-12-31',
    INTERVAL '1 day'
) AS g(d);


-- -- deprecated
-- CREATE TABLE dim_relationships (
--     relationship_id BIGSERIAL PRIMARY KEY ,
--     relationship_nk BIGINT UNIQUE,
--     people_id BIGINT,
--     company_id BIGINT,
--     title TEXT,
--     start_at INTEGER,
--     end_at INTEGER,
--     relationship_status VARCHAR(255),
--     relationship_order INT,
--     created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (people_id) REFERENCES dim_people(people_id),
--     FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
--     FOREIGN KEY (start_at) REFERENCES dim_date(date_id),
--     FOREIGN KEY (end_at) REFERENCES dim_date(date_id)

-- );

-- CREATE TABLE fact_funding_rounds (
--     funding_round_id BIGSERIAL PRIMARY KEY,
--     funding_round_nk BIGINT UNIQUE,
--     funded_at INTEGER,
--     company_id BIGINT,  -- ✅ tipe disamakan dengan dim_company
--     funding_round_type VARCHAR(255),
--     funding_round_code VARCHAR(255),
--     raised_amount_usd NUMERIC(15,2),
--     pre_money_valuation_usd NUMERIC(15,2),
--     post_money_valuation_usd NUMERIC(15,2),
--     round_position_desc VARCHAR(50),
--     round_stage_desc VARCHAR(50),
--     created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (funded_at) REFERENCES dim_date(date_id),
--     FOREIGN KEY (company_id) REFERENCES dim_company(company_id)
-- );

-- CREATE TABLE fact_investments (
--     investment_id BIGSERIAL PRIMARY KEY,
--     investment_nk BIGINT UNIQUE,
--     funding_round_id BIGINT,
--     investor_company_id BIGINT,
--     investee_company_id BIGINT,
--     created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (funding_round_id) REFERENCES fact_funding_rounds(funding_round_id),
--     FOREIGN KEY (investor_company_id) REFERENCES dim_company(company_id),
--     FOREIGN KEY (investee_company_id) REFERENCES dim_company(company_id)
-- );