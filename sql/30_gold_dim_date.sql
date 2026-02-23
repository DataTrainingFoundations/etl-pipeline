CREATE TABLE gold.dim_date (
    date_key       INT PRIMARY KEY,
    full_date      DATE NOT NULL UNIQUE,
    year           INT,
    quarter        INT,
    month          INT,
    day            INT,
    is_weekend     BOOLEAN
);