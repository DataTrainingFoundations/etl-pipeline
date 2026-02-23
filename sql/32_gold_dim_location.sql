CREATE TABLE gold.dim_location (
    location_key   BIGSERIAL PRIMARY KEY,
    state          CHAR(2),
    county         TEXT,
    city           TEXT,
    geom           GEOMETRY(Point, 4326)
);