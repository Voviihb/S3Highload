CREATE TABLE Objects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    size BIGINT,
    hash VARCHAR(255),
    status VARCHAR(50)
);