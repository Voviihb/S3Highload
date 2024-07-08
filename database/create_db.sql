CREATE TABLE Objects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    size BIGINT,
    hash VARCHAR(255),
    status VARCHAR(50)
);