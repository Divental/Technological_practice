CREATE TABLE continent (
    continent_id SERIAL PRIMARY KEY,
    continent_name VARCHAR(200) NOT NULL
);

CREATE TABLE country (
    country_id SERIAL PRIMARY KEY,
    continent_id INTEGER NOT NULL,
    country_name VARCHAR(200) NOT NULL,
    capital VARCHAR(200) NOT NULL,
    area INTEGER NOT NULL,
    population INTEGER NOT NULL,
    FOREIGN KEY (continent_id) REFERENCES continent(continent_id)
        ON DELETE CASCADE
);

CREATE TABLE city (
    city_id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL,
    city_name VARCHAR(200) NOT NULL,
    FOREIGN KEY (country_id) REFERENCES country(country_id)
        ON DELETE CASCADE
);