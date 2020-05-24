CREATE SCHEMA aux;

CREATE TABLE aux.fips_to_latlng
	 ( d SERIAL PRIMARY KEY,
    state_abbreviation text,
    fips integer,
    ansicode bigint,
    name text,
    aland bigint,
    awater bigint,
    aland_sqmi double precision,
    awater_sqmi double precision,
    lat double precision,
    lng double precision
	  );

CREATE UNIQUE INDEX fips_to_latlng_pkey ON aux.fips_to_latlng(id int4_ops);
CREATE INDEX state_abbreviation_fips_to_latlng_idx ON nytiauxes.fips_to_latlng(state_abbreviation text_ops);
CREATE INDEX fips_fips_to_latlng_idx ON aux.fips_to_latlng(fips int4_ops);
