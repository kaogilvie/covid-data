CREATE SCHEMA nytimes;

CREATE TABLE nytimes.cases_by_county
	 (id SERIAL PRIMARY KEY,
	  date DATE,
	  county varchar(100),
	  state varchar(55),
	  fips int,
	  cases int,
	  deaths int
	  );

CREATE INDEX cbc_date_idx ON nytimes.cases_by_county (date);
CREATE INDEX cbc_county_idx ON nytimes.cases_by_county (lower(county));
CREATE INDEX cbc_state_idx ON nytimes.cases_by_county (lower(state));
CREATE INDEX cbc_fips_idx ON nytimes.cases_by_county (fips);

CREATE TABLE nytimes.cases_by_state
	 (id SERIAL PRIMARY KEY,
	  date DATE,
	  state varchar(55),
	  fips int,
	  cases int,
	  deaths int
	  );

CREATE INDEX cbs_date_idx ON nytimes.cases_by_county (date);
CREATE INDEX cbs_county_idx ON nytimes.cases_by_county (lower(county));
CREATE INDEX cbs_state_idx ON nytimes.cases_by_county (lower(state));
CREATE INDEX cbs_fips_idx ON nytimes.cases_by_county (fips);
