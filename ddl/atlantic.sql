CREATE SCHEMA atlantic;

CREATE TABLE atlantic.daily_state
	 (id SERIAL PRIMARY KEY,
	  date DATE,
	  state varchar(55),
	  cases int,
	  deaths int
	  );

CREATE INDEX ds_atl_date_idx ON atlantic.daily_state (date);
CREATE INDEX ds_atl_state_idx ON atlantic.daily_state (lower(state));
