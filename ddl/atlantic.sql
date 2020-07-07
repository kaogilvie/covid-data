CREATE SCHEMA atlantic;

CREATE TABLE atlantic.daily_state
	 (id SERIAL PRIMARY KEY,
	  date DATE,
	  state varchar(55),
		fips varchar(20),
		lastUpdateEt timestamp,
		dataQualityGrade varchar(10),
		positive int,
		negative int,
		pending int,
		recovered int,
		death int,
		hospitalizedCurrently int,
		hospitalizedCumulative int,
		inIcuCurrently int,
		inIcuCumulative int,
		onVentilatorCurrently int,
		onVentilatorCumulative int,
		negativeTestsViral int,
		positiveTestsViral int,
		positiveCasesViral int,
		totalTestsViral int,
		totalTestResults int,
		positiveIncrease int,
		deathIncrease int,
		hopsitalizedIncrease int,
		totalTestResultsIncrease int
	  );

CREATE INDEX ds_atl_date_idx ON atlantic.daily_state (date);
CREATE INDEX ds_atl_state_idx ON atlantic.daily_state (lower(state));
CREATE INDEX ds_atl_fips_idx ON atlantic.daily_state (fips);
