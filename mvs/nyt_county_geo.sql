drop table if exists nytimes.cases_county_geo;
create table nytimes.cases_county_geo AS
	(SELECT a.date, a.county, a.state, a.cases, a.deaths,
	b.fips, b.ansicode, b.lat, b.lng, b.name
	FROM nytimes.cases_by_county a
	JOIN aux.fips_to_latlng b
	ON a.fips = b.fips
	);
