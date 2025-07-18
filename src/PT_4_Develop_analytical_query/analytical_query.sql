------------------------------------------------
-- First analytic query
------------------------------------------------
-- SELECT continent_name, country_name, capital, population
-- FROM country AS m
-- LEFT JOIN continent AS n ON m.continent_id = n.continent_id 
-- ORDER BY population desc


------------------------------------------------
-- Second analytic query
------------------------------------------------
-- SELECT continent_name, country_name, capital, area
-- FROM country AS m
-- LEFT JOIN continent AS n ON m.continent_id = n.continent_id 
-- WHERE continent_name = 'Europe'
-- ORDER BY area desc


------------------------------------------------
-- Third analytic query
------------------------------------------------
WITH temp_1 AS( 
	SELECT continent_name, country_name, capital, area
	FROM country AS m
	LEFT JOIN continent AS n ON m.continent_id = n.continent_id 
	ORDER BY area DESC
),
temp_2 AS(
	SELECT continent_name, SUM(area) AS continent_area_sum
	FROM temp_1
	GROUP BY continent_name
	ORDER BY continent_area_sum DESC
) 
SELECT continent_name, continent_area_sum
FROM temp_2




