--ERROR queries
SELECT COUNT(DISTINCT(ts_id)) from timesheet_logs
WHERE username IN ('bwalzer@capstoneemail.com','mwright2013@yahoo.com')
AND message LIKE ('%Changed clock-in%')
OR message LIKE ('%Total time worked changed%')
OR message LIKE ('%Changed clock-out%')
;

SELECT CAST(duration/3600 as FLOAT) as hours,
CASE WHEN timesheets.sheet_id IN
(SELECT ts_id from timesheet_logs
WHERE username IN ('bwalzer@capstoneemail.com','mwright2013@yahoo.com')
AND message LIKE ('%Changed clock-in%')
OR message LIKE ('%Total time worked changed%')
OR message LIKE ('%Changed clock-out%')) THEN 1
ELSE 0
END as error
FROM timesheets;





--Geoqueries
--Function
CREATE OR REPLACE FUNCTION distance(lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT) RETURNS FLOAT AS $$
DECLARE
   x float = 69.1 * (lat2 - lat1);
   y float = 69.1 * (lon2 - lon1) * cos(lat1 / 57.3);
BEGIN
   RETURN sqrt(x * x + y * y);
END
$$ LANGUAGE plpgsql;


--example queries for getting distance traveled for a specific employee

SELECT *, lead(latitude,1) over (order by created) as difflat, lead(longitude,1)
over (order by created)
as difflon,
distance(latitude, longitude, (lead(latitude,1) over (order by created)),
(lead(longitude,1) over (order by created)))
AS dist_traveled
FROM geo where employee_id=404389 LIMIT 10;


SELECT *, lead(latitude,1) over (order by created) as diff
FROM geo where employee_id=404389;

SELECT * from geo
WHERE employee_id=404389
order by created;
