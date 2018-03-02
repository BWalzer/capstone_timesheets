--Function
CREATE OR REPLACE FUNCTION distance(lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT) RETURNS FLOAT AS $$
DECLARE
   x float = 69.1 * (lat2 - lat1);
   y float = 69.1 * (lon2 - lon1) * cos(lat1 / 57.3);
BEGIN
   RETURN sqrt(x * x + y * y);
END
$$ LANGUAGE plpgsql;


--example queries for getting distance traveled for a specific

SELECT distance(x.latitude, x.longitude, y.latitude, y.longitude)
FROM (select latitude, longitude from geo
WHERE employee_id=404389 AND created in ('2016-05-16 14:30:30')) as x,
(SELECT latitude, longitude from geo
WHERE employee_id=404389 AND created in ('2016-05-17 22:08:06')) as y;

SELECT *, lead(latitude,1) over (order by created) as difflat, lead(longitude,1) over (order by created)
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
