USE belashovvi;
ADD FILE ./hive_ex4.sh;

SELECT TRANSFORM(ip, time_date, request, size, status, browser)
USING './hive_ex4.sh' AS (ip, time_date, request, size, status, browser)
FROM logs
LIMIT 10;
