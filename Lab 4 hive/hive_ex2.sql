USE belashovvi;

select time_date, count(1) as c from logs group by time_date order by c desc;
