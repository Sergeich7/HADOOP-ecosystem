USE belashovvi;

SET hive.auto.convert.join = false;
set hive.exec.reducers.max = 6;
set mapreduce.job.reduces = 6;

-- Total MapReduce CPU Time Spent: 0 days 1 hours 1 minutes 39 seconds 130 msec
-- Time taken: 2242.879 seconds, Fetched: 82 row(s)
select 
  region,
  sum(if(gender='male', 1, 0)),
  sum(if(gender='female', 1, 0))
from ipregions r
  inner join users u on r.ip = u.ip
  inner join logs l on r.ip = l.ip
group by region
--order by region
;