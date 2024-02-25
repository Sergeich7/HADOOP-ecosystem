USE belashovvi;

SET hive.auto.convert.join = false;
set hive.exec.reducers.max = 6;
set mapreduce.job.reduces = 6;


select avg(j.f/j.c)*100
from (
  select 
    sum(if(gender='female', 1, 0)) f,
    count(1) c
  from logs tablesample (100 rows) l
    inner join users u on l.ip = u.ip
    inner join ipregions r on r.ip = l.ip
  group by r.region
) j
;