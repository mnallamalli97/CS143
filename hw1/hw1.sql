
select 
	extract(hour from datetime) AS hour, SUM(throughput) AS trips 
from
	hw1.rides2017 
GROUP BY
	hour;



select 
	origin, destination, SUM(throughput) as sum 
FROM 
	hw1.rides2017 
where 
	extract(DOW from datetime) >= 1 AND extract(DOW from datetime) <= 5 
GROUP BY 
	origin, destination 
ORDER BY 
	sum DESC limit 1;



select 
	destination, AVG(throughput) AS avg 
from 
	hw1.rides2017 
where 
	extract(dow from datetime)=1 AND extract(hour from datetime)>=7 AND extract(hour from datetime)<10 
GROUP BY 
	destination 
ORDER BY avg DESC limit 5;


