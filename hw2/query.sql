--1a.
select highway, area from hw2.caltrans 
where condition like '%CLOSED%FOR THE WINTER%' or condition like '%CLOSED%DUE TO SNOW%' 
group by highway, area
order by highway, area desc 
limit 20;


---3a.
select 
	L.trip_id as trip_id,
	L.user_id as user_id, 
	--convert to trip_length seconds, if null end time, set total time to 24 hours (in seconds)
    coalesce(((DATE_PART('day', R.time - L.time) * 24 + 
                DATE_PART('hour', R.time - L.time)) * 60 +
                DATE_PART('minute', R.time - L.time)) * 60 +
                DATE_PART('second', R.time - L.time), 86400) as trip_length

from
	hw2.trip_start L
left join 
	hw2.trip_end R
on 
	L.trip_id = R.trip_id AND
	L.user_id = R.user_id
limit
	5;



--3b.
select t.trip_id, t.user_id, t.trip_length, ((ceiling(t.trip_length/60))*0.15) + 1 as trip_charge

from
( 
	select 
		L.trip_id as trip_id,
		L.user_id as user_id, 

		--convert to trip_length seconds, if null end time, set total time to 24 hours (in seconds)
	    coalesce(((DATE_PART('day', R.time - L.time) * 24 + 
	                DATE_PART('hour', R.time - L.time)) * 60 +
	                DATE_PART('minute', R.time - L.time)) * 60 +
	                DATE_PART('second', R.time - L.time), 86400) as trip_length
	from
		hw2.trip_start L
	left join 
		hw2.trip_end R
	on 
		L.trip_id = R.trip_id AND
		L.user_id = R.user_id
) t
limit 5;


--3c. 
select t2.user_id, sum(t2.trip_charge) as monthly_total
from
(
	select t1.trip_id, t1.user_id, t1.trip_length, ((ceiling(t1.trip_length/60))*0.15) + 1 as trip_charge, t1.time_init
		from
		( 
			select 
				L.trip_id as trip_id,
				L.user_id as user_id, 
				L.time as time_init,
				--convert to trip_length seconds, if null end time, set total time to 24 hours (in seconds)
			    coalesce(((DATE_PART('day', R.time - L.time) * 24 + 
			                DATE_PART('hour', R.time - L.time)) * 60 +
			                DATE_PART('minute', R.time - L.time)) * 60 +
			                DATE_PART('second', R.time - L.time), 86400) as trip_length
			from
				hw2.trip_start L
			left join 
				hw2.trip_end R
			on 
				L.trip_id = R.trip_id AND
				L.user_id = R.user_id
		) t1
)t2

where 
	t2.user_id=2 and extract(month from t2.time_init)=3
group by t2.user_id
limit 5;




