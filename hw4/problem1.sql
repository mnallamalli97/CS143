-- 1a.
-- 0
EXPLAIN SELECT
highway,
area,
COUNT(DISTINCT EXTRACT(DOY FROM reported)) * 100 / 365 AS percentage_of_days_closed_365,
COUNT(DISTINCT EXTRACT(DOY FROM reported)) * 100 / 353 AS percentage_of_days_closed_353
FROM hw2.caltrans
WHERE condition LIKE '%CLOSED%DUE TO SNOW%' OR condition LIKE '%CLOSED%FOR THE WINTER%'
GROUP BY highway, area
ORDER BY percentage_of_days_closed_365 DESC;


QUERY PLAN                                                          
-----------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=4523.29..4527.33 rows=1619 width=53)
   Sort Key: (((count(DISTINCT date_part('doy'::text, reported)) * 100) / 365)) DESC
   ->  GroupAggregate  (cost=4366.42..4436.99 rows=1619 width=53)
         Group Key: highway, area
         ->  Sort  (cost=4366.42..4371.88 rows=2182 width=45)
               Sort Key: highway, area
               ->  Seq Scan on caltrans  (cost=0.00..4245.41 rows=2182 width=45)
                     Filter: ((condition ~~ '%CLOSED%DUE TO SNOW%'::text) OR (condition ~~ '%CLOSED%FOR THE WINTER%'::text))

-- 1
EXPLAIN
SELECT
highway,
stretch,
COUNT(1) AS days_closed,
100 * COUNT(1) / 365 AS pct_closed_365,
100 * COUNT(1) / 353 AS pct_closed_353
FROM (
SELECT
highway AS highway,
area AS stretch,
DATE(reported) AS closure
FROM hw2.caltrans
WHERE condition LIKE '%CLOSED%' AND (
condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
GROUP BY highway, stretch, closure
) result GROUP BY highway, stretch ORDER BY pct_closed_365 DESC;


QUERY PLAN                                                                         
-----------------------------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=4531.47..4531.97 rows=200 width=61)
   Sort Key: (((100 * count(1)) / 365)) DESC
   ->  GroupAggregate  (cost=4512.70..4523.83 rows=200 width=61)
         Group Key: caltrans.highway, caltrans.area
         ->  Group  (cost=4512.70..4515.26 rows=203 width=41)
               Group Key: caltrans.highway, caltrans.area, (date(caltrans.reported))
               ->  Sort  (cost=4512.70..4513.21 rows=205 width=41)
                     Sort Key: caltrans.highway, caltrans.area, (date(caltrans.reported))
                     ->  Seq Scan on caltrans  (cost=0.00..4504.83 rows=205 width=41)
                           Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))

-- 2
EXPLAIN
SELECT
closures.highway,
stretch,
COUNT(1) AS days_closed,
100 * COUNT(1) / 365 AS pct_closed_365,
100 * COUNT(1) / 353 AS pct_closed_353
FROM (
SELECT
c.highway AS highway,
c.area AS stretch,
DATE(c.reported) AS closure
FROM hw2.caltrans c
JOIN (
SELECT
DISTINCT
highway,
area
FROM hw2.caltrans
WHERE condition like '%CLOSED%' AND (
condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO NOW%')
) snow_highways ON c.highway = snow_highways.highway
WHERE condition like '%CLOSED%' AND (
condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
GROUP BY c.highway, stretch, closure
) closures
GROUP BY closures.highway, closures.stretch
ORDER BY pct_closed_365 DESC;

QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=9037.08..9037.40 rows=128 width=61)
   Sort Key: (((100 * count(1)) / 365)) DESC
   ->  GroupAggregate  (cost=9025.56..9032.60 rows=128 width=61)
         Group Key: c.highway, c.area
         ->  Group  (cost=9025.56..9027.16 rows=128 width=41)
               Group Key: c.highway, c.area, (date(c.reported))
               ->  Sort  (cost=9025.56..9025.88 rows=128 width=41)
                     Sort Key: c.highway, c.area, (date(c.reported))
                     ->  Hash Join  (cost=4514.39..9021.08 rows=128 width=41)
                           Hash Cond: ((c.highway)::text = (snow_highways.highway)::text)
                           ->  Seq Scan on caltrans c  (cost=0.00..4504.32 rows=205 width=45)
                                 Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))
                           ->  Hash  (cost=4512.52..4512.52 rows=150 width=5)
                                 ->  Subquery Scan on snow_highways  (cost=4509.87..4512.52 rows=150 width=5)
                                       ->  Unique  (cost=4509.87..4511.02 rows=150 width=37)
                                             ->  Sort  (cost=4509.87..4510.25 rows=153 width=37)
                                                   Sort Key: caltrans.highway, caltrans.area
                                                   ->  Seq Scan on caltrans  (cost=0.00..4504.32 rows=153 width=37)
                                                         Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO NOW%'::text)))

-- 3
EXPLAIN
SELECT
highway,
stretch,
COUNT(1) AS days_closed,
100 * COUNT(1) / 365 AS pct_closed_365,
100 * COUNT(1) / 353 AS pct_closed_353
FROM (
SELECT
c.highway AS highway,
c.area AS stretch,
DATE(c.reported) AS closure
FROM hw2.caltrans c
WHERE (highway, area) IN (
SELECT
DISTINCT
highway,
area
FROM hw2.caltrans
WHERE condition like '%CLOSED%' AND (
condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
) AND condition LIKE '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
GROUP BY highway, stretch, closure
) closures
GROUP BY highway, stretch
ORDER BY pct_closed_365 DESC;

QUERY PLAN                                                                                     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=9024.18..9024.18 rows=1 width=61)
   Sort Key: (((100 * count(1)) / 365)) DESC
   ->  GroupAggregate  (cost=9024.11..9024.17 rows=1 width=61)
         Group Key: c.highway, c.area
         ->  Group  (cost=9024.11..9024.12 rows=1 width=41)
               Group Key: c.highway, c.area, (date(c.reported))
               ->  Sort  (cost=9024.11..9024.11 rows=1 width=41)
                     Sort Key: c.highway, c.area, (date(c.reported))
                     ->  Hash Join  (cost=4518.70..9024.10 rows=1 width=41)
                           Hash Cond: (((c.highway)::text = (caltrans.highway)::text) AND ((c.area)::text = (caltrans.area)::text))
                           ->  Seq Scan on caltrans c  (cost=0.00..4504.32 rows=205 width=45)
                                 Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))
                           ->  Hash  (cost=4515.72..4515.72 rows=199 width=37)
                                 ->  Unique  (cost=4512.19..4513.73 rows=199 width=37)
                                       ->  Sort  (cost=4512.19..4512.70 rows=205 width=37)
                                             Sort Key: caltrans.highway, caltrans.area
                                             ->  Seq Scan on caltrans  (cost=0.00..4504.32 rows=205 width=37)
                                                   Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))


-- 4
EXPLAIN
SELECT
highway,
stretch,
COUNT(1) AS days_closed,
100 * COUNT(1) / 365 AS pct_closed_365,
100 * COUNT(1) / 353 AS pct_closed_353
FROM (
SELECT
c.highway AS highway,
c.area AS stretch,
DATE(c.reported) AS closure
FROM hw2.caltrans c
WHERE EXISTS (
SELECT
1
FROM hw2.caltrans
WHERE condition like '%CLOSED%' AND (
condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
) AND condition LIKE '%CLOSED%' AND (condition LIKE '%FOR THE WINTER%' OR condition LIKE '%DUE TO SNOW%')
GROUP BY highway, stretch, closure
) closures
GROUP BY highway, stretch
ORDER BY pct_closed_365 DESC;

 QUERY PLAN                                                                            
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=4553.44..4553.94 rows=200 width=61)
   Sort Key: (((100 * count(1)) / 365)) DESC
   ->  GroupAggregate  (cost=4534.67..4545.80 rows=200 width=61)
         Group Key: c.highway, c.area
         ->  Group  (cost=4534.67..4537.23 rows=203 width=41)
               Group Key: c.highway, c.area, (date(c.reported))
               InitPlan 1 (returns $0)
                 ->  Seq Scan on caltrans  (cost=0.00..4504.32 rows=205 width=0)
                       Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))
               ->  Sort  (cost=4512.70..4513.21 rows=205 width=41)
                     Sort Key: c.highway, c.area, (date(c.reported))
                     ->  Result  (cost=0.00..4504.83 rows=205 width=41)
                           One-Time Filter: $0
                           ->  Seq Scan on caltrans c  (cost=0.00..4504.83 rows=205 width=41)
                                 Filter: ((condition ~~ '%CLOSED%'::text) AND ((condition ~~ '%FOR THE WINTER%'::text) OR (condition ~~ '%DUE TO SNOW%'::text)))


One Paragraph Reflection: 

> hash hoin doubles your cost. 2 and 3 are both not efficient because they both use hash joins. they are eliminated from the decisions. 
> next we look at 0, 1, 4
> the group GroupAggregate function increases the row size for 0, 1, and 4. There is no way to get around this because we are aggregating in our query. This factor is not reflected in my decision. 
> From here, it looks like all three queries 0, 1, and 4 have the same steps involved. 
> all three have the same costs, so then the next two things to look at are 1. total run-time  and 2. cost and 3. number of rows and 4. width. 
> Here, I can remove 0 because it has a lower fan out than 1 and 4. It also has a higher total run time than 1 and 4. 

> My final answer is 1 is the more efficient between 1 and 4 because it has a lower cost and that is the last benchmark to look at. 
















