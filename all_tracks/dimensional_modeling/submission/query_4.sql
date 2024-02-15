insert into surbhiwahie.actors_history_scd

WITH lagged as(

select 
actor,
is_active ,
lag(is_active,1) over(partition by actor order by current_year) as is_active_last_year,
lag(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) as quality_class_last_year,
current_year
from surbhiwahie.actors
where current_year <= 2020
),
streaked as(

select *,
sum(CASE WHEN is_active <> is_active_last_year
OR quality_class <> quality_class_last_year
 THEN 1 ELSE 0 END) over(partition by actor order by current_year) as streak_identifier
 from lagged)
 
 select 
 actor,
 max(is_active) as is_active,
 min(current_year) as start_year,
 max(current_year) as end_year,
 2020 as current_year
  from streaked
  group by actor, streak_identifier
