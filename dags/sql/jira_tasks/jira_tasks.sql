drop table if exists stg.jira_tasks;
create table if not exists stg.jira_tasks(
												row_id text
											 ,  task_id bigint 
											 ,  task_link text 
											 ,  task_key text
											 ,  fields jsonb
											 ,  updated_dttm timestamp												
)
;

insert into stg.jira_tasks (
									  row_id
									, task_id
									, task_link
									, task_key
									, fields
									, updated_dttm
)
with cte as (
select
	  expand :: text 
	, id :: bigint as task_id
	, "self" as task_link
	, key :: text task_key
	, fields :: jsonb as fields
	, current_timestamp as updated_dttm
	
from src.jira_tasks
)
, cte2 as (
select 
	 *
	, row_number() over (partition by task_id, task_key, fields order by updated_dttm)  as rn 
from cte
)
select 
	md5(task_id ||'_' ||
		task_key || '_' || 
		fields || '_'
		) as row_id
	, task_id
	, task_link
	, task_key
	, fields
	, updated_dttm

from cte2	
where rn = 1
;