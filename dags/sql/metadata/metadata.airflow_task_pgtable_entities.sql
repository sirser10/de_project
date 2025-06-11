drop table if exists metadata.airflow_task_pgtable_entities;
create table if not exists metadata.airflow_task_pgtable_entities(
    id serial primary key
    , table_schema text not null 
    , table_name text not null 
    , dag_name text 
    , task_table_name text
    , updated_dttm timestamp 
);

insert into metadata.airflow_task_pgtable_entities(
      table_schema 
    , table_name  
    , dag_name 
    , task_table_name
    , updated_dttm
)
with airflow_tasks as (
	select
		  dag_name
		, task
		, updated_dttm :: timestamp 
		, case
			when task ~ '^(src_|stg|ods|dds|dm)' then substring(task from '^(src_|stg|ods|dds|dm)')
		end as table_schema
		, regexp_replace(
						regexp_replace(task, '^(.*\.)|((src|stg|ods|dds|dm)._)','','g')
						, '^(src_|stg_|ods_|dds_|dm_)'
						, ''
						,'g'
						) as table_name
	from src.airflow_dag_tasks
	where (
			case 
				when task ~ '^(src_|stg|ods|dds|dm)' then substring(task from '^(src_|stg|ods|dds|dm)') 
				end
		) is not null 
)

select 
	  info_c.table_schema
	, info_c.table_name
	, at.dag_name
	, concat_ws('.',at.table_schema, at.table_name) as task_table_name
	, at.updated_dttm
	
from information_schema.tables info_c
left join airflow_tasks at using (table_schema, table_name)
where table_schema not in ('pg_catalog', 'information_schema')
;