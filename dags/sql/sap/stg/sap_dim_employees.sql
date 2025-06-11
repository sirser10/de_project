create table if not exists stg.sap_dim_employees(
												  row_code text
												, employee_id varchar(255)
												, email varchar(255)
												, name varchar(255)
												, surname varchar(255)
												, patronymic varchar(255)
												, ms_org_unit_id int 
												, ms_position_id int
												, updated_dttm timestamp 
);

create temp table src_dim_unmatched_employees as 

    with unmatched_tables as (
		select 
		      dte.id :: varchar as employee_id
		    , dte.email :: varchar as email
		    , dte.name :: varchar as name
		    , dte.surname :: varchar as surname
			, dte.patronymic :: varchar as patronymic
		    , dte.org_unit :: int as ms_org_unit_id
		    , dte.position :: int as ms_position_id
		    , dte.updated_dttm :: timestamp as updated_dttm
		    , row_number() over w1 as row_num
		    
		from src.sap_dim_jira_employees dte
		left join ods.sap_dim_employees de on 1=1
					and dte.id = de.employee_id
					and dte.org_unit :: int = de.ms_org_unit_id
					and dte.position:: int = de.ms_position_id
		where de.employee_id is null
		window w1 as (partition by dte.id, dte.org_unit, dte.position order by dte.updated_dttm desc)
    )
    
    select 
    	  employee_id
    	, email
    	, name
    	, surname
    	, patronymic
    	, ms_org_unit_id
    	, ms_position_id
    	, updated_dttm
    	
    from unmatched_tables
    where row_num = 1;

truncate stg.sap_dim_employees;
insert into stg.sap_dim_employees(
    
    
with cte1 as (
		select 
				   id :: varchar as employee_id
				, email :: varchar as email
				, name :: varchar as name
				, surname :: varchar as surname
				, patronymic :: varchar as patronymic
				, org_unit :: int as ms_org_unit_id
				, position :: int as ms_position_id
				, updated_dttm:: timestamp as updated_dttm
				, row_number() over w1 as row_num
		from src.sap_dim_employees
		window 
			w1 as(partition by id, org_unit, position order by updated_dttm desc)
)
, final_dedupication_row_n as (
    select  
    		 employee_id
    		, email
    		, name
    		, surname
    		, patronymic
    		, ms_org_unit_id
    		, ms_position_id
    		, updated_dttm
    from cte1
    where row_num = 1 
    
    UNION ALL 
    select 
    		  employee_id
    		, email
    		, name
    		, surname
    		, patronymic
    		, ms_org_unit_id
    		, ms_position_id
    		, updated_dttm
    		
    from src_dim_unmatched_employees
)
, final_deduplication as (
	select
		  employee_id
		, email
		, name
		, surname
		, patronymic
		, ms_org_unit_id
		, ms_position_id
		, updated_dttm
		, row_number() over w as row_num
	from final_dedupication_row_n
	window w as (partition by employee_id, ms_org_unit_id, ms_position_id order by updated_dttm desc)
) 
select 
	md5(
		employee_id ||'_'||
		ms_org_unit_id ||'_'||
		ms_position_id 
	) as row_code
	, employee_id
	, email
	, name
	, surname
	, patronymic
	, ms_org_unit_id
	, ms_position_id
	, updated_dttm
from final_deduplication
where row_num =1 
)
;
