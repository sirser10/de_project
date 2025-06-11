drop table if exists bckp.sap_dct_org_units_buffer;
create table if not exists bckp.sap_dct_org_units_buffer as 
select * from ods.sap_dim_org_units;

alter table bckp.sap_dct_org_units_buffer 
drop constraint if exists sap_dct_org_units_buffer_pk,
add constraint sap_dct_org_units_buffer_pk primary key (ms_org_unit_id, parent_ms_org_unit_id, updated_dttm);

insert into bckp.sap_dct_org_units_buffer as target(
		select 
				  row_code 
				, ms_org_unit_id 
				, parent_ms_org_unit_id
				, description 
				, manager 
				, hrbp 
				, hrd
				, updated_dttm
				, bc
				
		from stg.sap_dim_org_units
) on 
conflict on constraint sap_dct_org_units_buffer_pk do 

update 
set 
	  ms_org_unit_id        = excluded.ms_org_unit_id
	, parent_ms_org_unit_id = excluded.parent_ms_org_unit_id
	, description 			= excluded.description
	, manager 				= excluded.manager
	, hrbp 					= excluded.hrbp
	, hrd 					= excluded.hrd
	, bc 					= excluded.bc

where (
		case 
			when target.ms_org_unit_id 		  != excluded.ms_org_unit_id 	   then 1 
			when target.parent_ms_org_unit_id != excluded.parent_ms_org_unit_id then 1
			when target.description 		  != excluded.description 		   then 1
			when target.manager 			  != excluded.manager 			   then 1
			when target.hrbp 				  != excluded.hrbp 				   then 1
			when target.hrd 				  != excluded.hrd 				   then 1
			when target.bc 					  != excluded.bc 				   then 1
		  else 0
		end 
		) = 1
;

create table if not exists ods.sap_dim_org_units (
							  row_code text
							, ms_org_unit_id int 
							, parent_ms_org_unit_id int 
							, description text 
							, manager varchar(255)
							, hrbp varchar(255)
							, hrd varchar(255)
							, updated_dttm timestamp
							, bc text 
);

alter table ods.sap_dim_org_units
drop constraint if exists sap_dct_org_units_pk,
add constraint sap_dct_org_units_pk primary key (ms_org_unit_id, parent_ms_org_unit_id, updated_dttm); 

with null_erasing as (
select 
		row_code
		, ms_org_unit_id 
		, parent_ms_org_unit_id
		, description 
		, manager
		, coalesce(manager, 'none') as manager_null
		, hrbp  
		, hrd
		, updated_dttm
		, bc 
		
from bckp.sap_dct_org_units_buffer
)
, dedupl as(
	select
		row_code
		, ms_org_unit_id 
		, parent_ms_org_unit_id
		, description 
		, manager
		, manager_null
		, hrbp 
		, hrd
		, updated_dttm
		, bc 
		, lag(parent_ms_org_unit_id) over (partition by ms_org_unit_id order by updated_dttm) as previous_parent_id
		, lag(manager_null) over (
								partition by 
											  ms_org_unit_id
											, parent_ms_org_unit_id
								order by updated_dttm
							) as previous_manager
	
	from null_erasing
	)
, data_to_insert as (
select 
		row_code
		, ms_org_unit_id 
		, parent_ms_org_unit_id
		, description 
		, manager 
		, hrbp 
		, hrd
		, bc
		, updated_dttm 
	
from dedupl
where 
     (
		case 
		    when parent_ms_org_unit_id != previous_parent_id or previous_parent_id is null then 1
	        else 0
        end 
    )
    +
    (
     case 
        when manager_null != previous_manager  or previous_manager is null then 1
        else 0
      end 
    ) !=0
)
insert into ods.sap_dim_org_units as target(
		select 
				  row_code 
				, ms_org_unit_id 
				, parent_ms_org_unit_id
				, description 
				, manager 
				, hrbp 
				, hrd
				, updated_dttm
				, bc
				
		from data_to_insert
) on conflict on constraint sap_dct_org_units_pk do 

update 
set 
	  ms_org_unit_id        = excluded.ms_org_unit_id
	, parent_ms_org_unit_id = excluded.parent_ms_org_unit_id
	, description 			= excluded.description
	, manager 				= excluded.manager
	, hrbp 					= excluded.hrbp
	, hrd 					= excluded.hrd
	, bc 					= excluded.bc

where (
		case 
			when target.ms_org_unit_id 		  != excluded.ms_org_unit_id 	   then 1 
			when target.parent_ms_org_unit_id != excluded.parent_ms_org_unit_id then 1
			when target.description 		  != excluded.description 		   then 1
			when target.manager 			  != excluded.manager 			   then 1
			when target.hrbp 				  != excluded.hrbp 				   then 1
			when target.hrd 				  != excluded.hrd 				   then 1
			when target.bc					  != excluded.bc 				   then 1
		  else 0
		end 
		) = 1
;