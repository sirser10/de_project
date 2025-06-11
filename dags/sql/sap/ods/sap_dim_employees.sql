drop table if exists bckp.sap_dct_employees_buffer;
create table if not exists bckp.sap_dct_employees_buffer as 
select * from ods.sap_dim_employees
;

alter table bckp.sap_dct_employees_buffer 
drop constraint if exists sap_dct_employees_buffer_pk,
add constraint sap_dct_employees_buffer_pk primary key (employee_id, ms_org_unit_id, ms_position_id, updated_dttm)
;


insert into bckp.sap_dct_employees_buffer as target(
		select 
    			  row_code
    			, employee_id
    			, email
    			, name
    			, surname
    			, patronymic
    			, ms_org_unit_id
    			, ms_position_id
    			, updated_dttm
				
		from stg.sap_dim_employees
) on 
conflict on constraint sap_dct_employees_buffer_pk do 

update 
set 
	  employee_id = excluded.employee_id,
	  email       = excluded.email,
	  name 		  = excluded.name,
	  surname 	  = excluded.surname,
	  patronymic  = excluded.patronymic,
	  ms_org_unit_id = excluded.ms_org_unit_id,
	  ms_position_id = excluded.ms_position_id

where (
		case 
	         when target.employee_id != excluded.employee_id  then 1
			 when target.email 		 != excluded.email 		 then 1
			 when target.name 		 != excluded.name 		 then 1
			 when target.surname 	 != excluded.surname      then 1
			 when target.patronymic  != excluded.patronymic 	 then 1
			 when target.ms_org_unit_id != excluded.ms_org_unit_id  then 1
			 when target.ms_position_id != excluded.ms_position_id  then 1
		  else 0
		end 
		) = 1
;

create table if not exists ods.sap_dim_employees (
												  row_code text
												, employee_id varchar(255)
												, email varchar(255)
												, name varchar(255)
												, surname varchar(255)
												, patronymic varchar(255)
												, ms_org_unit_id int 
												, ms_position_id int
												, updated_dttm timestamp 
)
;

alter table ods.sap_dim_employees
drop constraint if exists sap_dct_employees_pk, 
add constraint sap_dct_employees_pk primary key  (employee_id, ms_org_unit_id, ms_position_id, updated_dttm)
;

with dedupl as(
	select
		  row_code
		, employee_id
		, email
		, name
		, surname
		, patronymic
		, ms_org_unit_id
		, ms_position_id
		, updated_dttm
        , lag(ms_org_unit_id) over (partition by employee_id order by updated_dttm) previous_ms_org_unit_id
	from bckp.sap_dct_employees_buffer
	)
, data_to_insert as (
select 
	  row_code
	, employee_id
	, email
	, name
	, surname
	, patronymic
	, ms_org_unit_id
	, ms_position_id
	, updated_dttm
	
from dedupl
where 
     (
		case 
		    when ms_org_unit_id != previous_ms_org_unit_id or previous_ms_org_unit_id is null then 1
	        else 0
        end
    ) = 1
)

insert into ods.sap_dim_employees as target(
		select 
        	  row_code
        	, employee_id
        	, email
        	, name
        	, surname
        	, patronymic
        	, ms_org_unit_id
        	, ms_position_id
        	, updated_dttm
				
		from data_to_insert
) on 
conflict on constraint sap_dct_employees_pk do 

update 
set 
	  employee_id = excluded.employee_id,
	  email       = excluded.email,
	  name 		  = excluded.name,
	  surname 	  = excluded.surname,
	  patronymic  = excluded.patronymic,
	  ms_org_unit_id = excluded.ms_org_unit_id,
	  ms_position_id = excluded.ms_position_id

where (
		case 
             when target.employee_id != excluded.employee_id  then 1
    		 when target.email 		 != excluded.email 		 then 1
    		 when target.name 		 != excluded.name 		 then 1
    		 when target.surname 	 != excluded.surname      then 1
    		 when target.patronymic  != excluded.patronymic 	 then 1
    		 when target.ms_org_unit_id != excluded.ms_org_unit_id  then 1
    		 when target.ms_position_id != excluded.ms_position_id  then 1
		  else 0
		end 
		) = 1
;