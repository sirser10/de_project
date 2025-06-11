create table if not exists stg.sap_dim_legal_struct(
														  row_code text
														, employee_id varchar(255) 
														, person_id varchar(255) 
														, email varchar(255)
														, username varchar(255)
														, legal_position_id int
														, legal_position_name text
														, legal_unit_id int
														, legal_unit_name text
														, employee_group_id varchar(50)
														, employee_group_name varchar(255)
														, employee_category_id varchar(50)
														, employee_category_name text
														, cost_center_bybudgetstruc text
														, cost_center_byposition text 
														, updated_dttm timestamp 
);

truncate stg.sap_dim_legal_struct;


insert into stg.sap_dim_legal_struct(

with cte1 as (
				select 
					      pernr :: int 										  as employee_id
						, person_id :: int 									  as person_id
						, email :: varchar 									  as email
						, user_name :: varchar 			  as username
						, position_id :: int 								  as legal_position_id
						, position_text :: text 			  as legal_position_name
						, org_unit_id :: int 								  as legal_unit_id
						, org_unit_text :: text 							  as legal_unit_name
						, employee_group_id :: varchar 						  as employee_group_id
						, employee_group_text :: varchar 					  as employee_group_name
						, employee_category_id :: varchar	 				  as employee_category_id
						, employee_category_text :: text 					  as employee_category_name
						, cost_center_bybudgetstruc :: text  as cost_center_bybudgetstruc
						, cost_center_byposition :: text 	  as cost_center_byposition
						, current_timestamp 								  as updated_dttm
						, row_number()over w1 								  as row_num
				
				from src.sap_dim_legal_struct
				window
					w1 as(partition by pernr, person_id  order by updated_dttm desc)

)

select 
		md5(
				employee_id::text || '_' ||
			    person_id::text
			  ) as row_code
	   	, employee_id
		, person_id
		, email
		, username
		, legal_position_id
		, legal_position_name
		, legal_unit_id
		, legal_unit_name
		, employee_group_id
		, employee_group_name
		, employee_category_id
		, employee_category_name
		, cost_center_bybudgetstruc
		, cost_center_byposition
		, updated_dttm
		
from cte1
where row_num = 1
);
