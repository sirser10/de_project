create table if not exists ods.sap_dim_legal_struct (

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


alter table ods.sap_dim_legal_struct
drop constraint if exists sap_legal_employees_struct_pk, 
add constraint sap_legal_employees_struct_pk primary key (employee_id, person_id); 

 INSERT INTO ods.sap_dim_legal_struct AS target (
		select
			  row_code
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
			
		FROM
			stg.sap_dim_legal_struct 
	) ON
	CONFLICT ON CONSTRAINT sap_legal_employees_struct_pk DO

UPDATE
SET
	  employee_id			 	 = excluded.employee_id,
	  person_id 			 	 = excluded.person_id,
	  email 			 		 = excluded.email,
	  username 	 				 = excluded.username,
	  legal_position_id 		 = excluded.legal_position_id,
	  legal_position_name 		 = excluded.legal_position_name,
	  legal_unit_id 			 = excluded.legal_unit_id,
	  legal_unit_name 			 = excluded.legal_unit_name,
	  employee_group_id 		 = excluded.employee_group_id,
	  employee_group_name 		 = excluded.employee_group_name,
	  employee_category_id 		 = excluded.employee_category_id,
	  employee_category_name 	 = excluded.employee_category_name,
	  cost_center_bybudgetstruc  = excluded.cost_center_bybudgetstruc,
	  cost_center_byposition 	 = excluded.cost_center_byposition
	  
where
		(
			CASE
				 when target.employee_id			 	!= excluded.employee_id then 1
				 when target.person_id 			 	 	!= excluded.person_id then 1
				 when target.email 			 		 	!= excluded.email then 1
				 when target.username 	 				!= excluded.username then 1
				 when target.legal_position_id 		 	!= excluded.legal_position_id then 1
				 when target.legal_position_name 		!= excluded.legal_position_name then 1
				 when target.legal_unit_id 			 	!= excluded.legal_unit_id then 1
				 when target.legal_unit_name 			!= excluded.legal_unit_name then 1
				 when target.employee_group_id 		 	!= excluded.employee_group_id then 1
				 when target.employee_group_name 		!= excluded.employee_group_name then 1
				 when target.employee_category_id 		!= excluded.employee_category_id then 1
				 when target.employee_category_name 	!= excluded.employee_category_name then 1
				 when target.cost_center_bybudgetstruc  != excluded.cost_center_bybudgetstruc then 1
				 when target.cost_center_byposition 	!= excluded.cost_center_byposition then 1
				ELSE 0
			END
			) = 1;