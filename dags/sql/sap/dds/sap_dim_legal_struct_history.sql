create table if not exists dds.sap_dim_legal_struct_history (
														  employee_id varchar(255) 
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
			  			    							, valid_from_dt date
														, valid_to_dt date
														, updated_dttm timestamp
														, primary key (
																			employee_id
																		, 	person_id
																		, 	valid_from_dt
														) 
);

truncate dds.sap_dim_legal_struct_history; 

insert into dds.sap_dim_legal_struct_history (
WITH cte AS (
    SELECT 
		   	  employee_id
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
			, updated_dttm :: date as updated_dt
        	, ROW_NUMBER() OVER (PARTITION BY person_id  ORDER BY updated_dttm asc) AS rn
        	
    FROM ods.sap_dim_legal_struct
)
,
cte2 AS (
    SELECT 
		   	  c1.employee_id
			, c1.person_id
			, c1.email
			, c1.username
			, c1.legal_position_id
			, c1.legal_position_name
			, c1.legal_unit_id
			, c1.legal_unit_name
			, c1.employee_group_id
			, c1.employee_group_name
			, c1.employee_category_id
			, c1.employee_category_name
			, c1.cost_center_bybudgetstruc
			, c1.cost_center_byposition
    	  	, c1.updated_dt as valid_from_dt
          	, c1.rn 
			, case 
				when c2.updated_dt is not null then c2.updated_dt - interval '1 day'
				else '9999-12-31'
			end :: date as valid_to_dt
			
    FROM cte c1
    LEFT JOIN cte c2
        ON c1.person_id = c2.person_id
          and c1.rn = c2.rn - 1
)
SELECT 
		   	  employee_id
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
			, valid_from_dt
			, valid_to_dt
			, current_timestamp as updated_dttm
    
FROM cte2
);

