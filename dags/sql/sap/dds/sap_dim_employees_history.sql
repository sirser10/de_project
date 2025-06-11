create table if not exists dds.sap_dim_employees_history (
													  employee_id varchar(255)
													, email varchar(255)
													, name varchar(255)
													, surname varchar(255)
													, patronymic varchar(255)
													, ms_org_unit_id int 
													, ms_position_id int
			  		    							, valid_from_dt date
													, valid_to_dt date
													, updated_dttm timestamp 
													, primary key (
																  employee_id 
																, ms_org_unit_id  
																, ms_position_id 
						  		    							, valid_from_dt
																)
);

truncate dds.sap_dim_employees_history; 

insert into dds.sap_dim_employees_history (
	with cte as (
				select 
						  employee_id
						, email
						, name
						, surname
						, patronymic
						, ms_org_unit_id
						, ms_position_id
						, updated_dttm :: date as updated_dt
						, row_number () over (partition by employee_id order by updated_dttm asc) as rn
				from ods.sap_dim_employees 
	)
	,
	cte2 as (
			select 
				   c1.employee_id 
				 , c1.email
				 , c1.name
				 , c1.surname
				 , c1.patronymic
				 , c1.ms_org_unit_id
				 , c1.ms_position_id
				 , c1.updated_dt as valid_from_dt
				 , c1.rn
				 , case 
				 	when c2.updated_dt is not null then c2.updated_dt - interval '1 day'
				 	else '9999-12-31'
				 end :: date as valid_to_dt
				 
			from cte c1 
			left join cte c2 
					on c1.employee_id = c2.employee_id
					and c1.rn = c2.rn - 1
	)
	select 
			   employee_id 
			 , email
			 , name
			 , surname
			 , patronymic
			 , ms_org_unit_id
			 , ms_position_id
			 , valid_from_dt
			 , valid_to_dt
			 , current_timestamp as updated_dttm
			 
	 from cte2
);
