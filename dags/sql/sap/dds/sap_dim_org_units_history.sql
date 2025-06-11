create table if not exists dds.sap_dim_org_units_history (
											  ms_org_unit_id int 
											, parent_ms_org_unit_id int 
											, description text 
											, manager varchar(255)
											, hrbp varchar(255)
											, hrd varchar(255)
	  		    							, valid_from_dt date
											, valid_to_dt date
											, updated_dttm timestamp
											, primary key (
														  ms_org_unit_id 
														, parent_ms_org_unit_id
				  		    							, valid_from_dt 
											)
);

truncate dds.sap_dim_org_units_history; 

insert into dds.sap_dim_org_units_history(
with cte as (
							select 
									  ms_org_unit_id 
									, parent_ms_org_unit_id
									, description 
									, manager 
									, hrbp 
									, hrd
									, updated_dttm :: date as updated_dt
									, row_number () over (partition by ms_org_unit_id order by updated_dttm asc) as rn 
							from ods.sap_dim_org_units
			)
			,
			cte2 as (
				select 
					  c1.ms_org_unit_id 
					, c1.parent_ms_org_unit_id
					, c1.description 
					, c1.manager 
					, c1.hrbp 
					, c1.hrd
					, c1.updated_dt as valid_from_dt
					, c1.rn
					, case 
						when c2.updated_dt is not null then c2.updated_dt - interval '1 day'
						else '9999-12-31'
					end :: date as valid_to_dt
					
				from cte c1 
				left join cte c2 on 
									 c1.ms_org_unit_id = c2.ms_org_unit_id
									 and c1.rn = c2.rn -1
			)
			select 
					  ms_org_unit_id 
					, parent_ms_org_unit_id
					, description
					, manager
					, hrbp
					, hrd 
					, valid_from_dt
					, valid_to_dt
					, current_timestamp as updated_dttm
			from cte2
);
