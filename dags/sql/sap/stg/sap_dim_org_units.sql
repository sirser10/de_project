create table if not exists stg.sap_dim_org_units(
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

truncate stg.sap_dim_org_units;

insert into stg.sap_dim_org_units (

								with cte1 as (
											select 
													  md5(id || '_'|| parent) :: text as row_code
													, id :: int as ms_org_unit_id
													, parent :: int as parent_ms_org_unit_id
													, description :: text as description 
													, manager :: varchar as manager 
													, hrbp :: varchar as hrbp 
													, hrd :: varchar as hrd 
													, current_timestamp as updated_dttm
													, bc :: text as bc
													, row_number() over w1 as row_num
													
											from src.sap_dim_org_units
											window
											w1 as (partition by id, parent order by updated_dttm desc)
												)
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
									  
								from cte1
								where row_num = 1
);
