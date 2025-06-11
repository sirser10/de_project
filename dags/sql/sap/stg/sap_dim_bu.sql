create table if not exists stg.sap_dim_bu(
													  	row_code text
													  , bu_id varchar(255)
													  , bu_type varchar(255)
													  , bu_name text
													  , bu_parent_type varchar(255)
													  , bu_parent_id varchar(255)
													  , manager_user_email varchar(255)
													  , financier_user text
													  , updated_dttm timestamp 
);

truncate stg.sap_dim_bu; 

insert into stg.sap_dim_bu (

	with cte1 as (
					select
							  id :: varchar as bu_id
							, type :: varchar as bu_type
							, name :: text as bu_name
							, coalesce (parent_type, 'none') :: varchar as bu_parent_type 
							, coalesce (parentid,'none') :: varchar as bu_parent_id
							, manager_user :: varchar as manager_user_email
							, financier_user:: text as financier_user
							, current_timestamp as updated_dttm
							, row_number () over w1 as row_num
					from src.sap_dim_bu
					window 
						w1 as (partition by id, type, name order by updated_dttm desc)
			)

	select 
			md5(
				bu_id   ||'_'||
				bu_type ||'_'||
				bu_name ||'_'||
				bu_parent_id
			) as row_code
			, bu_id
			, bu_type
			, bu_name
			, bu_parent_type
			, bu_parent_id
			, manager_user_email 
			, financier_user
			, updated_dttm

	from cte1
	where row_num = 1

);