create table if not exists dds.sap_dim_bu_history(
														    bu_id varchar(255)
														  , bu_type varchar(255)
														  , bu_name text
														  , bu_parent_type varchar(255)
														  , bu_parent_id varchar(255)
														  , manager_user_email varchar(255)
														  , financier_user text
	  				  									  , valid_from_dt date
												          , valid_to_dt date
														  , updated_dttm timestamp 
														  , primary key(
														  				  bu_id
														  				, bu_type
														  				, bu_name
														  				, bu_parent_id
														  				, valid_from_dt
														  				)
);


truncate dds.sap_dim_bu_history;

insert into dds.sap_dim_bu_history (
	WITH cte AS (
	    SELECT 
				  bu_id
				, bu_type
				, bu_name
				, bu_parent_type
				, bu_parent_id
				, manager_user_email
				, financier_user
				, updated_dttm :: date as updated_dt 
	        	, ROW_NUMBER() OVER (PARTITION BY bu_id ORDER BY updated_dttm asc) AS rn
	    FROM ods.sap_dim_bu
	)
	,
	cte2 AS (
	    SELECT 
				  c1.bu_id
				, c1.bu_type
				, c1.bu_name
				, c1.bu_parent_type
				, c1.bu_parent_id
				, c1.manager_user_email
				, c1.financier_user
				, c1.updated_dt as valid_from_dt
		      	, c1.rn 
				, case 
				 	when c2.updated_dt is not null then c2.updated_dt - interval '1 day'
				 	else '9999-12-31'
				 end :: date as valid_to_dt
		          
	    FROM cte c1
	    LEFT JOIN cte c2
	        ON c1.bu_id = c2.bu_id 
				and c1.rn = c2.rn - 1
	)
	
	SELECT 
			  bu_id
			, bu_type
			, bu_name
			, bu_parent_type
			, bu_parent_id
			, manager_user_email
			, financier_user
			, valid_from_dt
			, valid_to_dt
			, current_timestamp as updated_dttm
	        
	FROM cte2

);