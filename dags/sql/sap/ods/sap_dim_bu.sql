create table if not exists ods.sap_dim_bu(
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


alter table ods.sap_dim_bu
drop constraint if exists sap_dct_business_units_pk,
add constraint sap_dct_business_units_pk primary key (bu_id, bu_type, bu_name, bu_parent_id); 



INSERT INTO ods.sap_dim_bu AS target (
		select
			  row_code
			, bu_id
			, bu_type
			, bu_name
			, bu_parent_type
			, bu_parent_id
			, manager_user_email
			, financier_user
			, updated_dttm 
		FROM
			stg.sap_dim_bu 
	) ON
	CONFLICT ON CONSTRAINT sap_dct_business_units_pk DO

UPDATE
SET
	  bu_id			 	 = excluded.bu_id,
	  bu_type 			 = excluded.bu_type,
	  bu_name 			 = excluded.bu_name,
	  bu_parent_type 	 = excluded.bu_parent_type,
	  bu_parent_id 		 = excluded.bu_parent_id,
	  manager_user_email = excluded.manager_user_email,
	  financier_user 	 = excluded.financier_user
	  
where
		(
			CASE
				 when target.bu_id			 	 != excluded.bu_id			   then 1
				 when target.bu_type 			 != excluded.bu_type 		   then 1
				 when target.bu_name 			 != excluded.bu_name 		   then 1
				 when target.bu_parent_type 	 != excluded.bu_parent_type     then 1
				 when target.bu_parent_id 		 != excluded.bu_parent_id   then 1
				 when target.manager_user_email  != excluded.manager_user_email then 1
				 when target.financier_user 	 != excluded.financier_user     then 1
				ELSE 0
			END
			) = 1;
