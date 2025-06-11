create table if not exists ods.sap_dim_positions(
												  row_code text 	
												, ms_position_id int 
												, ms_org_unit_id int 
												, description text 
												, budget_code_id bigint
												, budget_code_name text 
												, new_position char(10)
												, date_entry date
												, updated_dttm timestamp 
);

alter table ods.sap_dim_positions
drop constraint if exists sap_dct_positions_pk, 
add constraint sap_dct_positions_pk primary key (ms_position_id,ms_org_unit_id,description); 


INSERT INTO ods.sap_dim_positions AS target (
		select
			  row_code
			, ms_position_id
			, ms_org_unit_id
			, description
			, updated_dttm
			, budget_code_id
			, budget_code_name
			, new_position
			, date_entry
		FROM
			stg.sap_dim_positions 
	) ON
	CONFLICT ON CONSTRAINT sap_dct_positions_pk DO

UPDATE
SET
	  ms_position_id = excluded.ms_position_id,
	  ms_org_unit_id = excluded.ms_org_unit_id,
	  description = excluded.description,
	  budget_code_id = excluded.budget_code_id,
	  budget_code_name  = excluded.budget_code_name,
	  new_position = excluded.new_position,
	  date_entry = excluded.date_entry

where
		(
			CASE
				WHEN target.ms_position_id != excluded.ms_position_id THEN 1
				WHEN target.ms_org_unit_id != excluded.ms_org_unit_id THEN 1
				WHEN target.description    != excluded.description THEN 1
				WHEN target.budget_code_id    != excluded.budget_code_id THEN 1
				WHEN target.budget_code_name    != excluded.budget_code_name THEN 1
				WHEN target.new_position    != excluded.new_position THEN 1
				WHEN target.date_entry    != excluded.date_entry THEN 1
				ELSE 0
			END
			) = 1;