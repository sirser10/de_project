create table if not exists stg.sap_dim_positions  (
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

create temp table src_dim_unmatched_position_id as 

	with unmatched_position_id as (
		select 
			  sdjp.id :: int as ms_position_id
			, sdjp.org_unit :: int as ms_org_unit_id
			, coalesce(sdjp.description, 'none') :: text as description
			, null :: numeric :: bigint as budget_code_id
			, null :: text as budget_code_name
			, null :: char(10) as new_position
			, null :: date as date_entry
			, sdjp.updated_dttm :: timestamp as updated_dttm
			
		from src.sap_dim_jira_positions sdjp 
		left join ods.sap_dim_positions sdp on 1=1
			and sdjp.id:: int = sdp.ms_position_id
			and sdjp.org_unit :: int = sdp.ms_org_unit_id
			and coalesce(sdjp.description,'none')  = sdp.description
			
		where sdp.ms_position_id is null 
	
	)
	, deduplication as (
		select 
				  ms_position_id
				, ms_org_unit_id
				, description
				, budget_code_id
				, budget_code_name
				, new_position
				, date_entry
				, row_number () over w as row_num		
		from unmatched_position_id
		window w as(partition by
							 ms_position_id,
							 ms_org_unit_id,
							 description,
							 budget_code_id
					order by updated_dttm desc
			)
	)
	select 
		md5(
			ms_position_id ||'_'||
			ms_org_unit_id ||'_'||
			description
			) as row_code	
		, ms_position_id
		, ms_org_unit_id
		, description
		, budget_code_id
		, budget_code_name
		, new_position
		, date_entry
		
	from deduplication
	where row_num = 1;
	
truncate stg.sap_dim_positions;

insert into stg.sap_dim_positions(

	with cte1 as (
		select 
				  id :: int as ms_position_id
				, org_unit :: int as ms_org_unit_id
				, coalesce(description, 'none') :: text as description
				, budget_code_id :: numeric :: bigint as budget_code_id
				, budget_code_name :: text as budget_code_name
				, new_position :: char(10) as new_position
				, case 
						when extract(year from to_date(date_entry, 'yyyy-mm-dd')) = 0 then null
						when  extract(year from to_date(date_entry, 'yyyy-mm-dd')) > 0 then to_date(date_entry, 'yyyy-mm-dd')
				   end  :: date as date_entry
				, updated_dttm :: timestamp as updated_dttm
		from src.sap_dim_positions
	
	),
			
	cte2 as (
		select 
				  ms_position_id
				, ms_org_unit_id
				, description
				, budget_code_id
				, budget_code_name
				, new_position
				, date_entry
				, row_number () over w1 as row_num		
		from cte1
		window 
			w1 as(partition by
							 ms_position_id,
							 ms_org_unit_id,
							 description,
							 budget_code_id
					order by updated_dttm desc
				)
	)
	
	select 
		md5(
			ms_position_id ||'_'||
			ms_org_unit_id ||'_'||
			description ||'_'||
			budget_code_id
			) as row_code	
	    , ms_position_id
		, ms_org_unit_id
		, description
		, budget_code_id
		, budget_code_name
		, new_position
		, date_entry
		, current_timestamp as updated_dttm
			
	from cte2
	where row_num = 1
	
	union all 
	
	select 
		  row_code	
		, ms_position_id
		, ms_org_unit_id
		, description
		, budget_code_id
		, budget_code_name
		, new_position
		, date_entry
		, current_timestamp as updated_dttm
	from src_dim_unmatched_position_id

);
