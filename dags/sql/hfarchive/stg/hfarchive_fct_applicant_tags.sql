CREATE OR REPLACE FUNCTION src.extract_list_items(column_name JSONB, json_key TEXT)
  RETURNS SETOF JSONB AS
$$
BEGIN
  RETURN QUERY SELECT value
               FROM jsonb_array_elements(column_name->json_key) AS value;
END;
$$
LANGUAGE plpgsql;



-- create table if not exists stg.hfarchive_fct_applicant_tags (
																		
-- 																	row_code text primary key
-- 																, applicant_id bigint
-- 																, tag_id bigint
-- 																, upd_dtime timestamp 
-- 	);


truncate stg.hfarchive_fct_applicant_tags;

insert into stg.hfarchive_fct_applicant_tags (
												row_code
											  , applicant_id
											  , tag_id
											  , upd_dtime
									)	
with stage1 as(											
				select 	
						md5(
							(content ->> 'applicant_id') || '_'	|| (src.extract_list_items(content, 'tag_id'))
						) as row_code
						, (content ->> 'applicant_id') :: numeric :: bigint as applicant_id 
						, ((src.extract_list_items(content, 'tag_id'))) :: bigint as tag_id
						, current_timestamp     as upd_dtime		
				from src.hfarchive_fct_applicant_tags
		)
				
select 
		row_code
	  , applicant_id
	  , tag_id
	  , upd_dtime
	  
from stage1