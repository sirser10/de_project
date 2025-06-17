-- CREATE OR REPLACE FUNCTION src.get_jsonb_list_value(column_name JSONB, json_key TEXT, key_name TEXT)
-- RETURNS SETOF TEXT AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT value ->> key_name
--     FROM jsonb_array_elements(column_name -> json_key) AS value;
-- END;
-- $$ LANGUAGE plpgsql;


-- create table if not exists stg.hfarchive_fct_applicant_duplicates (
-- 																	  row_code text primary key
-- 																	, applicant_id BIGINT 
-- 																	, dupl_applicant_id bigint 
-- 																	, dupl_comment_txt text
-- 																	, dupl_log_created_by_user_id bigint 
-- 																	, dupl_log_created_dtime TIMESTAMP
-- 																	, upd_dtime TIMESTAMP
-- 								);



truncate stg.hfarchive_fct_applicant_duplicates;

insert into stg.hfarchive_fct_applicant_duplicates (
													  row_code
													, applicant_id
													, dupl_log_created_by_user_id
													, dupl_applicant_id
													, dupl_comment_txt
													, dupl_log_created_dtime
													, upd_dtime
													)
WITH stage1 AS (
				     SELECT 
				        md5((content ->> 'applicant_id') || '_' || (src.get_jsonb_list_value(content, 'logs', 'dupl_applicant_id'))) AS row__code,
				        (content ->> 'applicant_id') :: NUMERIC :: BIGINT  AS applicant_id,
				        ((src.get_jsonb_list_value(content, 'logs', 'dupl_log_created_by_user_id'))) :: BIGINT AS dupl_log_created_by_user_id,
				        ((src.get_jsonb_list_value(content, 'logs', 'dupl_applicant_id')))  :: BIGINT AS dupl_applicant_id,
				        ((src.get_jsonb_list_value(content, 'logs', 'dupl_comment_txt'))) AS dupl_comment_txt,
				        ((src.get_jsonb_list_value(content, 'logs', 'dupl_log_created_dtime'))) :: TIMESTAMP AS dupl_log_created_dtime,
				        CURRENT_TIMESTAMP AS upd_dtime
				     FROM src.hfarchive_fct_applicant_duplicates
			     ),

stage2 AS (
			   SELECT 
					    row__code,
					    applicant_id,
					    dupl_log_created_by_user_id,
					    dupl_applicant_id,
					    dupl_comment_txt,
					    dupl_log_created_dtime,
					    upd_dtime,
					    ROW_NUMBER() OVER (PARTITION BY applicant_id, dupl_applicant_id ORDER BY dupl_log_created_dtime) AS row_num
			   FROM stage1
)

SELECT 
	   row__code,
	   applicant_id,
	   dupl_log_created_by_user_id,
	   dupl_applicant_id,
	   dupl_comment_txt,
	   dupl_log_created_dtime,
	   upd_dtime
   
FROM stage2
WHERE row_num = 1;
  