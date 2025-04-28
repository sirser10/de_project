drop table if exists  stg.manual_interviewed_data;                                                    

CREATE TABLE IF NOT EXISTS stg.manual_interviewed_data (
						                             candidate_id INT
                                                     , username TEXT
                                                     , contract_type TEXT
                                                     , res_id INT
                                                     , status TEXT
                                                     , c_owner TEXT
                                                     , source_type TEXT
                                                     , profile_title TEXT
                                                     , first_name TEXT
                                                     , middle_name TEXT
                                                     , last_name TEXT
                                                     , address1 TEXT
                                                     , address2 TEXT
                                                     , city TEXT
                                                     , state TEXT
                                                     , country TEXT
                                                     , zip INT
                                                     , availability_status TEXT
                                                     , currency CHAR(3)
                                                     , date_of_birth DATE
                                                     , resume_file_name TEXT
                                                     , job_title TEXT
                                                     , phones TEXT
                                                     , source TEXT
                                                     , notes TEXT
                                                     , updated_dttm TIMESTAMP
                                                     );

insert into stg.manual_interviewed_data (  
  
  WITH serialization AS(
        SELECT 
                  candidate_id:: NUMERIC :: INT AS candidate_id
                , username:: TEXT AS username
                , contract_type:: TEXT AS contract_type
                , res_id:: NUMERIC :: INT AS res_id
                , status:: TEXT AS status
                , c_owner:: TEXT AS c_owner
                , source_type:: TEXT AS source_type
                , profile_title:: TEXT AS profile_title
                , first_name:: TEXT AS first_name
                , middle_name:: TEXT AS middle_name
                , last_name:: TEXT AS last_name
                , address1:: TEXT AS address1
                , address2:: TEXT AS address2
                , city:: TEXT AS city
                , state:: TEXT AS state
                , country:: TEXT AS country
                , zip:: NUMERIC :: INT AS zip
                , availability_status:: TEXT AS availability_status
                , currency:: CHAR(3) AS currency
                ,  CASE 
                    WHEN LENGTH(date_of_birth) =10 AND position('.' IN date_of_birth) > 0 THEN TO_DATE(date_of_birth,'DD.MM.YYYY')
                    WHEN LENGTH(date_of_birth) =10 AND position('-' IN date_of_birth) > 0 THEN TO_DATE(date_of_birth,'YYYY-MM-DD')
                    WHEN LENGTH(date_of_birth) =10 AND position('/' IN date_of_birth) > 0 THEN TO_DATE(date_of_birth,'DD/MM/YYYY')
                    END :: DATE AS date_of_birth
                , resume_file_name:: TEXT AS resume_file_name
                , job_title:: TEXT AS job_title
                , phones:: TEXT AS phones
                , source:: TEXT AS source
                , notes:: TEXT AS notes               
                , CURRENT_TIMESTAMP AS updated_dttm
        FROM src.manual_interviewed_data
        where candidate_id is not null --due to candidate_id cannot contain null if we consider it as PK
)
, deduplication AS (
    select * , ROW_NUMBER() OVER (PARTITION BY candidate_id) AS rn 
	FROM serialization
	where (
		 case
		 	when first_name is null then 0 
		 	when middle_name is null then 0
		 	when last_name is null then 0
		 	when date_of_birth is null then 0
		 	else 1
		 end
	)=1 /*
            also we are supposed that first/middle/last_name or date_of_birth cannot be void 
            due to the concept of clean data provision
        */
	
)
SELECT 
	 candidate_id          
	, username
    , contract_type
    , res_id
    , status
    , c_owner
    , source_type
    , profile_title
    , first_name
    , middle_name
    , last_name
    , address1
    , address2
    , city
    , state
    , country
    , zip
    , availability_status
    , currency
    , date_of_birth
    , resume_file_name
    , job_title
    , phones
    , source
    , notes
    , updated_dttm
    
FROM deduplication
WHERE rn = 1
);