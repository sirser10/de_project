create table if not exists ods.manual_interviewed_data(
                                                       candidate_id INT
                                                     , username TEXT not null
                                                     , contract_type TEXT
                                                     , res_id INT
                                                     , status TEXT
                                                     , c_owner TEXT
                                                     , source_type TEXT
                                                     , profile_title TEXT
                                                     , first_name TEXT not null
                                                     , middle_name TEXT not null
                                                     , last_name TEXT not null
                                                     , address1 TEXT
                                                     , address2 TEXT
                                                     , city TEXT
                                                     , state TEXT
                                                     , country TEXT
                                                     , zip INT
                                                     , availability_status TEXT
                                                     , currency CHAR(3)
                                                     , date_of_birth DATE not null
                                                     , resume_file_name TEXT
                                                     , job_title TEXT
                                                     , phones TEXT
                                                     , source TEXT
                                                     , notes TEXT
                                                     , updated_dttm TIMESTAMP
                                                     );

alter table ods.manual_interviewed_data
drop constraint if exists manual_interviewed_data_pk, 
add constraint  manual_interviewed_data_pk primary key (candidate_id);

INSERT INTO ods.manual_interviewed_data AS target (
		select
                
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
		FROM
			stg.manual_interviewed_data 
	) ON
	CONFLICT ON CONSTRAINT manual_interviewed_data_pk DO

UPDATE
SET
     candidate_id         = excluded.candidate_id          
    , username            = excluded.username
    , contract_type       = excluded.contract_type
    , res_id              = excluded.res_id
    , status              = excluded.status
    , c_owner             = excluded.c_owner
    , source_type         = excluded.source_type
    , profile_title       = excluded.profile_title 
    , first_name          = excluded.first_name
    , middle_name         = excluded.middle_name
    , last_name           = excluded.last_name
    , address1            = excluded.address1
    , address2            = excluded.address2
    , city                = excluded.city
    , state               = excluded.state
    , country             = excluded.country
    , zip                 = excluded.zip 
    , availability_status = excluded.availability_status
    , currency            = excluded.currency
    , date_of_birth       = excluded.date_of_birth
    , resume_file_name    = excluded.resume_file_name
    , job_title           = excluded.job_title
    , phones              = excluded.phones
    , source              = excluded.source
    , notes               = excluded.notes
	  
where
		(
			CASE
               when  target.candidate_id        != excluded.candidate_id then 1
                when target.username            != excluded.username then 1
                when target.contract_type       != excluded.contract_type then 1
                when target.res_id              != excluded.res_id then 1
                when target.status              != excluded.status then 1
                when target.c_owner             != excluded.c_owner then 1
                when target.source_type         != excluded.source_type then 1 
                when target.profile_title       != excluded.profile_title  then 1
                when target.first_name          != excluded.first_name then 1
                when target.middle_name         != excluded.middle_name then 1
                when target.last_name           != excluded.last_name then 1
                when target.address1            != excluded.address1 then 1
                when target.address2            != excluded.address2 then 1
                when target.city                != excluded.city then 1
                when target.state               != excluded.state then 1
                when target.country             != excluded.country then 1
                when target.zip                 != excluded.zip then 1
                when target.availability_status != excluded.availability_status then 1
                when target.currency            != excluded.currency then 1
                when target.date_of_birth       != excluded.date_of_birth then 1
                when target.resume_file_name    != excluded.resume_file_name then 1
                when target.job_title           != excluded.job_title then 1
                when target.phones              != excluded.phones then 1
                when target.source              != excluded.source then 1
                when target.notes               != excluded.notes then 1
                ELSE 0
                END
         ) = 1;

