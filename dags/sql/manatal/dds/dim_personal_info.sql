drop table if exists dds.dim_personal_info;
create table if not exists dds.dim_personal_info(
												id serial
												, candidate_id int
												, user_name text
												, profile_title text
												, first_name text
												, middle_name text
												, last_name text 
												, date_of_birth date 
												, phone text 
												, valid_from_dt date
												, valid_to_dt date
												, updated_dttm timestamp 
												, primary key (candidate_id, valid_from_dt)
);

insert into dds.dim_personal_info(
									candidate_id
									, user_name
									, profile_title 
									, first_name
									, middle_name 
									, last_name 
									, date_of_birth
									, phone
									, valid_from_dt
									, valid_to_dt 
									, updated_dttm
)
with _ as (
		select 
			candidate_id
			, username
			, profile_title
			, first_name
			, middle_name
			, last_name
			, date_of_birth
			, phones
			, updated_dttm :: date as valid_from_dt
			, current_timestamp as updated_dttm
			, row_number() over (partition by candidate_id order by updated_dttm) as rn 
		from ods.manual_interviewed_data
	) 
, valid_to_dt as (
	select 
		t1.*
		, case 
			when t2.valid_from_dt is not null then t2.valid_from_dt - interval '1 day' 
			else '9999-12-31' 
		 end :: date as valid_to_dt
	from _ t1
	left join _ t2 on  t1.candidate_id = t2.candidate_id and t1.rn = t2.rn -1 
)
select 
		candidate_id
		, username
		, profile_title
		, first_name
		, middle_name
		, last_name
		, date_of_birth
		, phones
		, valid_from_dt
		, valid_to_dt
		, updated_dttm
from valid_to_dt;
