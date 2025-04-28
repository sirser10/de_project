drop table if exists dds.dim_candidate_addresses;

create table if not exists dds.dim_candidate_addresses(
														id serial
														, candidate_id int 
														, address1 text
														, address2 text
														, city text
														, state text 
														, country text 
														, zip int
														, valid_from_dt date
														, valid_to_dt date
														, updated_dttm timestamp 
														, primary key(candidate_id, valid_from_dt)
);

insert into dds.dim_candidate_addresses(
										 candidate_id
										, address1
										, address2
										, city
										, state
										, country
										, zip
										, valid_from_dt
										, valid_to_dt
										, updated_dttm
)
with _ as (
	select 									
			candidate_id
			, address1
			, address2
			, city
			, state
			, country
			, zip
			, updated_dttm :: date valid_from_dt
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
		, address1
		, address2
		, city
		, state
		, country
		, zip
		, valid_from_dt
		, valid_to_dt
		, updated_dttm
		
from valid_to_dt; 
