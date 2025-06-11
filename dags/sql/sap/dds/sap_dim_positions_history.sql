drop table if exists dds.sap_dim_positions_history;
create table if not exists dds.sap_dim_positions_history(
												  ms_position_id int
												, ms_org_unit_id int
												, description text
                                                , budget_code_id bigint
												, budget_code_name text 
												, new_position char(10)
												, date_entry date
												, valid_from_dt date
												, valid_to_dt date
												, updated_dttm timestamp
                                                , primary key (
                                                                ms_position_id
                                                            ,   ms_org_unit_id
                                                            ,   valid_from_dt
                                                            ,   valid_to_dt
                                                )
);

insert into dds.sap_dim_positions_history (
    WITH cte AS (
        SELECT 
              ms_position_id 
            , ms_org_unit_id
            , description
            , budget_code_id
            , budget_code_name
            , new_position
            , date_entry
            , updated_dttm :: date as updated_dt
            , ROW_NUMBER() OVER (PARTITION BY ms_position_id  ORDER BY updated_dttm asc) AS rn
        FROM ods.sap_dim_positions
    )
    ,
    cte2 AS (
        SELECT 
                c1.ms_position_id
            , c1.ms_org_unit_id
            , c1.description
            , c1.budget_code_id
            , c1.budget_code_name
            , c1.new_position
            , c1.date_entry
            , c1.updated_dt as valid_from_dt
            , c1.rn 
            , case 
				 	when c2.updated_dt is not null then c2.updated_dt - interval '1 day'
				 	else '9999-12-31'
            end :: date as valid_to_dt

        FROM cte c1
        LEFT JOIN cte c2
            ON c1.ms_position_id = c2.ms_position_id 
                    and c1.rn = c2.rn - 1
)
SELECT 
    	ms_position_id
      , ms_org_unit_id
      , description
      , budget_code_id
      , budget_code_name
      , new_position
      , date_entry
      , valid_from_dt
      , valid_to_dt
      , current_timestamp as updated_dttm
    
FROM cte2

);