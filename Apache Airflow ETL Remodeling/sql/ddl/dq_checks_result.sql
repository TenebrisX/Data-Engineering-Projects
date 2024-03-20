create schema IF NOT EXISTS Stage;

create table if not exists staging.dq_checks_results (
	Table_name varchar(255),
	DQ_check_name varchar(255),
	Datetime timestamp,
	DQ_check_result numeric(8,2) check (DQ_check_result <= 1 and DQ_check_result >= 0)
	
);

select * from stage.dq_checks_results dcr
where dcr.dq_check_result = 1;