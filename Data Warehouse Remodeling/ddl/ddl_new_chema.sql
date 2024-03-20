-- drops
drop table if exists shipping_country_rates;
drop table if exists shipping_agreement;
drop table if exists shipping_transfer;
drop table if exists shipping_info;
drop table if exists shipping_status;
drop view if exists shipping_datamart;

--truncates
truncate shipping_country_rates;
truncate shipping_agreement;
truncate shipping_transfer;
truncate shipping_info cascade;
truncate shipping_status;

---------------------------------

-- creating directory "shipping_country_rates"
-- from "shipping_country" and "shipping_country_base_rate" of the "shipping" table.

--shipping_country_rates creation
create table if not exists shipping_country_rates (
	id serial4 not null primary key,
	shipping_country text null,
	shipping_country_base_rate numeric(14, 3) null
);


-- creating directory "shipping_agreement"
-- from "vendor_agreement_description" of the "shipping" table.

--shipping_agreement creation
create table if not exists shipping_agreement (
	agreement_id bigint primary key,
	agreement_number text null,
	agreement_rate numeric(14, 3) null,
	agreement_commission numeric(14, 3) null
);


-- creating directory "shipping_transfer"
-- from "shipping_transfer_description " of the "shipping" table.

--shipping_transfer creation
create table if not exists shipping_transfer (
	id serial4 not null primary key,
	transfer_type text null,
	transfer_model text null,
	shipping_transfer_rate numeric(14, 3) null
);


-- creating table "shipping_info"
-- from "shipping_country_rates", "shipping_agreement", "shipping_transfer" tables.
-- And "shipping_plan_datetime", "payment_amount", "vendor_id" from "shipping" table.

-- shipping_info creation
create table if not exists shipping_info (
	shipping_id bigint not null primary key,
	vendor_id bigint null,
	payment_amount numeric(14, 2) null,
	shipping_plan_datetime timestamp null,
	shipping_transfer_id bigint,
	shipping_agreement_id bigint,
	shipping_country_rate_id bigint,
	
	foreign key (shipping_transfer_id) references public.shipping_transfer (id) on update cascade,
	foreign key (shipping_agreement_id) references shipping_agreement (agreement_id) on update cascade,
	foreign key (shipping_country_rate_id) references shipping_country_rates (id) on update cascade
);


-- creating table "shipping_status"
-- from "status" and "state" columns from "shipping" 
-- calculating "shipping_start_fact_datetime" - date when booked, 
-- and "shipping_end_fact_datetime" date when recieved.
		
-- shipping_status creation
create table if not exists shipping_status (
	shipping_id bigint not null primary key,
	status text null,
	state text null,
	shipping_start_fact_datetime timestamp null,
	shipping_end_fact_datetime timestamp null
);

