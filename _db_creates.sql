

-------------------------------------------------------------------------------
-- Tables
-------------------------------------------------------------------------------

-- drop table if exists extractor.energyday;
-- drop table if exists extractor.powerday;

create table extractor.energyday (
    sourcefile varchar(50) not null,
	ts timestamp null,
	kwh varchar(20) not null,
	primary key (sourcefile, ts)
);
create index ndx_ex_energydayts on extractor.energyday (ts);

create table extractor.powerday (
    sourcefile varchar(50) not null,
	ts timestamp null,
	watts varchar(20) not null,
	primary key (sourcefile, ts)
);
create index ndx_ex_powerdayts on extractor.powerday (ts);

-------------------------------------------------------------------------------
-- Views
-------------------------------------------------------------------------------
select
	ed.*
from
	extractor.energyday ed
;
select
	*
from
	extractor.powerday pd
where
	ts >= '2025-02-01'
	;



-- drop view if exists  extractor.energy_by_day;
create or replace view extractor.energy_by_day as
select
	extract (year from ts)  yr
	, extract (month from ts)  mnth
	, extract (week from ts)  wk
	, extract (day from ts)  dy
	, ts::date fulldate
	, sum(ed.kwh::float) day_kwh
	, count(*) day_readings
	, case when count(*)=24 then 1 else 0 end day_has_all_readings
from
	extractor.energyday ed
group by
	extract (year from ts)
	, extract (month from ts)
	, extract (week from ts)
	, extract (day from ts)
	, ts::date
;
select * from extractor.energy_by_day order by fulldate;

-- drop view if exists extractor.energy_by_week;
create or replace view extractor.energy_by_week as
select
	ebd.yr
	, ebd.wk
	, sum(ebd.day_kwh) week_kwh
	, sum(ebd.day_readings) readings
	, count(*) days
	, case when sum(ebd.day_has_all_readings)=7 then 1 else 0 end wk_has_all_readings
from
	extractor.energy_by_day ebd
group by
	ebd.yr
	, ebd.wk
;
select * from extractor.energy_by_week order by yr, wk;


-- drop view if exists extractor.energy_by_month;
create or replace view extractor.energy_by_month as
select
	ebd.yr
	, ebd.mnth
	, sum(ebd.day_kwh) month_kwh
	, sum(ebd.day_readings) readings
	, count(*) days
	, case
		when ebd.mnth in (1,3,5,7,8,10,12) and sum(ebd.day_has_all_readings)=31 then 1
		when ebd.mnth in (4,6,9,11) and sum(ebd.day_has_all_readings)=30 then 1
		when ebd.mnth = 2 then
			case
				when (ebd.yr % 4 = 0) AND ((ebd.yr % 100 <> 0) OR (ebd.yr % 400 = 0)) and sum(ebd.day_has_all_readings)=29 then 1
				else 0
			end
		else 0
	  end month_has_all_readings
from
	extractor.energy_by_day ebd
group by
	ebd.yr
	, ebd.mnth
;
select * from extractor.energy_by_month order by yr, mnth;



