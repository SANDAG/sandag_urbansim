libname sql_dbo odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=dbo;

libname sql_edd odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_edd;

libname sql_lehd odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=lehd_lodes;

libname sql_bea odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=bea;

libname st "T:\socioec\socioec_data_test\SAS\Tables";

proc sql;
create table esri_0 as select * from sql_dbo.BUSINESS_LOCATIONS_2015_BLK2010(obs=100);

create table esri_1 as select locnum,naics_6,empnum format=comma10.,zip,blockid10 as blk
,case when int(naics_6/1000) in (721,722) then int(naics_6/1000) else int(naics_6/10000) end as naics
from sql_dbo.BUSINESS_LOCATIONS_2015_BLK2010 where naics_6^=.
order by empnum desc;

create table esri_1a as select naics,sum(empnum) as emp from esri_1 group by naics;
create table esri_1b as select sum(empnum) as emp from esri_1;
quit;

proc sql;
create table test_1 as select * from b_1 where naics=.;
quit;

proc sql;
create table naics_sandag_industry as select * from sql_edd.xref_naics_sandag_industry;
create table sandag_industry_edd as select * from sql_edd.xref_sandag_industry_edd_sector;
quit;

proc sql;
create table esri_2 as select x.*,y.sandag_industry_id
from esri_1 as x left join sql_edd.xref_naics_sandag_industry as y on x.naics=y.naics
where y.yr=2013;

create table esri_2a as select sandag_industry_id,sum(empnum) as esri_2015 from esri_2 group by sandag_industry_id;
update esri_2a set sandag_industry_id=152325 where sandag_industry_id=15;

create table esri_2b as
select * from esri_2a union all
select 1819 as sandag_industry_id,sum(esri_2015) as esri_2015 from esri_2a where sandag_industry_id in (18,19);
quit;

proc sql;
create table edd_micro_1 as select sandag_industry_id,sum(emp_adj) as edd_2013a,sum(emp3) as edd_2013b
FROM sql_edd.emp_2013 where sandag_industry_id^=.
group by sandag_industry_id;
quit;

proc sql;
create table edd_emp_1 as select y.sandag_industry_id format=8.,x.description,x.yr,x.employment as edd_emp
from sql_edd.sd_industry_employment as x inner join sql_edd.xref_sandag_industry_edd_sector as y
on x.ss_naics=y.edd_sector where x.yr in (2012,2013,2014);

create table edd_emp_2 as select x.*,y.edd_2013,u.edd_2013a,u.edd_2013b,z.edd_2014
from (select sandag_industry_id,description,sum(edd_emp) as edd_2012 from edd_emp_1 where yr=2012 group by sandag_industry_id,description) as x
inner join (select sandag_industry_id,sum(edd_emp) as edd_2013 from edd_emp_1 where yr=2013 group by sandag_industry_id) as y
on x.sandag_industry_id=y.sandag_industry_id
inner join (select sandag_industry_id,sum(edd_emp) as edd_2014 from edd_emp_1 where yr=2014 group by sandag_industry_id) as z
on x.sandag_industry_id=z.sandag_industry_id
inner join edd_micro_1 as u on x.sandag_industry_id=u.sandag_industry_id;

create table edd_emp_3 as
select * from edd_emp_2 union all
select 1819 as sandag_industry_id,"Accomodation+Food Services & Drinking Places" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (18,19) union all
select 152325 as sandag_industry_id,"Educational Services (Private and Government)" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (15,23,25) union all

select 2324 as sandag_industry_id,"State Government" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (23,24) union all

select 2526 as sandag_industry_id,"Local Government" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (25,26) union all

select 2122 as sandag_industry_id,"Federal Government (civilian)" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (21,22) union all

select 21222426 as sandag_industry_id,"Government (excluding education)" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (21,22,24,26) union all

select 2325 as sandag_industry_id,"State and Local Government (education)" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (23,25) union all

select 80000000 as sandag_industry_id,"Government (civilian)" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2 where sandag_industry_id in (21,22,23,24,25,26) union all

select 90000000 as sandag_industry_id,"Total" as description
,sum(edd_2012) as edd_2012,sum(edd_2013) as edd_2013,sum(edd_2013a) as edd_2013a,sum(edd_2013b) as edd_2013b
,sum(edd_2014) as edd_2014
from edd_emp_2
order by sandag_industry_id;
quit;



proc sql;
create table cbp_1 as select yr,industry_code,emp,est,payroll_q1 from st.cbp_all
where (industry_code in ("721","722") or length(industry_code)=2) and 
yr in (2012,2013) and fips_state="06" and fips_county="073";
quit;

proc sql;
create table cbp_2 as select x.sandag_industry_id,y.yr,sum(y.emp) as cbp_emp
from naics_sandag_industry as x inner join cbp_1 as y on x.naics=input(y.industry_code,3.0)
group by sandag_industry_id,y.yr;

create table cbp_3 as
select * from cbp_2 union all
select 1819 as sandag_industry_id,yr,sum(cbp_emp) as cbp_emp from cbp_2 where sandag_industry_id in (18,19)
group by yr;
quit;


proc sql;
create table wac_1(drop=segment) as select *
from sql_lehd.wac(keep=w_geoid yr cn: type segment)
where yr in (2012,2013,2014) and substr(w_geoid,1,5)="06073" and type in ("JT00","JT02") /* all jobs and all private jobs*/
and segment="S000"
order by yr,w_geoid,type;
quit;

proc transpose data=wac_1 out=wac_2(drop=_label_);by yr w_geoid type;run;

proc sql;
create table wac_3 as select yr,w_geoid
,case when type="JT02" then "Priv" else "Tot" end as type
,case
when input(substr(_name_,4,2),2.0)=1 then 11
when input(substr(_name_,4,2),2.0)=2 then 21
when input(substr(_name_,4,2),2.0)=3 then 22
when input(substr(_name_,4,2),2.0)=4 then 23
when input(substr(_name_,4,2),2.0)=5 then 31
when input(substr(_name_,4,2),2.0)=6 then 42
when input(substr(_name_,4,2),2.0)=7 then 44
when input(substr(_name_,4,2),2.0)=8 then 48
when input(substr(_name_,4,2),2.0)=9 then 51
when input(substr(_name_,4,2),2.0)=10 then 52
when input(substr(_name_,4,2),2.0)=11 then 53
when input(substr(_name_,4,2),2.0)=12 then 54
when input(substr(_name_,4,2),2.0)=13 then 55
when input(substr(_name_,4,2),2.0)=14 then 56
when input(substr(_name_,4,2),2.0)=15 then 61
when input(substr(_name_,4,2),2.0)=16 then 62
when input(substr(_name_,4,2),2.0)=17 then 71
when input(substr(_name_,4,2),2.0)=18 then 72
when input(substr(_name_,4,2),2.0)=19 then 81
when input(substr(_name_,4,2),2.0)=20 then 92 end as naics,sum(col1) as j
from wac_2 where col1>0 group by yr,w_geoid,calculated type,naics
order by yr,w_geoid,naics,type;

create table wac_3a as select yr,naics,type,sum(j) as j from wac_3 group by yr,naics,type;
quit;

proc transpose data=wac_3 out=wac_4(drop=_name_);by yr w_geoid naics;var j;id type;run;

proc sql;
update wac_4 set priv=0 where naics=92 and priv>0;

create table wac_5 as select yr,w_geoid,naics,coalesce(tot,0) as tot,coalesce(priv,0) as priv
,coalesce(tot,0)-coalesce(priv,0) as gov
from wac_4 order by yr,w_geoid,naics;

create table wac_5a as select yr,naics,sum(tot) as tot,sum(priv) as priv,sum(gov) as gov from wac_5
group by yr,naics;

create table wac_6 as select w_geoid,yr,
case
when naics=11 then 1
when naics=21 then 2
when naics=22 then 3
when naics=23 then 4
when naics=31 then 5
when naics=32 then 5
when naics=33 then 5
when naics=42 then 6
when naics=44 then 7
when naics=45 then 7
when naics=48 then 8
when naics=49 then 8
when naics=51 then 9
when naics=52 then 10
when naics=53 then 11
when naics=54 then 12
when naics=55 then 13
when naics=56 then 14
when naics=61 then 15
when naics=62 then 16
when naics=71 then 17
when naics=81 then 20
when naics=72 then 1819 end as sandag_industry_id,priv as emp from wac_5
	union all
select w_geoid,yr,2325 as sandag_industry_id,gov as emp from wac_5 where naics=61
	union all

select w_geoid,yr,21222426 as sandag_industry_id,sum(gov) as emp from wac_5 where naics^=61 group by w_geoid,yr
	union all

select w_geoid,yr,152325 as sandag_industry_id,tot as emp from wac_5 where naics=61
	union all
select w_geoid,yr,80000000 as sandag_industry_id,sum(gov) as emp from wac_5 group by w_geoid,yr
	union all
select w_geoid,yr,90000000 as sandag_industry_id,sum(tot) as emp from wac_5 group by w_geoid,yr;

create table wac_6a as select yr,sandag_industry_id,sum(emp) as emp from wac_6 group by yr,sandag_industry_id;

create table wac_6a_ as select * from wac_6 where sandag_industry_id=. and emp>0;

create table wac_6b as select x.sandag_industry_id,x.emp as wac_2012,y.emp as wac_2013,z.emp as wac_2014
from wac_6a as x
left join wac_6a as y on x.sandag_industry_id=y.sandag_industry_id
left join wac_6a as z on x.sandag_industry_id=z.sandag_industry_id
where x.yr=2012 and y.yr=2013 and z.yr=2014
order by sandag_industry_id;
quit;

proc sql;
create table test as select * from wac_5 where w_geoid="060730083602006" and yr=2012;
quit;

/*
proc sql;
create table esri_3 as select int(naics_6/1000) as naics_3,sum(empnum) as emp,count(*) as est
from sql_dbo.BUSINESS_LOCATIONS_2015_BLK2010 where int(naics_6/10000)=62
group by naics_3;

create table esri_4 as select int(naics_6/100) as naics_4,sum(empnum) as emp,count(*) as est
from sql_dbo.BUSINESS_LOCATIONS_2015_BLK2010 where int(naics_6/10000)=62
group by naics_4;

create table cbp_3 as select industry_code,emp,est,payroll_q1 from st.cbp_all
where industry_code in ("621","622","623","624") and yr in (2013) and fips_state="06" and fips_county="073";

create table cbp_4 as select industry_code,emp,est,payroll_q1 from st.cbp_all
where length(industry_code)=4 and substr(industry_code,1,2)="62" and yr in (2013) and fips_state="06" and fips_county="073";

create table esri_cbp_1 as select x.naics_4
,x.emp as esri_emp,y.emp as cbp_emp,x.emp-y.emp as emp_d
,x.est as esri_est,y.est as cbp_est,x.est-y.est as est_d
from esri_4 as x left join cbp_4 as y on x.naics_4=input(y.industry_code,4.0)
order by naics_4;
quit;
*/


proc sql;
create table bea_1 as select
x.line_id
,y.line_title,y.industry_code
,x.value
,case
when x.line_id="L_0070" then 1
when x.line_id="L_0100" then 1
when x.line_id="L_0200" then 2
when x.line_id="L_0300" then 3
when x.line_id="L_0400" then 4
when x.line_id="L_0500" then 5
when x.line_id="L_0600" then 6
when x.line_id="L_0700" then 7
when x.line_id="L_0800" then 8
when x.line_id="L_0900" then 9
when x.line_id="L_1000" then 10
when x.line_id="L_1100" then 11
when x.line_id="L_1200" then 12
when x.line_id="L_1300" then 13
when x.line_id="L_1400" then 14
when x.line_id="L_1500" then 15
when x.line_id="L_1600" then 16
when x.line_id="L_1700" then 17
when x.line_id="L_1800" then 1819
when x.line_id="L_1900" then 20
when x.line_id="L_2001" then 2122
when x.line_id="L_2011" then 2324
when x.line_id="L_2012" then 2526 end as sandag_industry_id
from (select * from sql_bea.reg_economic_accounts where fips=6073 and table_id="CA25N" and yr=2013) as x
left join sql_bea.reg_economic_accounts_codes as y
on x.table_id=y.table_id and x.line_id=y.line_id
order by line_id;

create table bea_2 as
select sandag_industry_id,sum(value) as bea_2013 from bea_1 where sandag_industry_id^=. group by sandag_industry_id
	union all
select 90000000 as sandag_industry_id,sum(value) as bea_2013 from bea_1 where sandag_industry_id^=.
	union all
select 80000000 as sandag_industry_id,sum(value) as bea_2013 from bea_1 where sandag_industry_id in (2122,2324,2526)
;
quit;

proc sql;
create table emp_1 as select x.*
,y.esri_2015
,z1.cbp_emp as cbp_2012,z2.cbp_emp as cbp_2013
,u.wac_2012,u.wac_2013,u.wac_2014
,v.bea_2013
,v.bea_2013-edd_2013 as bea_edd_2013
from edd_emp_3(drop=edd_2013a) as x
left join esri_2b as y on x.sandag_industry_id=y.sandag_industry_id
left join (select * from cbp_3 where yr=2012) as z1 on x.sandag_industry_id=z1.sandag_industry_id
left join (select * from cbp_3 where yr=2013) as z2 on x.sandag_industry_id=z2.sandag_industry_id
left join wac_6b as u on x.sandag_industry_id=u.sandag_industry_id
left join bea_2 as v on x.sandag_industry_id=v.sandag_industry_id
order by sandag_industry_id;
quit;

/*
proc sql;
create table emp_2013_blk_1 as select input(blockid10,15.) as block_id format=15.0,sandag_industry_id as sector_id
,sum(emp_adj) as emp from sql_dbo.EMP_2013_BLK2010
where blockid10^="" and sandag_industry_id^=. and emp_adj^=.
group by block_id,sector_id;
quit;

proc sql;
create table emp_2013_blk_1a as select input(blockid10,15.) as block_id format=15.0,sandag_industry_id as sector_id
,sum(emp1) as emp1,sum(emp2) as emp2,sum(emp3) as emp3
from sql_dbo.EMP_2013_BLK2010
where blockid10^="" and sandag_industry_id^=. and emp_adj^=.
group by block_id,sector_id;
quit;

proc sql;
create table emp_2013_blk_1b as select 
coalesce(x.block_id,y.block_id) as block_id
,coalesce(x.sector_id,y.sector_id) as sector_id
,coalesce(x.emp,0) as emp
,coalesce(y.emp1,0) as emp1
,coalesce(y.emp2,0) as emp2
,coalesce(y.emp3,0) as emp3
from emp_2013_blk_1 as x 
full join emp_2013_blk_1a as y on x.block_id=y.block_id and x.sector_id=y.sector_id
order by block_id,sector_id;
quit;


data emp_2013_blk_2(drop=i emp);set emp_2013_blk_1;
do i=1 to emp;output;end;
run;

proc sort data=emp_2013_blk_2;by block_id sector_id;run;

data emp_2013_blk_3;retain block_id job_id sector_id;set emp_2013_blk_2;job_id=_n_;run;


proc sql;
create table emp_2013_blk_3a as select distinct sector_id from emp_2013_blk_3;
quit;
*/


/*
proc export data=emp_2013_blk_3 outfile="T:\socioec\urbansim\data\employment\jobs_2013_edd_blk.csv"
dbms=csv replace;run;
*/

proc sql;
create table test as select blockid10,input(blockid10,15.) as block_id format=15.0
from sql_dbo.EMP_2013_BLK2010(obs=10);
quit;

proc sql;
create table emp_2013_blk_1 as select input(blockid10,15.) as block_id format=15.0,sandag_industry_id as sector_id
,sum(emp_adj) as emp from sql_dbo.EMP_2013_BLK2010
where blockid10^="" and sandag_industry_id^=. and emp_adj>0
group by block_id,sector_id;

create table emp_2013_bg_1 as select int(block_id/1000) as bg_id,sector_id,sum(emp) as emp
from emp_2013_blk_1 group by bg_id,sector_id;

create table emp_2013_ct_1 as select int(block_id/10000) as ct_id,sector_id,sum(emp) as emp
from emp_2013_blk_1 group by ct_id,sector_id;

create table emp_2013_cn_1 as select sector_id,sum(emp) as emp
from emp_2013_blk_1 group by sector_id;

create table emp_2013_blk_1a as select *
,case
when sector_id in (18,19) then 1819
when sector_id in (23,25) then 2325
when sector_id in (21,22,24,26) then 21222426 else sector_id end as sector_id2
,emp/sum(emp) as f
from emp_2013_blk_1 group by block_id,sector_id2;

create table emp_2013_bg_1a as select *
,case
when sector_id in (18,19) then 1819
when sector_id in (23,25) then 2325
when sector_id in (21,22,24,26) then 21222426 else sector_id end as sector_id2
,emp/sum(emp) as f
from emp_2013_bg_1 group by bg_id,sector_id2; 

create table emp_2013_ct_1a as select *
,case
when sector_id in (18,19) then 1819
when sector_id in (23,25) then 2325
when sector_id in (21,22,24,26) then 21222426 else sector_id end as sector_id2
,emp/sum(emp) as f
from emp_2013_ct_1 group by ct_id,sector_id2; 

create table emp_2013_cn_1a as select *
,case
when sector_id in (18,19) then 1819
when sector_id in (23,25) then 2325
when sector_id in (21,22,24,26) then 21222426 else sector_id end as sector_id2
,emp/sum(emp) as f
from emp_2013_cn_1 group by sector_id2; 
quit;




proc sql;
create table wac_7 as select input(x.w_geoid,15.) as block_id format=15.
,x.sandag_industry_id,x.emp
,v.sector_id,v.f as f_cn
,y.f as f_blk,y.emp as emp_edd as emp_edd_blk
,y.f as f_bg
,u.f as f_ct
from (select w_geoid,sandag_industry_id,emp from wac_6
where emp>0 and yr=2013 and (sandag_industry_id<=20 or sandag_industry_id in (1819,2325,21222426))) as x
left join emp_2013_cn_1a as v on x.sandag_industry_id=v.sector_id2
left join emp_2013_blk_1a as y on input(x.w_geoid,15.)=y.block_id and x.sandag_industry_id=y.sector_id2 and v.sector_id=y.sector_id
left join emp_2013_bg_1a as z on int(input(x.w_geoid,15.)/1000)=z.bg_id and x.sandag_industry_id=z.sector_id2 and v.sector_id=z.sector_id
left join emp_2013_ct_1a as u on int(input(x.w_geoid,15.)/10000)=u.ct_id and x.sandag_industry_id=u.sector_id2 and v.sector_id=u.sector_id

order by block_id,sector_id;

create table wac_7a as select * from wac_7 where sandag_industry_id>20 and f_bg=.;
create table wac_7b as select * from wac_7 where sandag_industry_id>20 and f_bg=. and f_ct=.;
create table wac_7c as select * from wac_7 where sandag_industry_id>20 and f_bg=. and f_ct=. and f_cn=.;
quit;

proc sql;
create table wac_test as select * from wac_7
where emp>0 and sandag_industry_id in (1819)
order by block_id;
quit;


proc sql;
create table wac_8 as select block_id
,case
when sandag_industry_id<=20 then sandag_industry_id
else sector_id end as sector_id
,case
when sandag_industry_id<=20 then emp
when f_blk^=. then int(emp*f_blk)
when f_bg^=. then int(emp*f_bg)
when f_ct^=. then int(emp*f_ct)
else int(emp*f_cn) end as emp
from wac_7;

create table wac_8a as select block_id,sector_id,emp,emp/sum(emp) as f
from wac_8 where emp>0 group by sector_id;

create table wac_8b as select x.sector_id,x.emp format=comma8.,y.emp as emp_edd format=comma8.,z.cbp_emp as emp_cbp format=comma8.
,z.cbp_emp-y.emp as cbp_edd_d format=comma8.
/*
,case
when z.cbp_emp=. then y.emp
when y.emp>z.cbp_emp then y.emp
else z.cbp_emp end as emp_target
*/
,y.emp as emp_target
from (select sector_id,sum(emp) as emp from wac_8a group by sector_id) as x
left join emp_2013_cn_1 as y on x.sector_id=y.sector_id
left join (select * from cbp_3 where yr=2013 and sandag_industry_id>3) as z on x.sector_id=z.sandag_industry_id;

create table wac_8c as select sum(emp_edd) as emp_edd format=comma9.,sum(emp_target) as emp_target format=comma9.

,sum(cbp_edd_d) as cbp_edd_d format=comma9.,sum(emp_target)-sum(emp_edd) as emp_d format=comma9.
from wac_8b;
quit;

proc sql;
create table wac_9 as select x.*,y.emp_target,ceil(x.f*y.emp_target) as j
from wac_8a as x left join wac_8b as y on x.sector_id=y.sector_id
order by sector_id,j desc;
quit;

data wac_9a;set wac_9;by sector_id;retain jc;
if first.sector_id then do;j1=min(j,emp_target);jc=j1;end;
else do;j1=min(j,emp_target-jc);jc=jc+j1;end;
l=last.sector_id;
run;

proc sql;
create table wac_9b as select * from wac_9a where l=1 and jc^=emp_target;
quit;

proc sql;
create table wac_9c as select block_id,sector_id,j1 as emp from wac_9a
order by block_id,sector_id;
quit;

data wac_9c(drop=i emp);set wac_9c;
do i=1 to emp;output;end;
run;

proc sort data=wac_9c;by block_id sector_id;run;

data wac_10;retain block_id job_id sector_id;format block_id 14.job_id 7. sector_id 2.;
set wac_9c;job_id=_n_;run;


libname sql_sc odbc noprompt="driver=SQL Server; server=sql2014a8; database=spacecore;bulkload=yes;dbcommit=10000;
Trusted_Connection=yes" schema=input;

/*
proc sql;
drop table sql_sc.jobs_edd_2013;
create table sql_sc.jobs_edd_2013 as select * from wac_10;
quit;
*/


