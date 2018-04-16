----2916 total parcels with SFU units only (no MFU), there are 8 sites with both SFU and MFU and 4801 total parcels with any sched dev (mfu or sfu)   
--select * from urbansim.urbansim.scheduled_development_parcel
--where sfu > 0 and mfu > 0 

--select DISTINCT site_id from urbansim.urbansim.scheduled_development_parcel
--where sfu > 0 and mfu = 0 

--select * 
--from urbansim.urbansim.scheduled_development_parcel

--***************************************SFU PROJECTS ONLY************************

drop table if exists #sfu_parcels; 

select s.*, gp.gplu, p.proportion_undevelopable, p.parcel_acres 
,(1 - p.proportion_undevelopable) as proportion_developable
,CASE 
	  WHEN p.proportion_undevelopable IS NULL THEN parcel_acres 
	  WHEN p.proportion_undevelopable IS NOT NULL THEN (parcel_acres - (parcel_acres*p.proportion_undevelopable))
	  END AS developable_acres
--insert variable to keep track of allocation case (how we determined how to allocate the units to a parcel) 
	  ,NULL as case_id  
into #sfu_parcels
from urbansim.urbansim.scheduled_development_parcel as s 
join urbansim.urbansim.general_plan_parcel as gp on s.parcel_id = gp.parcel_id 
join spacecore.urbansim.parcels as p on s.parcel_id = p.parcel_id 
where sfu > 0 and mfu = 0 
order by site_id; 

--select * from #sfu_parcels order by site_id, parcel_id 
select distinct site_id from #sfu_parcels 
--select * from #sfu_sites order by site_id 

--change developable acreage for those parcels which do not have any residential land uses according to GPLU
update #sfu_parcels
set developable_acres = 0
from #sfu_parcels
where gplu not in(1000,1100,1200,9600,9700) ;

drop table if exists #sfu_parcels_0;

select *
,row_number () over(partition by #sfu_parcels.site_id order by developable_acres DESC) as row_num 
into #sfu_parcels_0
from #sfu_parcels ;

--set row number variable to null when there is no developable acreage on the parcel, so that when we allocate residuals later, those parcels will not be assigned units
update #sfu_parcels_0
set row_num = NULL 
from #sfu_parcels_0
where developable_acres = 0 ;

--select * from #sfu_parcels_0 order by site_id

/*count the number of parcel_id's by site_id, if the count of parcels = the number of SFU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

drop table if exists #sfu_parcel_count 

select 
site_id
,min(sfu) as sfu 
,count(parcel_id) as parcel_count
,CAST(NULL as numeric(36,18)) as units_per_dev_acre
INTO #sfu_parcel_count
from #sfu_parcels_0
where sfu >0 and mfu = 0 
--and developable_acres <>0
group by site_id
order by site_id  ;

--select * from #sfu_parcel_count order by site_id

/*count the number of parcel_id's by site_id, if the count of parcels = the number of SFU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

drop table if exists #sfu_sites;

select 
s.site_id
,min(s.sfu) as sfu 
,min(pc.parcel_count) as parcel_count 
,CAST(NULL as numeric(36,18)) as sfu_per_dev_acre
,CAST(NULL as int) as sfu_effective
,CAST(NULL as int) as row_num
INTO #sfu_sites
from #sfu_parcels_0 as s 
join #sfu_parcel_count as pc on s.site_id = pc.site_id 
where s.sfu >0 and s.mfu = 0 
--and developable_acres <>0
group by s.site_id
order by s.site_id ;

--select * from #sfu_parcels_0 order by site_id 
--select * from #sfu_sites order by site_id 
--select * from #sfu_parcel_count order by site_id
--select distinct site_id from #sfu_sites 
--select distinct site_id from #sfu_parcels_0 
--select distinct site_id from #sfu_parcel_count
 
/*CASE 1: if there are the same number of parcels as SFUs then assign one to one*/
/*CASE 2: if there are is only one parcel for the site_id then assign all units to one parcel*/

drop table if exists #sfu_parcels_2 ;

select 
#sfu_parcels_0.site_id 
,parcel_id
,startdate
,compdate
,civemp
,milemp
,#sfu_parcels_0.sfu
,#sfu_sites.parcel_count
,mhu
,gplu
,parcel_acres
,proportion_developable
,proportion_undevelopable
,developable_acres
,#sfu_parcels_0.row_num
,(#sfu_parcels_0.sfu - parcel_count) as sfu_difference
,CASE WHEN ((#sfu_parcels_0.sfu - parcel_count) = 0)  THEN 1                   --Case 1: when the number of parcels in a site_id = the number of units in the site_id 
WHEN parcel_count = 1  
--and #sfu_parcels_0.row_num = 1 
THEN #sfu_parcels_0.sfu  --Case 2: when there is only one developable parcel for the site_id, assign all units to the one parcel 
END as sfu_effective 
,CASE WHEN ((#sfu_parcels_0.sfu - parcel_count) =0) THEN 'case 1'  
WHEN parcel_count = 1 THEN 'case 2'             
END as case_id
INTO #sfu_parcels_2 
from #sfu_parcels_0 
join #sfu_sites on #sfu_parcels_0.site_id = #sfu_sites.site_id ;

--select * from #sfu_parcels_2 

--select sfu  from #sfu_parcels_2 
--group by site_id 
--having sum(developable_acres) = 0 
--order by site_id 

--Case 3: assign one unit to one parcel where there are more parcels than units, assigned based on the rank, where parcels are ranked by largest amount of developable acreage
--some parcels will have zero units  
update #sfu_parcels_2 
set case_id = 'case 3' 
where parcel_count > sfu and sfu_effective IS NULL ;

update #sfu_parcels_2 
set sfu_effective =  1 
where case_id = 'case 3' and row_num < =sfu ;

update #sfu_parcels_2 
set sfu_effective =  0 
where case_id = 'case 3' and sfu_effective IS NULL  ;

--Case 4: When we have more units than we have parcels many to more than one, but less than the number of SFU (but parcel count is less than units) 
--sum up acreage of all parcels in a site_id, divide the number of units by total developable acreage per site_id, multiply this fraction by the developable acreage for 
--each parcel_id, deal with the residual 
update #sfu_parcels_2 
set case_id = 'case 4' 
where parcel_count < sfu and parcel_count > 1 and case_id IS NULL ;


--calculate  the units per dev acre for all sites where developable acres > 0 
--there are 3 sites where there are no parcels with any developable acreage: 19007, 6026, 19013
update s
set sfu_per_dev_acre = (s.sfu)/(developable_acres) 
--,sum(developable_acres) as sum_dev_acres
--,sum(developable_acres) * (sfu)/(developable_acres) sfu_test
from  #sfu_sites as s
join(select site_id, (sum(developable_acres)) as developable_acres, min(sfu) as sfu  from #sfu_parcels_2 group by site_id having sum(developable_acres)  >0)  as p on s.site_id = p.site_id;

--select * from #sfu_sites
--select * from #sfu_parcels_2 order by site_id

----to see which rounding works better 
--select #parcels_4.*
--,units_per_dev_acre
--,ROUND (developable_acres * units_per_dev_acre,0) as calc_units
--,CEILING (developable_acres * units_per_dev_acre) as up_calc_units
--,FLOOR  (developable_acres * units_per_dev_acre) as down_calc_units
--from #parcels_5 
--join #parcels_4 on #parcels_4.site_id = #parcels_5.site_id 
--where sfu > parcel_count and parcel_count > 1
--order by site_id, developable_acres


--select site_id
--,min(sfu) as sfu 
--,sum(calc_units)      as sum_calc
--,sum(up_calc_units)   as sum_up_calc
--,sum(down_calc_units) as sum_down_calc
--from #parcels_6
--group by site_id
--order by site_id


--select #parcels_4.site_id, min(sfu) as sfu, sum(FLOOR  (developable_acres * units_per_dev_acre)) as down_calc_units, min(sfu) - sum(FLOOR  (developable_acres * units_per_dev_acre)) as diff_units 
--from #parcels_4 
--join #parcels_5 on #parcels_4.site_id = #parcels_5.site_id 
--group by #parcels_4.site_id, sfu
--having sfu != sum(FLOOR  (developable_acres * units_per_dev_acre)) 
--order by min(sfu) - sum(FLOOR  (developable_acres * units_per_dev_acre))


--multiply units_per_dev_acre * developable_acres to get sfu effective where we have more units than we have parcels (Case 4) 
update p 
set sfu_effective = floor(s.sfu_per_dev_acre * p.developable_acres) 
from #sfu_parcels_2 as p  
join #sfu_sites as s on s.site_id = p.site_id 
where case_id = 'case 4' and p.row_num IS NOT NULL; 

--select * from #sfu_parcels_2 order by site_id

--assign the number of units that were not assigned by multiplying units_per_dev_acre * developable_acres, assign that difference to parcels by site_id based row_number 
with x as (
select 
site_id
,min(sfu) as sfu 
,sum(sfu_effective)       as sfu_assigned 
,min(sfu) - sum(sfu_effective) as sfu_assigned_diff
from #sfu_parcels_2
where case_id = 'case 4' 
group by site_id 
) 
update p  
set sfu_effective = sfu_effective + 1 
from #sfu_parcels_2 as p 
join x on p.site_id = x.site_id
where case_id = 'case 4' and row_num < = x.sfu_assigned_diff; 

select distinct site_id from #sfu_parcels_2 
order by site_id 

select distinct site_id from #sfu_sites -- this should be 364 

--check to see if all sfus are assigned
--select 
--site_id
--,min(sfu) as sfu 
--,sum(sfu_effective)       as sfu_assigned
--,min(sfu) -  sum(sfu_effective) as sfu_diff 
--from #sfu_parcels_2 
--group by site_id 
--order by site_id;

--**************************************************************************************MFU and MHU PROJECTS ONLY******************************************************************************************
drop table if exists #mfu_parcels;

select s.*, gp.gplu, p.proportion_undevelopable, p.parcel_acres 
,(1 - p.proportion_undevelopable) as proportion_developable
,CASE 
	  WHEN p.proportion_undevelopable IS NULL THEN parcel_acres 
	  WHEN p.proportion_undevelopable IS NOT NULL THEN (parcel_acres - (parcel_acres*p.proportion_undevelopable))
	  END AS developable_acres
--insert variable to keep track of allocation case (how we determined how to allocate the units to a parcel) 
	  ,NULL as case_id  
into #mfu_parcels
from urbansim.urbansim.scheduled_development_parcel as s 
join urbansim.urbansim.general_plan_parcel as gp on s.parcel_id = gp.parcel_id 
join spacecore.urbansim.parcels as p on s.parcel_id = p.parcel_id 
where (mfu > 0 and sfu = 0) or (s.site_id = 3006) or (sfu = 0 and mhu > 0)  
order by site_id ;

--select * from #mfu_parcels order by site_id, parcel_id 
select distinct site_id from #mfu_parcels 
--select * from #mfu_sites order by site_id 

--change developable acreage for those parcels which do not have any residential land uses according to GPLU
update #mfu_parcels
set developable_acres = 0
from #mfu_parcels
where gplu not in(1000,1100,1200,1300,9600,9700) ;

drop table if exists #mfu_parcels_0;

select *
,row_number () over(partition by #mfu_parcels.site_id order by developable_acres DESC) as row_num 
into #mfu_parcels_0
from #mfu_parcels ;

--set row number variable to null when there is no developable acreage on the parcel, so that when we allocate residuals later, those parcels will not be assigned units
update #mfu_parcels_0
set row_num = NULL 
from #mfu_parcels_0
where developable_acres = 0 ;

--select * from #mfu_parcels_0 order by site_id

/*count the number of parcel_id's by site_id, if the count of parcels = the number of mfU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/
drop table if exists #mfu_parcel_count;

select 
site_id
,min(mfu) as mfu 
,count(parcel_id) as parcel_count
,CAST(NULL as numeric(36,18)) as units_per_dev_acre
INTO #mfu_parcel_count
from #mfu_parcels_0
where (mfu >0 and sfu = 0) or (site_id = 3006) or (sfu = 0 and mhu > 0)  
--and developable_acres <>0
group by site_id
order by site_id  ;

--select * from #mfu_parcel_count order by site_id
--select distinct site_id from #mfu_parcel_count --these should be 249
--select distinct site_id from #mfu_parcels_0
--select distinct site_id from #mfu_sites 

/*count the number of parcel_id's by site_id, if the count of parcels = the number of mfU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

drop table if exists #mfu_sites;

select 
s.site_id
,min(s.mfu) as mfu 
,min(pc.parcel_count) as parcel_count 
,CAST(NULL as numeric(36,18)) as mfu_per_dev_acre
,CAST(NULL as int) as mfu_effective
,CAST(NULL as int) as mhu_effective
,CAST(NULL as int) as row_num
INTO #mfu_sites
from #mfu_parcels_0 as s 
join #mfu_parcel_count as pc on s.site_id = pc.site_id 
where (s.mfu >0 and s.sfu = 0) or (s.site_id = 3006) or (sfu = 0 and mhu > 0)
--and developable_acres <>0
group by s.site_id
order by s.site_id ;

--select * from #mfu_parcels_0 order by site_id 
--select * from #mfu_sites order by site_id 
--select * from #mfu_parcel_count order by site_id

 
/*CASE 1: if there are the same number of parcels as mfUs then assign one to one*/
/*CASE 2: if there are is only one parcel for the site_id then assign all units to one parcel*/

drop table if exists #mfu_parcels_2;

select 
#mfu_parcels_0.site_id 
,parcel_id
,startdate
,compdate
,civemp
,milemp
,#mfu_parcels_0.mfu
,#mfu_sites.parcel_count
,mhu
,gplu
,parcel_acres
,proportion_developable
,proportion_undevelopable
,developable_acres
,#mfu_parcels_0.row_num
,(#mfu_parcels_0.mfu - parcel_count) as mfu_difference
,CASE WHEN ((#mfu_parcels_0.mfu - parcel_count) = 0)  THEN 1					--Case 1: when the number of parcels in a site_id = the number of units in the site_id MFU
WHEN parcel_count = 1  
--and #mfu_parcels_0.row_num = 1 
THEN #mfu_parcels_0.mfu															--Case 2: when there is only one developable parcel for the site_id, assign all units to the one parcel MFU
END as mfu_effective 
,CASE WHEN ((#mfu_parcels_0.mhu - parcel_count) = 0)  THEN 1					--Case 1: when the number of parcels in a site_id = the number of units in the site_id MHU
WHEN parcel_count = 1  
--and #mfu_parcels_0.row_num = 1 
THEN #mfu_parcels_0.mhu															--Case 2: when there is only one developable parcel for the site_id, assign all units to the one parcel MHU
END as mhu_effective 
,CASE WHEN (((#mfu_parcels_0.mfu - parcel_count) =0) or ((#mfu_parcels_0.mhu - parcel_count)=0))  THEN 'case 1'  
WHEN parcel_count = 1 THEN 'case 2'             
END as case_id
INTO #mfu_parcels_2 
from #mfu_parcels_0 
join #mfu_sites on #mfu_parcels_0.site_id = #mfu_sites.site_id ;

--select * from #mfu_parcels_2 

--Case 3: assign one unit to one parcel where there are more parcels than units, assigned based on the rank, where parcels are ranked by largest amount of developable acreage
--some parcels will have zero units  
--update #mfu_parcels_2 
--set case_id = 'case 3' 
--where parcel_count > mfu and mfu_effective IS NULL 

--update #mfu_parcels_2 
--set mfu_effective =  1 
--where case_id = 'case 3' and row_num < =mfu 

--update #mfu_parcels_2 
--set mfu_effective =  0 
--where case_id = 'case 3' and mfu_effective IS NULL  

--Case 4: When we have more units than we have parcels many to more than one, but less than the number of mfU (but parcel count is less than units) 
--sum up acreage of all parcels in a site_id, divide the number of units by total developable acreage per site_id, multiply this fraction by the developable acreage for 
--each parcel_id, deal with the residual 
update #mfu_parcels_2 
set case_id = 'case 4' 
where parcel_count < mfu and parcel_count > 1 and case_id IS NULL ;


--calculate  the units per dev acre for all sites where developable acres > 0 
update s
set mfu_per_dev_acre = (s.mfu)/(developable_acres) 
--,sum(developable_acres) as sum_dev_acres
--,sum(developable_acres) * (mfu)/(developable_acres) mfu_test
from  #mfu_sites as s
join(select site_id, (sum(developable_acres)) as developable_acres, min(mfu) as mfu  from #mfu_parcels_2 group by site_id having sum(developable_acres)  >0)  as p on s.site_id = p.site_id;

--select * from #mfu_sites
--select * from #mfu_parcels_2 order by site_id


--multiply units_per_dev_acre * developable_acres to get mfu effective where we have more units than we have parcels (Case 4) 
update p 
set mfu_effective = floor(s.mfu_per_dev_acre * p.developable_acres) 
from #mfu_parcels_2 as p  
join #mfu_sites as s on s.site_id = p.site_id 
where case_id = 'case 4' and p.row_num IS NOT NULL; 

--select * from #mfu_parcels_2 order by site_id

--assign the number of units that were not assigned by multiplying units_per_dev_acre * developable_acres, assign that difference to parcels by site_id based row_number 
with x as (
select 
site_id
,min(mfu) as mfu 
,sum(mfu_effective)       as mfu_assigned 
,min(mfu) - sum(mfu_effective) as mfu_assigned_diff
from #mfu_parcels_2
where case_id = 'case 4' 
group by site_id 
) 
update p  
set mfu_effective = mfu_effective + 1 
from #mfu_parcels_2 as p 
join x on p.site_id = x.site_id
where case_id = 'case 4' and row_num < = x.mfu_assigned_diff ;

----check to see if all mfus are assigned
--select 
--site_id
--,min(mfu) as mfu 
--,sum(mfu_effective)       as mfu_assigned
--,min(mfu) -  sum(mfu_effective) as mfu_diff 
--from #mfu_parcels_2 
--group by site_id 
--order by site_id;
select distinct site_id from #mfu_parcels_2

--**********************************************************************************SFU and MFU projects****************************************************************************************
--select distinct gplu 
--from urbansim.urbansim.scheduled_development_parcel as p 
--join urbansim.urbansim.general_plan_parcel as gp on p.parcel_id = gp.parcel_id
--where sfu > 0 and mfu = 0 
----where sfu > 0 and mfu > 0 
----where sfu = 0 and mfu > 0 
--order by gplu

--select distinct site_id 
--from urbansim.urbansim.scheduled_development_parcel as p 
--where sfu > 0 and mfu > 0 

--1100 SF residential 
--1200 MF residential
--2103 Light Indutry-General 
--5002 Regional Shopping Center 
--5003 Community Shopping Center 
--6100 Public Services 
--6109 Other Public Services 
--7601 Park-Active 
--7603 Open Space/Preserve
--9600 SPA
--9700 Mixed Use 

drop table if exists #parcel_count;

select 
site_id
,min(sfu) as sfu 
,min(mfu) as mfu
,count(parcel_id) as parcel_count
,CAST(NULL as numeric(36,18)) as units_per_dev_acre
INTO #parcel_count
from urbansim.urbansim.scheduled_development_parcel
where sfu >0 and mfu >0
group by site_id
order by site_id  ;

--select * from #parcel_count


drop table if exists #sf_mf_parcels;

select s.*, gp.gplu, p.proportion_undevelopable, p.parcel_acres 
,(1 - p.proportion_undevelopable) as proportion_developable
,CASE 
	  WHEN p.proportion_undevelopable IS NULL THEN parcel_acres 
	  WHEN p.proportion_undevelopable IS NOT NULL THEN (parcel_acres - (parcel_acres*p.proportion_undevelopable))
	  END AS developable_acres
--insert variable to keep track of allocation case (how we determined how to allocate the units to a parcel) 
	  ,NULL as case_id  
into #sf_mf_parcels
from urbansim.urbansim.scheduled_development_parcel as s 
join urbansim.urbansim.general_plan_parcel as gp on s.parcel_id = gp.parcel_id 
join spacecore.urbansim.parcels as p on s.parcel_id = p.parcel_id 
where sfu > 0 and mfu > 0 
order by site_id ;

--select * from #sf_mf_parcels 
select * from #sf_mf_parcels

/*count the number of parcel_id's by site_id, if the count of parcels = the number of SFU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

drop table if exists #sf_mf_sites;

select 
s.site_id
,min(s.sfu) as sfu 
,min(s.mfu) as mfu
,min(pc.parcel_count) as parcel_count 
,CAST(NULL as numeric(36,18)) as mfu_per_dev_acre
,CAST(NULL as numeric(36,18)) as sfu_per_dev_acre
,CAST(NULL as int) as mfu_effective
,CAST(NULL as int) as sfu_effective
,CAST(NULL as int) as row_num
INTO #sf_mf_sites
from urbansim.urbansim.scheduled_development_parcel as s 
join #parcel_count as pc on s.site_id = pc.site_id 
where s.sfu >0 and s.mfu >0
group by s.site_id
order by s.site_id ;

--select * from #sf_mf_sites 

--change developable acreage for those parcels which do not have any residential land uses according to GPLU
update #sf_mf_parcels
set developable_acres = 0
from #sf_mf_parcels
where gplu not in(1100,1200,9600,9700) ;

drop table if exists #sf_mf_parcels_0;

select *
,row_number () over(partition by #sf_mf_parcels.site_id order by developable_acres DESC) as row_num 
into #sf_mf_parcels_0
from #sf_mf_parcels ;

--set row number variable to null when there is no developable acreage on the parcel, so that when we allocate residuals later, those parcels will not be assigned units
update #sf_mf_parcels_0
set row_num = NULL 
from #sf_mf_parcels_0
where developable_acres = 0 ;

--select * from #sf_mf_parcels_0 order by site_id, row_num 

/*CASE 1: does not apply to projects with both SFU and MFU*/
/*CASE 2: if there are is only one parcel for the site_id then assign all SF and MF units to one parcel*/
drop table if exists #sf_mf_parcels_2;

select 
#sf_mf_parcels_0.site_id 
,parcel_id
,startdate
,compdate
,civemp
,milemp
,#sf_mf_parcels_0.sfu
,#sf_mf_sites.parcel_count
,#sf_mf_parcels_0.mfu
,mhu
,gplu
,parcel_acres
,proportion_developable
,proportion_undevelopable
,developable_acres
,(#sf_mf_parcels_0.sfu - parcel_count) as sfu_difference
,(#sf_mf_parcels_0.mfu - parcel_count) as mfu_difference
,#sf_mf_parcels_0.row_num 
,CASE WHEN (#sf_mf_parcels_0.sfu - parcel_count) = 0   THEN 1                                --Case 1: when the number of parcels in a site_id = the number of units in the site_id 
WHEN parcel_count = 1 and #sf_mf_parcels_0.row_num = 1 THEN #sf_mf_parcels_0.sfu             --Case 2: when there is only one parcel for the site_id, assign all units to the one parcel 
END as sfu_effective 
,CASE WHEN (#sf_mf_parcels_0.mfu - parcel_count) = 0   THEN 1                                --Case 1: when the number of parcels in a site_id = the number of units in the site_id 
WHEN parcel_count = 1 
--and #sf_mf_parcels_0.row_num = 1 
THEN #sf_mf_parcels_0.mfu             --Case 2: when there is only one parcel for the site_id, assign all units to the one parcel 
END as mfu_effective 
,CASE WHEN parcel_count = 1 THEN 'case 2'             
END as case_id
--,row_number () over(partition by #sf_mf_parcels_0.site_id order by developable_acres DESC) as row_num
INTO #sf_mf_parcels_2 
from #sf_mf_parcels_0 
join #sf_mf_sites  on #sf_mf_parcels_0.site_id = #sf_mf_sites.site_id ;



--select * from #sf_mf_parcels_2 order by site_id, row_num 
select distinct site_id from #sf_mf_parcels_2

--Case 4: When we have more units than we have parcels, many to more than one, but less than the number of MFU (but parcel count is less than units) 
--sum up acreage of all parcels in a site_id, divide the number of units by total developable acreage per site_id, multiply this fraction by the developable acreage for 
--each parcel_id, deal with the residual 
update #sf_mf_parcels_2 
set case_id = 'case 4' 
where parcel_count < mfu and parcel_count < sfu and parcel_count > 1 and case_id IS NULL ;

--calculate the units per dev acre for all sites where developable acres > 0 
update s
set mfu_per_dev_acre = (s.mfu)/(developable_acres) 
,sfu_per_dev_acre = (s.sfu)/(developable_acres) 
from  #sf_mf_sites as s
join(select site_id, (sum(developable_acres)) as developable_acres, min(mfu) as mfu, min(sfu) as sfu from #sf_mf_parcels_2 group by site_id having sum(developable_acres)  >0)  as p on s.site_id = p.site_id;

--select * from #sf_mf_sites order by site_id
--select * from #sf_mf_parcels_2 order by site_id
--site_id 2014, the parcel does not have any lus that are residential, but since there is only one parcel in the site_id, all the units are assigned to the one parcel.

update #sf_mf_parcels_2
set sfu_effective = sfu 
, mfu_effective = mfu 
where site_id = 2014  and gplu = 6109;


--multiply units_per_dev_acre * developable_acres to get mfu effective where we have more units than we have parcels (Case 4) 
--first restrict to only parcels with lu 1200 or 9600 or 9700, if there are no parcels with those land uses, the units will be allocated later, using the row_num on any residential land use parcel
--look at site_id 2011 to see where this is an issue--there are 2 parcels, one has mfu lu and one has sf lu, want to make sure these are allocated correctly. 
update p 
set mfu_effective = floor(s.mfu_per_dev_acre * p.developable_acres)
from #sf_mf_parcels_2 as p  
join #sf_mf_sites as s on s.site_id = p.site_id 
where case_id = 'case 4' and gplu in(1200,9600,9700) ;

--assign units for projects where there are no lu's that have MF but do have SF allowed
--specifically check 1890 and 3060
with x as (
select 
site_id
,min(mfu) as mfu
,ISNULL(sum(mfu_effective),0)              as mfu_assigned 
,ISNULL((min(mfu) - sum(mfu_effective)),0) as mfu_assigned_diff
from #sf_mf_parcels_2
where case_id = 'case 4' 
group by site_id 
)
update p 
set mfu_effective = floor(y.mfu_per_dev_acre * p.developable_acres)
from #sf_mf_parcels_2 as p  
join x			       on x.site_id = p.site_id 
join #sf_mf_sites as y on y.site_id = x.site_id 
where case_id = 'case 4' and gplu in(1100) and x.mfu_assigned = 0 ;

--select * from #sf_mf_parcels_2 order by site_id

--MFU's 
--assign the number of units that were not assigned by multiplying units_per_dev_acre * developable_acres, assign that difference to parcels by site_id based row_number 
with x as (
select 
site_id
,min(mfu) as mfu
,ISNULL(sum(mfu_effective),0)              as mfu_assigned 
,ISNULL((min(mfu) - sum(mfu_effective)),0) as mfu_assigned_diff
from #sf_mf_parcels_2
where case_id = 'case 4' 
group by site_id 
) 
update p  
set mfu_effective = ISNULL(mfu_effective,0) + mfu_assigned_diff 
from #sf_mf_parcels_2 as p 
join x on p.site_id = x.site_id
where case_id = 'case 4' and row_num = 1 and mfu_assigned_diff >0 ;

--select * from #sf_mf_parcels_2 

--same as above but with the SFUs 
--first restrict to only parcels with lu 1100 or 9600 or 9700, if there are no parcels with those land uses, the units will be allocated later, using the row_num on any residential land use parcel
update p 
set sfu_effective = floor(s.sfu_per_dev_acre * p.developable_acres)
from #sf_mf_parcels_2 as p  
join #sf_mf_sites as s on s.site_id = p.site_id 
where case_id = 'case 4' and gplu in(1100,9600,9700) ;

--assign units for projects where there are no lu's that have MF but do have SF allowed
--specifically check 1890 and 3060
with x as (
select 
site_id
,min(sfu) as sfu
,ISNULL(sum(sfu_effective),0)              as sfu_assigned 
,ISNULL((min(sfu) - sum(sfu_effective)),0) as sfu_assigned_diff
from #sf_mf_parcels_2
where case_id = 'case 4' 
group by site_id 
)
update p 
set sfu_effective = floor(y.sfu_per_dev_acre * p.developable_acres)
from #sf_mf_parcels_2 as p  
join x			       on x.site_id = p.site_id 
join #sf_mf_sites as y on y.site_id = x.site_id 
where case_id = 'case 4' and gplu in(1200) and x.sfu_assigned = 0 ;

--select * from #sf_mf_parcels_2 order by site_id

--SFU's 
--assign the number of units that were not assigned by multiplying units_per_dev_acre * developable_acres, assign that difference to parcels by site_id based row_number 
with x as (
select 
site_id
,min(sfu) as sfu 
,ISNULL(sum(sfu_effective),0)              as sfu_assigned 
,ISNULL((min(sfu) - sum(sfu_effective)),0) as sfu_assigned_diff
from #sf_mf_parcels_2
where case_id = 'case 4' 
group by site_id 
) 
update p  
set sfu_effective = ISNULL(sfu_effective,0) + sfu_assigned_diff
from #sf_mf_parcels_2 as p 
join x on p.site_id = x.site_id
where case_id = 'case 4' and row_num = 1 and sfu_assigned_diff >0;

select distinct site_id from #sf_mf_parcels_2


--select
--site_id  
--,min(sfu) as sfu
--,sum(sfu_effective) as sfu_effective 
--,min(mfu) as mfu
--,sum(mfu_effective) as mfu_effective 
--from #sf_mf_parcels_2 
--group by site_id
--order by site_id;


----select * from #sf_mf_parcels_2 
--select
--site_id  
--,min(mfu) as mfu
--,sum(mfu_effective) as mfu_effective 
--from #mfu_parcels_2 
--group by site_id
--order by site_id;

----select * from #mfu_parcels_2 order by site_id
--select
--site_id  
--,min(sfu) as sfu
--,sum(sfu_effective) as sfu_effective 
--from #sfu_parcels_2 
--group by site_id
--order by site_id;

--select * from #sfu_parcels_2 order by site_id


--drop table #sched_dev_all
--select * into #sched_dev_all from (

use urbansim;

drop table if exists urbansim.urbansim.sched_dev_all;

select * into urbansim.urbansim.sched_dev_all

from (
select NULL as scenario, site_id, parcel_id, startdate, compdate, civemp, milemp, sfu, mfu, mhu, ISNULL(sfu_effective,0) as sfu_effective, ISNULL(mfu_effective,0) as mfu_effective, case_id 
from #sf_mf_parcels_2

union all

select NULL as scenario, site_id, parcel_id, startdate, compdate, civemp, milemp, sfu, 0 as mfu, mhu, ISNULL(sfu_effective,0) as sfu_effective, 0 as mfu_effective, case_id 
from #sfu_parcels_2

union all

select NULL as scenario, site_id, parcel_id, startdate, compdate, civemp, milemp, 0 as sfu, mfu, mhu, 0 as sfu_effective, ISNULL(mfu_effective,0) as sfu_effective, case_id 
from #mfu_parcels_2)
as temp;

select distinct site_id from urbansim.urbansim.sched_dev_all
--select * from [urbansim].[scheduled_development_parcel]
--where site_id in(3006,3089,3311,3324,15037,16028)

----select * from #sched_dev_all
--select * from urbansim.urbansim.sched_dev_all
--where site_id = 1727
----where sfu_effective > 250 or mfu_effective > 250

--update #sched_dev_all
--set compdate = '2024-12-31'
--where (sfu_effective <= 250 and sfu_effective >0) or (mfu_effective <= 250 and mfu_effective >0)

--select * 
--from #sched_dev_all as a 
--join #sched_dev_all as b on  a.site_id = b.site_id 

--update urbansim.urbansim.sched_dev_all
--set scenario = 1 
--CASE WHEN compdate IS NULL THEN startdate = 2017 
--set compdate 