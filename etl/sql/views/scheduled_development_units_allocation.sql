USE urbansim
;
----2916 total parcels with SFU units only (no MFU), there are 8 sites with both SFU and MFU and 4801 total parcels with any sched dev (mfu or sfu)   
--select * from spacecore.urbansim.scheduled_development_parcels
--where sfu > 0 and mfu > 0 

--select DISTINCT site_id from spacecore.urbansim.scheduled_development_parcels
--where sfu > 0 and mfu = 0 

--select * 
--from spacecore.urbansim.scheduled_development_parcels

--***************************************SFU PROJECTS ONLY************************

DROP TABLE IF EXISTS #sfu_parcels; 

SELECT 
	sdp.ogr_fid
	,sdp.site_id
	,sdp.parcel_id
	,sds.sfu
	,sds.mfu
	,sds.mhu
	,sds.startdate
	,sds.compdate
	,sds.devtypeid
	,gpp.gplu
	,usp.proportion_undevelopable
	,usp.parcel_acres 
	,(1 - usp.proportion_undevelopable) AS proportion_developable
	,CASE 
		WHEN usp.proportion_undevelopable IS NULL THEN parcel_acres 
		WHEN usp.proportion_undevelopable IS NOT NULL THEN (parcel_acres - (parcel_acres * usp.proportion_undevelopable))
	END AS developable_acres
--insert variable to keep track of allocation case (how we determined how to allocate the units to a parcel) 
	,NULL AS case_id  
INTO #sfu_parcels
FROM spacecore.urbansim.scheduled_development_parcels AS sdp
JOIN spacecore.gis.scheduled_development_sites AS sds
	ON sdp.ogr_fid = sds.ogr_fid
JOIN spacecore.urbansim.general_plan_parcels AS gpp
	ON sdp.parcel_id = gpp.parcel_id 
JOIN spacecore.urbansim.parcels AS usp
	ON sdp.parcel_id = usp.parcel_id
WHERE sds.sfu > 0
	AND sds.mfu = 0 
	AND sds.status <> 'completed'
ORDER BY
	sdp.ogr_fid
	,sdp.site_id
; 

SELECT DISTINCT site_id FROM #sfu_parcels 

--change developable acreage for those parcels which do not have any residential land uses according to GPLU
UPDATE sfup
	SET developable_acres = 0
FROM #sfu_parcels AS sfup
WHERE gplu not in(1000,1100,1200,9600,9700)
AND site_id NOT IN(							--OVERWRITE EXCEPTIONS
28
,1860
,13004
);


DROP TABLE IF EXISTS #sfu_parcels_0
;
SELECT
	*
	,ROW_NUMBER () OVER(PARTITION BY sfup.ogr_fid ORDER BY developable_acres DESC) AS row_num 
INTO #sfu_parcels_0
FROM #sfu_parcels AS sfup;
;
SELECT * FROM #sfu_parcels_0

--set row number variable to null when there is no developable acreage on the parcel, so that when we allocate residuals later, those parcels will not be assigned units
UPDATE sfup
	SET row_num = NULL 
FROM #sfu_parcels_0 AS sfup
WHERE developable_acres = 0
;


/*count the number of parcel_id's by site_id, if the count of parcels = the number of SFU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

DROP TABLE IF EXISTS #sfu_parcel_count 
;
SELECT 
	ogr_fid
	,site_id
	,MIN(sfu) AS sfu 
	,COUNT(parcel_id) AS parcel_count
	,CAST(NULL AS numeric(36,18)) AS units_per_dev_acre
INTO #sfu_parcel_count
FROM #sfu_parcels_0
WHERE sfu > 0 AND mfu = 0 
--and developable_acres <>0
GROUP BY
	ogr_fid
	,site_id
ORDER BY
	ogr_fid
	,site_id
;


/*count the number of parcel_id's by site_id, if the count of parcels = the number of SFU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

DROP TABLE IF EXISTS #sfu_sites;
;
SELECT 
	s.ogr_fid
	,s.site_id
	,MIN(s.sfu) AS sfu 
	,MIN(pc.parcel_count) AS parcel_count 
	,CAST(NULL AS numeric(36,18)) AS sfu_per_dev_acre
	,CAST(NULL AS int) as sfu_effective
	,CAST(NULL AS int) as row_num
INTO #sfu_sites
FROM #sfu_parcels_0 AS s 
JOIN #sfu_parcel_count AS pc
	ON s.ogr_fid = pc.ogr_fid
WHERE s.sfu > 0 AND s.mfu = 0 
--and developable_acres <>0
GROUP BY
	s.ogr_fid
	,s.site_id
ORDER BY
	s.ogr_fid
	,s.site_id
;
SELECT * FROM #sfu_sites
 
/*CASE 1: if there are the same number of parcels as SFUs then assign one to one*/
/*CASE 2: if there are is only one parcel for the site_id then assign all units to one parcel*/

drop table if exists #sfu_parcels_2 ;

select 
#sfu_parcels_0.site_id 
,parcel_id
,startdate
,compdate
--,civemp
--,milemp
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



--check to see if all sfus are assigned
select 
site_id
,min(sfu) as sfu 
,sum(sfu_effective)       as sfu_assigned
,min(sfu) -  sum(sfu_effective) as sfu_diff 
from #sfu_parcels_2 
group by site_id 
order by site_id;

--**************************************************************************************MFU and MHU PROJECTS ONLY******************************************************************************************
DROP TABLE IF EXISTS #mfu_parcels; 
;
SELECT 
	sdp.ogr_fid
	,sdp.site_id
	,sdp.parcel_id
	,sds.sfu
	,sds.mfu
	,sds.mhu
	,sds.civemp
	,sds.milemp
	,sds.startdate
	,sds.compdate
	,sds.devtypeid
	,gpp.gplu
	,usp.proportion_undevelopable
	,usp.parcel_acres 
	,(1 - usp.proportion_undevelopable) AS proportion_developable
,CASE 
	  WHEN usp.proportion_undevelopable IS NULL THEN parcel_acres
	  WHEN sdp.site_id IN(11003, 15035) THEN parcel_acres		--OVERWRITE EXCEPTIONS
	  WHEN usp.proportion_undevelopable IS NOT NULL THEN (parcel_acres - (parcel_acres * usp.proportion_undevelopable))
	  END AS developable_acres
--insert variable to keep track of allocation case (how we determined how to allocate the units to a parcel) 
	  ,NULL as case_id  
INTO #mfu_parcels
FROM spacecore.urbansim.scheduled_development_parcels as sdp
JOIN spacecore.gis.scheduled_development_sites AS sds
	ON sdp.ogr_fid = sds.ogr_fid
JOIN spacecore.urbansim.general_plan_parcels as gpp
	ON sdp.parcel_id = gpp.parcel_id 
JOIN spacecore.urbansim.parcels AS usp
	ON sdp.parcel_id = usp.parcel_id 
WHERE (sds.mfu > 0 and sds.sfu = 0) or (sdp.site_id = 3006) or (sds.sfu = 0 and sds.mhu > 0)  
ORDER BY
	sdp.ogr_fid
	,sdp.site_id
;

SELECT DISTINCT site_id FROM #mfu_parcels
SELECT * FROM #mfu_parcels ORDER BY site_id, ogr_fid


--change developable acreage for those parcels which do not have any residential land uses according to GPLU
update #mfu_parcels
set developable_acres = 0
from #mfu_parcels
where gplu not in(1000,1100,1200,1300,9600,9700)
AND site_id NOT IN(							--OVERWRITE EXCEPTIONS
1685
,1745
,1748
,1761
,1859
,2015
,3149
,3181
,11003
,12010
,14075
,14081
,15035
,17003
,17006
);


drop table if exists #mfu_parcels_0;

select *
,row_number () over(partition by sfup.ogr_fid, sfup.site_id order by developable_acres DESC) as row_num 
into #mfu_parcels_0
from #mfu_parcels AS sfup
;

--set row number variable to null when there is no developable acreage on the parcel, so that when we allocate residuals later, those parcels will not be assigned units
update #mfu_parcels_0
set row_num = NULL 
from #mfu_parcels_0
where developable_acres = 0 ;


------OVERWRITE EXCEPTION, FIX MULTI SITE ID
----UPDATE mfup
----SET mfu = sds.mfu
----FROM #mfu_parcels_0 AS mfup
----JOIN (SELECT siteid, SUM(mfu) AS mfu FROM [gis].[scheduled_development_sites] GROUP BY siteid) AS sds	--OVERWRITE EXCEPTIONS
----	ON mfup.site_id = sds.siteid
----WHERE mfup.site_id = 15035


/*count the number of parcel_id's by site_id, if the count of parcels = the number of mfU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/
drop table if exists #mfu_parcel_count;

select 
	sds.ogr_fid
	,site_id
,min(sds.mfu) as mfu																					--OVERWRITE EXCEPTIONS
,count(parcel_id) as parcel_count
,CAST(NULL as numeric(36,18)) as units_per_dev_acre
INTO #mfu_parcel_count
from #mfu_parcels_0 AS mfu
JOIN (SELECT ogr_fid, siteid, SUM(mfu) AS mfu FROM [spacecore].[gis].[scheduled_development_sites] WHERE status <> 'completed' GROUP BY ogr_fid, siteid) AS sds	--OVERWRITE EXCEPTIONS
	ON mfu.site_id = sds.siteid AND mfu.ogr_fid = sds.ogr_fid
where (sds.mfu >0 and sfu = 0) or (site_id = 3006) or (sfu = 0 and mhu > 0)								--OVERWRITE EXCEPTIONS
--and developable_acres <>0
GROUP BY
	sds.ogr_fid
	,site_id
ORDER BY
	sds.ogr_fid
	,site_id
;


/*count the number of parcel_id's by site_id, if the count of parcels = the number of mfU then you can allocate one to one*/
/*if there is only one parcel, all the units go to that parcel*/

drop table if exists #mfu_sites;

select 
	s.ogr_fid
	,s.site_id
,min(sds.mfu) as mfu																					--OVERWRITE EXCEPTIONS
,min(pc.parcel_count) as parcel_count 
,CAST(NULL as numeric(36,18)) as mfu_per_dev_acre
,CAST(NULL as int) as mfu_effective
,CAST(NULL as int) as mhu_effective
,CAST(NULL as int) as row_num
INTO #mfu_sites
from #mfu_parcels_0 as s 
join #mfu_parcel_count as pc on s.site_id = pc.site_id AND s.ogr_fid = pc.ogr_fid
JOIN (SELECT ogr_fid, siteid, SUM(mfu) AS mfu FROM [spacecore].[gis].[scheduled_development_sites] GROUP BY ogr_fid, siteid) AS sds	--OVERWRITE EXCEPTIONS
	ON s.site_id = sds.siteid AND s.ogr_fid = sds.ogr_fid
where (sds.mfu >0 and s.sfu = 0) or (s.site_id = 3006) or (sfu = 0 and mhu > 0)
--and developable_acres <>0
GROUP BY
	s.ogr_fid
	,s.site_id
ORDER BY
	s.ogr_fid
	,s.site_id
;

SELECT * FROM #mfu_sites WHERE site_id = 15035

----SELECT * FROM #mfu_parcels_0 WHERE site_id = 15035

 
/*CASE 1: if there are the same number of parcels as mfUs then assign one to one*/
/*CASE 2: if there are is only one parcel for the site_id then assign all units to one parcel*/

drop table if exists #mfu_parcels_2;

select 
	#mfu_parcels_0.ogr_fid
	,#mfu_parcels_0.site_id 
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
join #mfu_sites on #mfu_parcels_0.site_id = #mfu_sites.site_id AND #mfu_parcels_0.ogr_fid = #mfu_sites.ogr_fid
	AND #mfu_parcels_0.ogr_fid = #mfu_sites.ogr_fid
;


--select * from #mfu_parcels_2 ORDER BY site_id

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
join(select ogr_fid, site_id, (sum(developable_acres)) as developable_acres, min(mfu) as mfu  from #mfu_parcels_2 group by ogr_fid, site_id having sum(developable_acres)  >0)  as p on s.site_id = p.site_id AND s.ogr_fid = p.ogr_fid;

--select * from #mfu_sites
--select * from #mfu_parcels_2 order by site_id


--multiply units_per_dev_acre * developable_acres to get mfu effective where we have more units than we have parcels (Case 4) 
update p 
set mfu_effective = floor(s.mfu_per_dev_acre * p.developable_acres) 
from #mfu_parcels_2 as p  
join #mfu_sites as s on s.site_id = p.site_id AND s.ogr_fid = p.ogr_fid
where case_id = 'case 4' and p.row_num IS NOT NULL; 

--select * from #mfu_parcels_2 order by site_id

--assign the number of units that were not assigned by multiplying units_per_dev_acre * developable_acres, assign that difference to parcels by site_id based row_number 
with x as (
select 
ogr_fid
,site_id
,min(mfu) as mfu 
,sum(mfu_effective)       as mfu_assigned 
,min(mfu) - sum(mfu_effective) as mfu_assigned_diff
from #mfu_parcels_2
where case_id = 'case 4' 
group by ogr_fid, site_id 
) 
update p  
set mfu_effective = mfu_effective + 1 
from #mfu_parcels_2 as p 
join x on p.site_id = x.site_id AND p.ogr_fid = x.ogr_fid
where case_id = 'case 4' and row_num < = x.mfu_assigned_diff ;

--check to see if all mfus are assigned
SELECT
	ogr_fid
	,site_id
	,MIN(mfu) as mfu 
	,SUM(mfu_effective) AS mfu_assigned
	,MIN(mfu) - SUM(mfu_effective) AS mfu_diff 
FROM #mfu_parcels_2 
GROUP BY ogr_fid, site_id 
ORDER BY ogr_fid, site_id
;

select distinct site_id from #mfu_parcels_2

--**********************************************************************************SFU and MFU projects****************************************************************************************
--select distinct gplu 
--from spacecore.urbansim.scheduled_development_parcels as p 
--join spacecore.urbansim.general_plan_parcels as gp on p.parcel_id = gp.parcel_id
--where sfu > 0 and mfu = 0 
----where sfu > 0 and mfu > 0 
----where sfu = 0 and mfu > 0 
--order by gplu

--select distinct site_id 
--from spacecore.urbansim.scheduled_development_parcels as p 
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
from spacecore.urbansim.scheduled_development_parcels AS sdp
JOIN spacecore.gis.scheduled_development_sites AS sds
	ON sdp.ogr_fid = sds.ogr_fid
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
from spacecore.urbansim.scheduled_development_parcels as s
JOIN spacecore.gis.scheduled_development_sites AS sds
	ON s.ogr_fid = sds.ogr_fid
join spacecore.urbansim.general_plan_parcels as gp on s.parcel_id = gp.parcel_id 
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
,min(sds.sfu) as sfu 
,min(sds.mfu) as mfu
,min(startdate) AS startdate
,min(compdate) AS  compdate
,min(civemp) AS civemp
,min(milemp) AS milemp
,min(pc.parcel_count) as parcel_count 
,CAST(NULL as numeric(36,18)) as mfu_per_dev_acre
,CAST(NULL as numeric(36,18)) as sfu_per_dev_acre
,CAST(NULL as int) as mfu_effective
,CAST(NULL as int) as sfu_effective
,CAST(NULL as int) as row_num
INTO #sf_mf_sites
from spacecore.urbansim.scheduled_development_parcels as s 
JOIN spacecore.gis.scheduled_development_sites AS sds
	ON s.ogr_fid = sds.ogr_fid
join #parcel_count as pc on s.site_id = pc.site_id 
where sds.sfu >0 and sds.mfu >0
group by s.site_id
order by s.site_id ;

--select * from #sf_mf_sites 

--change developable acreage for those parcels which do not have any residential land uses according to GPLU
update #sf_mf_parcels
set developable_acres = 0
from #sf_mf_parcels
where gplu not in(1100,1200,9600,9700)
;

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
,sfu
,#sf_mf_sites.parcel_count
,mfu
--,mhu
,gplu
,parcel_acres
,proportion_developable
,proportion_undevelopable
,developable_acres
,(sfu - parcel_count) as sfu_difference
,(mfu - parcel_count) as mfu_difference
,#sf_mf_parcels_0.row_num 
,CASE WHEN (sfu - parcel_count) = 0   THEN 1                                --Case 1: when the number of parcels in a site_id = the number of units in the site_id 
WHEN parcel_count = 1 and #sf_mf_parcels_0.row_num = 1 THEN sfu             --Case 2: when there is only one parcel for the site_id, assign all units to the one parcel 
END as sfu_effective 
,CASE WHEN (mfu - parcel_count) = 0   THEN 1                                --Case 1: when the number of parcels in a site_id = the number of units in the site_id 
WHEN parcel_count = 1 
--and #sf_mf_parcels_0.row_num = 1 
THEN mfu             --Case 2: when there is only one parcel for the site_id, assign all units to the one parcel 
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

drop table if exists spacecore.urbansim.sched_dev_all;

select * into spacecore.urbansim.sched_dev_all

from (
select NULL as scenario, site_id, parcel_id, startdate, compdate, civemp, milemp, sfu, mfu, 0 AS mhu, ISNULL(sfu_effective,0) as sfu_effective, ISNULL(mfu_effective,0) as mfu_effective, case_id
from #sf_mf_parcels_2			--CHECK '0 AS mhu' OVERRIDE

union all

select NULL as scenario, site_id, parcel_id, startdate, compdate, 0 AS civemp, 0 AS milemp, sfu, 0 as mfu, mhu, ISNULL(sfu_effective,0) as sfu_effective, 0 as mfu_effective, case_id
from #sfu_parcels_2				--CHECK '0 AS civemp, 0 AS milemp' OVERRIDE

union all

select NULL as scenario, site_id, parcel_id, startdate, compdate, civemp, milemp, 0 as sfu, mfu, mhu, 0 as sfu_effective, ISNULL(mfu_effective,0) as sfu_effective, case_id
from #mfu_parcels_2)			--CHECK '0 as sfu' OVERRIDE
as temp;

select distinct site_id from spacecore.urbansim.sched_dev_all

/*#################### INSERT ADDITIONAL CAPACITY DATA FROM PARCELS ####################*/
ALTER TABLE spacecore.urbansim.sched_dev_all
ADD du_2015 int
	,du_2017 int
	,capacity_1 int
	,capacity_2 int
	,max_res_units int
;

UPDATE sda
SET sda.du_2015 = usp.du_2015
	,sda.du_2017 = usp.du_2017
	,sda.max_res_units = usp.max_res_units
	,sda.capacity_1 = usp.capacity_1
	,sda.capacity_2 = usp.capacity_2
FROM spacecore.urbansim.sched_dev_all AS sda
JOIN spacecore.urbansim.parcels AS usp ON sda.parcel_id = usp.parcel_id
;

/*
/*#################### INSERT TO SCHEDULED DEVELOPMENT PARCELS ####################*/
ALTER TABLE spacecore.urbansim.scheduled_development_parcels
ADD du_2015 int
	,du_2017 int
	,capacity_1 int
	,capacity_2 int
	,max_res_units int
;

UPDATE sdp
SET sda.du_2015 = usp.du_2015
	,sda.du_2017 = usp.du_2017
	,sda.capacity_1 = usp.capacity_1
	,sda.capacity_2 = usp.capacity_2
	,sda.max_res_units = usp.max_res_units
FROM spacecore.urbansim.scheduled_development_parcels AS sdp
JOIN #sched_dev_all AS sda
JOIN spacecore.urbansim.parcels AS usp ON sda.parcel_id = usp.parcel_id
;
*/

--*********************************************************************************************************


/*######################################## CHECK UNIT ALLOCATION ########################################*/

--**TOTAL
SELECT
	SUM([sfu_effective]) AS sfu_effective
	,SUM([mfu_effective]) AS mfu_effective
	,SUM([du_2015]) AS du_2015
	,SUM([du_2017]) AS du_2017
FROM [spacecore].[urbansim].[sched_dev_all]

SELECT
	SUM([sfu]) AS sfu
	,SUM([mfu]) AS mfu
	,SUM([mhu]) AS mhu
FROM [spacecore].[gis].[scheduled_development_sites]
WHERE status <> 'completed'
--AND EXISTS (SELECT siteid FROM [spacecore].[urbansim].[sched_dev_all])


--**SITE
DROP TABLE IF EXISTS #sda
SELECT site_id,
	--,SUM([sfu])
	--,SUM([mfu])
	--,SUM([mhu])
	SUM([sfu_effective]) AS sfu_effective
	,SUM([mfu_effective]) AS mfu_effective
	,SUM([du_2015]) AS du_2015
	,SUM([du_2017]) AS du_2017
INTO #sda
FROM [spacecore].[urbansim].[sched_dev_all]
--WHERE site_id = 1062
GROUP BY site_id
ORDER BY site_id

DROP TABLE IF EXISTS #sds
SELECT siteid,
	SUM([sfu]) AS sfu
	,SUM([mfu]) AS mfu
	,SUM([mhu]) AS mhu
INTO #sds
FROM [spacecore].[gis].[scheduled_development_sites]
--WHERE siteid = 1062
WHERE status <> 'completed'
GROUP BY siteid
ORDER BY siteid


--SFU
SELECT *
FROM #sda AS sda
JOIN #sds AS sds ON sda.site_id = sds.siteid
WHERE sfu_effective <> sfu
ORDER BY site_id

--MFU
SELECT *
FROM #sda AS sda
FULL OUTER JOIN #sds AS sds ON sda.site_id = sds.siteid
WHERE mfu_effective <> mfu
ORDER BY COALESCE(siteid, site_id)


SELECT *
FROM [spacecore].[urbansim].[sched_dev_all]
WHERE parcel_id = 5200983 

SELECT *
FROM [spacecore].[urbansim].[sched_dev_all]
WHERE site_id = 15035
