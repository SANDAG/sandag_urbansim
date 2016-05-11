use [spacecore];
GO

IF OBJECT_ID('input.assessor_par_transactions', 'U') IS NOT NULL
BEGIN
	DROP TABLE input.assessor_par_transactions;
END
CREATE TABLE input.assessor_par_transactions (parcel_id int NOT NULL
                                        ,apn bigint NOT NULL
										,year_built smallint
										,beds smallint
										,baths decimal(3,1)
										,has_view bit
										,sqft int
										,oc_doc_date char(6) NOT NULL
                                        ,tx_date date
										,tx_price int NOT NULL);

DECLARE @chg_summary TABLE (change varchar(20));

MERGE INTO input.assessor_par_transactions
USING (
    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE1
        ,assessor_par.OC_PRICE1
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE1 > 0
    AND OC_DOC_DATE1 <> '000000'
    
    UNION ALL

    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE2
        ,assessor_par.OC_PRICE2
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE2 > 0
    AND OC_DOC_DATE2 <> '000000'

    UNION ALL

    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE3
        ,assessor_par.OC_PRICE3
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE3 > 0
    AND OC_DOC_DATE3 <> '000000'

    UNION ALL

    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE4
        ,assessor_par.OC_PRICE4
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE4 > 0
    AND OC_DOC_DATE4 <> '000000'

    UNION ALL

    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE5
        ,assessor_par.OC_PRICE5
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE5 > 0
    AND OC_DOC_DATE5 <> '000000'

    UNION ALL

    SELECT parcels.parcelid as parcel_id
        ,assessor_par.apn
        ,OC_DOC_DATE6
        ,assessor_par.OC_PRICE6
    FROM input.assessor_par
        INNER JOIN gis.parcels
        ON parcels.apn = assessor_par.APN
    WHERE OC_PRICE6 > 0
    AND OC_DOC_DATE6 <> '000000') AS source (parcel_id, apn, oc_doc_date, tx_price)
ON assessor_par_transactions.apn = source.apn
AND assessor_par_transactions.oc_doc_date = source.oc_doc_date
WHEN NOT MATCHED BY TARGET THEN
INSERT (parcel_id, apn, oc_doc_date, tx_price) VALUES (parcel_id, apn, oc_doc_date, tx_price)
OUTPUT $action INTO @chg_summary;

SELECT change, COUNT(*) AS CountPerChange
FROM @chg_summary
GROUP BY change;

--Set the APN's year_built, beds, baths, has_view, sqft
UPDATE input.assessor_par_transactions
SET year_built = p.year_built
    ,beds = p.beds
    ,baths = p.baths
    ,has_view = p.has_view
    ,sqft = p.sqft
FROM (
    SELECT apn
        ,CASE
                WHEN YEAR_EFFECTIVE BETWEEN '00' AND '16' THEN 2000 + CAST(YEAR_EFFECTIVE as smallint)
                ELSE 1900 + CAST(YEAR_EFFECTIVE as smallint)
            END as year_built
            ,CAST(BEDROOMS AS smallint) as beds
            ,CAST(BATHS as smallint) / 10.0 as baths
            ,CASE
                WHEN PAR_VIEW = 'Y' THEN 1
                ELSE 0
            END as has_view
            ,TOTAL_LVG_AREA + ADDITION_AREA as sqft
        FROM input.assessor_par) p
WHERE p.APN = assessor_par_transactions.apn;


--Fix malformed dates and convert column to date type.
UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + REPLACE(RIGHT(oc_doc_date, 4), '0230', '0229')
WHERE RIGHT(oc_doc_date, 4) = '0230';

UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + REPLACE(RIGHT(oc_doc_date, 4), '0431', '0430')
WHERE RIGHT(oc_doc_date, 4) = '0431';

UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + REPLACE(RIGHT(oc_doc_date, 4), '0631', '0630')
WHERE RIGHT(oc_doc_date, 4) = '0631';

UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + REPLACE(RIGHT(oc_doc_date, 4), '0931', '0930')
WHERE RIGHT(oc_doc_date, 4) = '0931';

UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + REPLACE(RIGHT(oc_doc_date, 4), '1131', '1130')
WHERE RIGHT(oc_doc_date, 4) = '1131';

--Fix malformed leap years.
UPDATE input.assessor_par_transactions
SET oc_doc_date = LEFT(oc_doc_date, 2) + '0228'
WHERE RIGHT(oc_doc_date,4) = '0229'
AND LEFT(oc_doc_date, 2) % 4 <> 0;

--SET tx_date based on fixed oc_doc_date; code 111 = YYMMDD format
UPDATE input.assessor_par_transactions
SET tx_date = CONVERT(DATE, oc_doc_date, 111);