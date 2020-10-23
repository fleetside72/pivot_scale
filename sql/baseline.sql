WITH
baseline AS (
    -----------------------copy YTD sales---------------------------------------------------------------------
    SELECT
        ,'15mo' "version"
        ,'actuals' iter
    FROM
        rlarp.osm_dev o
        --snap the ship dates of the historic fiscal period
        LEFT OUTER JOIN gld ON
            gld.fspr = o.fspr
        --get the shipping season for open orders based on the snapped date
        LEFT OUTER JOIN gld ss ON
            greatest(least(o.sdate,gld.edat),gld.sdat) BETWEEN ss.sdat AND ss.edat
    WHERE
        (
            --base period orders booked....
            o.odate BETWEEN '2019-06-01' AND '2020-02-29'
            --...or any open orders currently booked before cutoff....
            OR (o.calc_status IN ('OPEN','BACKORDER') and o.odate < '2020-03-01')
            --...or anything that shipped in that period
            OR o.fspr BETWEEN '2001' AND '2009'
        )
        AND fs_line = '41010'
        AND calc_status <> 'CANCELED'
        AND NOT (calc_status = 'CLOSED' AND flag = 'REMAINDER')
    UNION ALL
    ---------option 1: fill in the rest of the year, with the prior years sales-sales----------------------------
    SELECT
        ,'actuals' "version"
        ,'actuals_plug' iter
    FROM
        rlarp.osm_dev o
        LEFT OUTER JOIN gld ON
            gld.fspr = o.fspr
        LEFT OUTER JOIN gld ss ON
            greatest(least(o.sdate,gld.edat),gld.sdat) + interval '1 year' BETWEEN ss.sdat AND ss.edat
    WHERE
        o.odate BETWEEN '2019-03-01' AND '2019-05-31'
        AND fs_line = '41010'
        AND calc_status <> 'CANCELED'
        ------exclude actuals for now and use forecast to get the plug for the rest of the year
        AND false
    UNION ALL
    --------option 2: fill in the remainder of the current year with current forecase-----------------------------
    SELECT
        ,'actuals' "version"
        ,'forecast_plug' iter
    FROM
        rlarp.osmp_dev o
        LEFT OUTER JOIN gld ON
            gld.fspr = o.fspr
        LEFT OUTER JOIN gld ss ON
            greatest(least(o.sdate,gld.edat),gld.sdat)  BETWEEN ss.sdat AND ss.edat
    WHERE
        o.odate BETWEEN '2020-03-01' AND '2020-05-31'
        AND fs_line = '41010'
        AND calc_status <> 'CANCELED'
)
-------------------copy the baseline just generated and increment all the dates by one year------------------------------------
,incr AS (
SELECT 
    ,o.dcodat + interval '1 year'   --incremented
    ,o.ddqdat + interval '1 year'   --incremented
    ,o.dhidat + interval '1 year'   --incremented
    ,o.odate + interval '1 year'    --incremented
    ,o.oseas + 1                    --incremented
    ,o.rdate + interval '1 year'    --incremented
    ,o.rseas + 1                    --incremented
    ,o.sdate + interval '1 year'    --incremented
    ,o.sseas + 1                    --incremented
    ,'b21' "version"
    ,'copy' iter
FROM 
    baseline o
    LEFT OUTER JOIN gld ON
        o.sdate + interval '1 year' BETWEEN gld.sdat and gld.edat
WHERE
    o.odate + interval '1 year' >= '2020-06-01'
)
-------------insert the baseline actuals + x months of forecast in addtion to 12 months of forecase----------------------------
INSERT INTO rlarp.osmf_dev
SELECT * FROM incr
UNION ALL
SELECT * FROM baseline;

---identify short ships: causes disconnect with actual sales-------------------------------------------------------------------
--UPDATE rlarp.osmfs SET iter = 'short ship' WHERE calc_status = 'CLOSED' AND flag = 'REMAINDER';

---identify goofy ship dates: causes disconnect with sales when splicing in a forecast that has this problem-------------------
--UPDATE rlarp.osmfs SET iter = 'bad date' WHERE adj_shipdate < adj_orderdate;
