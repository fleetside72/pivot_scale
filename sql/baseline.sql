--a baseline period will have to be identified
WITH
baseline AS (
    -----------------------copy YTD sales---------------------------------------------------------------------
    SELECT
        ,'baseline' "version"
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
            [target_odate] BETWEEN [target_odate_from] AND [target_odate_to]
            --...or any open orders currently booked before cutoff....
            OR ([status_flag] IN ([status_list]) and [target_date] <= [target_date_from])
            --...or anything that shipped in that period
            OR ([target_sdate] BETWEEN [target_sdate_from] AND [target_sdate_to])
        )
        --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
    UNION ALL
    ---------option 1: fill in the rest of the year, with the prior years sales-sales----------------------------
    SELECT
        ,'baseline' "version"
        ,'plug' iter
    FROM
        rlarp.osm_dev o
        LEFT OUTER JOIN gld ON
            gld.fspr = o.fspr
        LEFT OUTER JOIN gld ss ON
            greatest(least(o.sdate,gld.edat),gld.sdat) + interval '1 year' BETWEEN ss.sdat AND ss.edat
    WHERE
        [target_odate] BETWEEN [target_odate_plug_from] AND [target_odate_plug_to]
        --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
    UNION ALL
    --------option 2: fill in the remainder of the current year with current forecase-----------------------------
    SELECT
        ,'baseline' "version"
        ,'plug' iter
    FROM
        rlarp.osmp_dev o
        LEFT OUTER JOIN gld ON
            gld.fspr = o.fspr
        LEFT OUTER JOIN gld ss ON
            greatest(least(o.sdate,gld.edat),gld.sdat)  BETWEEN ss.sdat AND ss.edat
    WHERE
        [target_odate] BETWEEN [target_odate_plug_from] AND [target_odate_plug_to]
        AND false
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
    ,'baseline' "version"
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
