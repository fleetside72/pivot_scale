WITH 
target AS (select target_vol vincr, target_prc pincr)
,testv AS (
    SELECT
        sum(units) tot
        ,sum(units) FILTER (WHERE iter = 'copy') base
        ,sum(units) FILTER (WHERE module = 'new basket') newpart
    FROM
        rlarp.osm_pool
    WHERE
        -----------------scenario----------------------------
        where_clause
        -----------------additional params-------------------
        AND calc_status||flag <> 'CLOSEDREMAINDER' --exclude short ships when building order adjustments
        AND order_date <= ship_date

)
,flagv AS (
    SELECT
        tot
        ,base
        ,newpart
        ,CASE WHEN tot = 0 THEN
            CASE WHEN base = 0 THEN
                CASE WHEN newpart = 0 THEN
                    'unclean data. tested -> does not exist'
                ELSE
                    'scale new part'
                END
            ELSE
                'scale copy'
            END
        ELSE
            'scale all'
        END flag
    FROM
        testv
)
,basemix AS (
    SELECT
        fspr
        ,plnt          ---master data 
        ,promo          --history date mix
        ,terms
        ,bill_cust_descr          --history cust mix
        ,ship_cust_descr          --history cust mix
        ,dsm
        ,quota_rep_descr          --master data 
        ,director
        ,billto_group          --master data 
        ,shipto_group
        ,chan          --master data 
        ,chansub
        ,chan_retail
        ,part
        ,part_descr
        ,part_group
        ,branding
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        ,segm
        ,substance
        ,fs_line          --master data 
        ,r_currency          --history cust mix
        ,r_rate          --master data 
        ,c_currency          --master data 
        ,c_rate          --master data 
        ,sum(coalesce(units,0)) units          --history value
        ,sum(coalesce(value_loc,0)) value_loc          --history value
        ,sum(coalesce(value_usd,0)) value_usd          --0 
        ,sum(coalesce(cost_loc,0)) cost_loc          --history part mix
        ,sum(coalesce(cost_usd,0)) cost_usd
        ,calc_status          --0 
        ,flag          --0 
        ,order_date          --history date mix
        ,order_month
        ,order_season
        ,request_date          --history date mix
        ,request_month
        ,request_season
        ,ship_date          --history date mix
        ,ship_month
        ,ship_season
    FROM
        rlarp.osm_pool
    WHERE
        -----------------scenario----------------------------
        where_clause
        -----------------additional params-------------------
        AND CASE (SELECT flag FROM flagv) 
                WHEN 'scale all' THEN true
                WHEN 'scale copy' THEN iter = 'copy'
                WHEN 'scale new part' THEN module = 'new basket'
            END
        AND calc_status||flag <> 'CLOSEDREMAINDER' --exclude short ships when building order adjustments
        AND order_date <= ship_date
    GROUP BY
                fspr
        ,plnt          ---master data 
        ,promo          --history date mix
        ,terms
        ,bill_cust_descr          --history cust mix
        ,ship_cust_descr          --history cust mix
        ,dsm
        ,quota_rep_descr          --master data 
        ,director
        ,billto_group          --master data 
        ,shipto_group
        ,chan          --master data 
        ,chansub
        ,chan_retail
        ,part
        ,part_descr
        ,part_group
        ,branding
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        ,segm
        ,substance
        ,fs_line          --master data 
        ,r_currency          --history cust mix
        ,r_rate          --master data 
        ,c_currency          --master data 
        ,c_rate          --master data 
        ,calc_status          --0 
        ,flag          --0 
        ,order_date          --history date mix
        ,order_month
        ,order_season
        ,request_date          --history date mix
        ,request_month
        ,request_season
        ,ship_date          --history date mix
        ,ship_month
        ,ship_season
)
,vscale AS (
    SELECT
        (SELECT vincr::numeric FROM target) incr
        ,(SELECT sum(units)::numeric FROM basemix) base
        ,(SELECT vincr::numeric FROM target)/(SELECT sum(units)::numeric FROM basemix) factor
)
--select * from vscale
,log AS (
    INSERT INTO rlarp.osm_log(doc) SELECT $$replace_iterdef$$::jsonb doc RETURNING *
)
,volume AS (
    SELECT
        fspr
        ,plnt          ---master data 
        ,promo          --history date mix
        ,terms
        ,bill_cust_descr          --history cust mix
        ,ship_cust_descr          --history cust mix
        ,dsm
        ,quota_rep_descr          --master data 
        ,director
        ,billto_group          --master data 
        ,shipto_group
        ,chan          --master data 
        ,chansub
        ,chan_retail
        ,part
        ,part_descr
        ,part_group
        ,branding
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        ,segm
        ,substance
        ,fs_line          --master data 
        ,r_currency          --history cust mix
        ,r_rate          --master data 
        ,c_currency          --master data 
        ,c_rate          --master data 
        ,units*s.factor units
        ,value_loc*s.factor value_loc
        ,value_usd*s.factor value_usd
        ,cost_loc*s.factor cost_loc
        ,cost_usd*s.factor cost_usd
        ,calc_status          --0 
        ,flag          --0 
        ,order_date          --history date mix
        ,order_month
        ,order_season
        ,request_date          --history date mix
        ,request_month
        ,request_season
        ,ship_date          --history date mix
        ,ship_month
        ,ship_season
        ,'replace_version' "version"
        ,'replace_source'||' volume'  iter
        ,log.id
        ,COALESCE(log.doc->>'tag','') "tag"
        ,log.doc->>'message' "comment"
        ,log.doc->>'type' module
FROM
    basemix b
    CROSS JOIN vscale s
    CROSS JOIN log
)
,pscale AS (
    SELECT
        (SELECT pincr::numeric FROM target) incr
        ,(SELECT sum(value_loc * r_rate) FROM volume) base
        ,CASE WHEN (SELECT sum(value_loc * r_rate) FROM volume) = 0 THEN
            --if the base value is -0- scaling will not work, need to generate price, factor goes to -0-
            0
        ELSE
            --if the target $amount is not achieved, adjust further
            ((SELECT pincr::numeric FROM target)-(SELECT sum(value_loc * r_rate) FROM volume))/(SELECT sum(value_loc * r_rate) FROM volume)
        END factor
        ,CASE WHEN (SELECT sum(value_loc * r_rate) FROM volume) = 0 THEN
            CASE WHEN ((SELECT pincr::numeric FROM target) - (SELECT sum(value_loc * r_rate) FROM volume)) <> 0 THEN
                --if the base value is -0- but the target value hasn't been achieved, derive a price to apply
                ((SELECT pincr::numeric FROM target) - (SELECT sum(value_loc * r_rate) FROM volume))/(SELECT sum(units) FROM volume)
            ELSE
                0
            END
        ELSE
            0
        END mod_price
)
--select * from pscale
,pricing AS (
    SELECT
        fspr
        ,plnt          ---master data 
        ,promo          --history date mix
        ,terms
        ,bill_cust_descr          --history cust mix
        ,ship_cust_descr          --history cust mix
        ,dsm
        ,quota_rep_descr          --master data 
        ,director
        ,billto_group          --master data 
        ,shipto_group
        ,chan          --master data 
        ,chansub
        ,chan_retail
        ,part
        ,part_descr
        ,part_group
        ,branding
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        ,segm
        ,substance
        ,fs_line          --master data 
        ,r_currency          --history cust mix
        ,r_rate          --master data 
        ,c_currency          --master data 
        ,c_rate          --master data 
        ,0::numeric units
        ,(CASE WHEN s.factor = 0 THEN b.units * s.mod_price/b.r_rate ELSE b.value_loc*s.factor END)::numeric value_loc
        ,(CASE WHEN s.factor = 0 THEN b.units * s.mod_price ELSE b.value_usd*s.factor END)::numeric value_usd
        ,0::numeric cost_loc
        ,0::numeric cost_usd
        ,calc_status          --0 
        ,flag          --0 
        ,order_date          --history date mix
        ,order_month
        ,order_season
        ,request_date          --history date mix
        ,request_month
        ,request_season
        ,ship_date          --history date mix
        ,ship_month
        ,ship_season
        ,'replace_version' "version"
        ,'replace_source'||' price'  iter
        ,log.id
        ,COALESCE(log.doc->>'tag','') "tag"
        ,log.doc->>'message' "comment"
        ,log.doc->>'type' module
    FROM
        volume b
        CROSS JOIN pscale s
        CROSS JOIN log
    WHERE
        s.factor <> 0 or s.mod_price <> 0
)
--select sum(value_usd), sum(units) from pricing
, ins AS (
    INSERT INTO rlarp.osm_pool (SELECT * FROM pricing UNION ALL SELECT * FROM volume) RETURNING *
)
,insagg AS (
    SELECT
        ---------customer info-----------------
        bill_cust_descr
        ,billto_group
        ,ship_cust_descr
        ,shipto_group
        ,quota_rep_descr
        ,director
        ,segm
        ,substance
        ,chan
        ,chansub
        ---------product info------------------
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        --,brand
        --,part_family
        ,part_group
        ,branding
        --,color
        ,part_descr
        ---------dates-------------------------
        ,order_season
        ,order_month
        ,ship_season
        ,ship_month
        ,request_season
        ,request_month
        ,promo
        ,version
        ,iter
        ,logid
        ,tag
        ,comment
        --------values-------------------------
        ,sum(value_loc) value_loc
        ,sum(value_usd) value_usd
        ,sum(cost_loc) cost_loc
        ,sum(cost_usd) cost_usd
        ,sum(units) units
    FROM
        ins
    GROUP BY
        ---------customer info-----------------
        bill_cust_descr
        ,billto_group
        ,ship_cust_descr
        ,shipto_group
        ,quota_rep_descr
        ,director
        ,segm
        ,substance
        ,chan
        ,chansub
        ---------product info------------------
        ,majg_descr
        ,ming_descr
        ,majs_descr
        ,mins_descr
        --,brand
        --,part_family
        ,part_group
        ,branding
        --,color
        ,part_descr
        ---------dates-------------------------
        ,order_season
        ,order_month
        ,ship_season
        ,ship_month
        ,request_season
        ,request_month
        ,promo
        ,version
        ,iter
        ,logid
        ,tag
        ,comment
)
SELECT json_agg(row_to_json(insagg)) x from insagg
