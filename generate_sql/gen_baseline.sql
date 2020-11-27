DO
$func$
DECLARE
    _clist text;
    _clist_inc text;
    _ytdbody text;
    _order_date text;
    _ship_date text;
    _order_status text;
    _actpy text;
    _sql text;
    _baseline text;
    _date_funcs jsonb;
    _perd_joins text;
    _interval interval;

/*----------------parameters listing--------------
app_baseline_from_date
app_baseline_to_date
app_first_forecast_date
app_openorder_cutoff
app_openstatus_code
app_plug_fromdate
app_plug_todate
------------------------------------------------*/

BEGIN

-----------------populate application variables--------------------------------------------
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_date') INTO _order_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'ship_date') INTO _ship_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_status') INTO _order_status;
--the target interval
SELECT interval '1 year' INTO _interval;
SELECT jsonb_agg(func) INTO _date_funcs FROM fc.target_meta WHERE dtype = 'date' AND fkey is NOT null;
--create table join for each date based func in target_meta joining to fc.perd static table
--the join, though, should be based on the target date, which is needs an interval added to get to the target
SELECT
    string_agg(
        'LEFT OUTER JOIN fc.perd '||func||' ON'||
        $$
        $$||'(o.'||fkey||' + interval '||format('%L',_interval) ||' )::date <@ '||func||'.drange'
    ,E'\n')
INTO
    _perd_joins
FROM 
    fc.target_meta 
WHERE 
    dtype = 'date' 
    AND fkey IS NOT NULL;

CREATE TABLE IF NOT EXISTS fc.sql(cmd text PRIMARY KEY, t text );

-------------------------------build a column list-----------------------------------------
SELECT 
    string_agg('o.'||format('%I',cname),E'\n    ,' ORDER BY opos ASC)
INTO
    _clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

---------------------------build column to increment dates---------------------------------

SELECT 
    string_agg(
        CASE
            --if you're dealing with a date function...
            WHEN _date_funcs ? func THEN
                CASE 
                    --...but it's not the date itself...
                    WHEN fkey IS NULL THEN 
                        --...pull the associated date field from perd table
                        func||'.'||m.dateref
                    --...and it's the primary key date...
                    ELSE 
                        --use the date key but increment by the target interval
                        --this assumes that the primary key for the func is a date, but it has to be or it wont join anyways
                        'o.'||fkey||' + interval '||format('%L',_interval) ||' AS '||fkey
                END
            ELSE
                'o.'||format('%I',cname)
        END
        ,E'\n    ,' ORDER BY opos ASC
    )
INTO
    _clist_inc
FROM 
    fc.target_meta m
WHERE 
    func NOT IN ('version');

--RAISE NOTICE 'build list: %',clist;

--------------------------------------clone the actual baseline-----------------------------------------------

SELECT 
$$SELECT
    $$::text||    
    _clist||
    $$
    ,'forecast_name' "version"
    ,'actuals' iter
FROM
    fc.live o
WHERE
    (
        --base period orders booked....
        $$||_order_date||$$ BETWEEN 'app_baseline_from_date'::date AND 'app_baseline_to_date'::date
        --...or any open orders currently booked before cutoff....
        OR ($$||_order_status||$$ IN (app_openstatus_code) and $$||_order_date||$$ <= 'app_openorder_cutoff'::date)
        --...or anything that shipped in that period
        OR ($$||_ship_date||$$ BETWEEN 'app_baseline_from_date'::date AND 'app_baseline_to_date'::date)
    )
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
$$::text
INTO
    _ytdbody;

--RAISE NOTICE '%', _ytdbody;


------------------------------------pull a plug from actuals to create a full year baseline------------------

SELECT
$$SELECT
    $$||_clist_inc||
$$
    ,'forecast_name' "version"
    ,'plug' iter
FROM
    fc.live o$$||E'\n'||_perd_joins||$$
WHERE
    $$||_order_date||$$ BETWEEN 'app_plug_fromdate'::date AND 'app_plug_todate'::date
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
$$ 
INTO
    _actpy;

------------------------------copy a full year and increment by 1 year for the baseline-------------------------

SELECT
--$$INSERT INTO 
--    fc.live
$$,incr AS (
SELECT
    $$||_clist_inc||
    $$
    ,'forecast_name' "version"
    ,'baseline' iter
FROM
    baseline o$$||E'\n'||_perd_joins||$$
)
,ins AS (
INSERT INTO
    fc.live
SELECT
    *
FROM
    incr i
WHERE
    i.$$||_order_date||$$ >= 'app_first_forecast_date'::date$$||$$
    OR i.$$||_ship_date||$$ >= 'app_first_forecast_date'::date$$||$$
RETURNING *
)
SELECT COUNT(*) num_rows  FROM ins$$
    --any orders in the forecast period, or any sales in the forecast period (from open orders)
INTO
    _baseline;
    

------------------------------stack the sql into the final format------------------------------------------------

SELECT
$$DELETE FROM fc.live WHERE version = 'forecast_name';
WITH
baseline AS (
$$||_ytdbody||
$$UNION ALL
$$||_actpy
||$$)
$$||_baseline
INTO
    _sql;

INSERT INTO fc.sql SELECT 'baseline', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
