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

BEGIN

CREATE TABLE IF NOT EXISTS fc.sql(cmd text PRIMARY KEY, t text );

-------------------------------build a column list----------------------------------------
SELECT 
    string_agg(format('%I',cname),E'\n    ,' ORDER BY opos ASC)
INTO
    _clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

---------------------------build column to increment dates--------------------------------

SELECT 
    string_agg(
        format('%I',cname) || CASE WHEN func IN ('odate','sdate') AND dtype = 'date' THEN ' + interval ''1 year''' ELSE '' END,E'\n    ,' ORDER BY opos ASC)
INTO
    _clist_inc
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

--RAISE NOTICE 'build list: %',clist;

SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_date') INTO _order_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'ship_date') INTO _ship_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_status') INTO _order_status;

--------------------------------------clone the actual baseline-----------------------------------------------

SELECT 
$a$SELECT
    $a$::text||    
    _clist||
    $b$
    ,'forecast_name' "version"
    ,'actuals' iter
FROM
    rlarp.osm_dev o
WHERE
    (
        --base period orders booked....
        $b$||_order_date||$c$ BETWEEN [app_baseline_from_date] AND [app_baseline_to_date]
        --...or any open orders currently booked before cutoff....
        OR ($c$||_order_status||$d$ IN ([app_openstatus_code]) and $d$||_order_date||$e$ <= [app_openorder_cutoff])
        --...or anything that shipped in that period
        OR ($e$||_ship_date||$f$ BETWEEN [app_baseline_from_date] AND [app_baseline_to_date])
    )
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
$f$::text
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
    rlarp.osm_dev o
WHERE
    $$||_order_date||$$ BETWEEN [app_plug_fromdate] AND [app_plug_todate]
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
$$ 
INTO
    _actpy;

------------------------------copy a full year and increment by 1 year for the baseline-------------------------

SELECT
$a$
INSERT INTO 
    fc.live
SELECT
    $a$||_clist_inc||
    $b$
    'forecast_name' "version",
    'baseline' iter
FROM
    baseline
WHERE
    $b$||_order_date||$c$ + interval '1 year' >= $c$||'[app_first_order_date_year]'
    --the final forecast baseline should have orders greater than or equal to the
    --start of the year since new orders is the intended forecast
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
$$
||_baseline
INTO
    _sql;

INSERT INTO fc.sql SELECT 'baseline', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
