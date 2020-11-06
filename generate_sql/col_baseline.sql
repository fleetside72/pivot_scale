DO
$$
DECLARE
    _clist text;
    _ytdbody text;
    _order_date text;
    _ship_date text;
    _order_status text;

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

--RAISE NOTICE 'build list: %',clist;

SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_date') INTO _order_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'ship_date') INTO _ship_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_status') INTO _order_status;

SELECT 
$a$SELECT
    $a$::text||    
    _clist||
    $b$
    ,'baseline' "version"
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

INSERT INTO fc.sql SELECT 'baseline', _ytdbody ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$$;

SELECT * FROM fc.sql;
