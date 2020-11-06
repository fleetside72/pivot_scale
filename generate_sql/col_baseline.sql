DO
$func$
DECLARE
    _clist text;
    _ytdbody text;
    _order_date text;
    _ship_date text;
    _order_status text;
    _actpy text;
    _sql text;

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

--------------------------------------clone the actual baseline-----------------------------------------------

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


------------------------------------pull a plug from actuals to create a full year baseline------------------

SELECT
$$
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
$$ 
INTO
    _actpy;

------------------------------stack the sql into the final format------------------------------------------------

SELECT
    _ytdbody
    ||$$UNION ALL
    $$||_actpy
INTO
    _sql;

INSERT INTO fc.sql SELECT 'baseline', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
