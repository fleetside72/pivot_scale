DO
$func$
DECLARE
    _clist text;
    _clist_vol text;
    _clist_prc text;
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
    _units_col text;
    _value_col text;


BEGIN
-----------------populate application variables--------------------------------------------
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_date') INTO _order_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'ship_date') INTO _ship_date;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'order_status') INTO _order_status;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'units') INTO _units_col;
SELECT (SELECT cname FROM fc.target_meta WHERE appcol = 'value') INTO _value_col;
-------------------------all columns ------------------------------------------------------
SELECT 
    string_agg('o.'||format('%I',cname),E'\n    ,' ORDER BY opos ASC)
INTO
    _clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');
-------------------------all columns except volume scale-----------------------------------
SELECT 
    string_agg(
        --create the column reference
        'o.'||format('%I',cname)||
        CASE WHEN appcol IN ('units', 'value', 'cost') THEN ' * vscale.factor AS '||format('%I',cname) ELSE '' END,
        --delimiter
        E'\n    ,' 
        --sort column ordinal
        ORDER BY opos ASC
    )
INTO
    _clist_vol
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

 -------------------------all columns except volume scale-----------------------------------
SELECT 
    string_agg(
        --create the column reference
        CASE 
            WHEN appcol IN ('units', 'cost') THEN '0::numeric'
            WHEN appcol IN ('value') THEN $$(CASE WHEN pscale.factor = 0 THEN o.$$||_units_col||$$ * pscale.mod_price ELSE o.$$||format('%I',cname)||' * pscale.factor END)::numeric AS '||_value_col
            ELSE 'o.'||format('%I',cname)
        END,
        --delimiter
        E'\n    ,' 
        --sort column ordinal
        ORDER BY opos ASC
    )
INTO
    _clist_prc
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

SELECT
---------$$app_req$$ will hold the request body--------------------
$$WITH
req AS  (SELECT $$||'$$app_req$$::jsonb j)'||$$
,target AS (
    SELECT
        (req.j->>'vincr')::numeric vincr   --volume
        ,(req.j->>'pincr')::numeric pincr  --price
    FROM
        req
)
-----this block is supposed to test for new products that might not be in baseline etc-------
,test AS (
    SELECT
        sum($$||_units_col||$$) FILTER (WHERE version <> 'ACTUALS') total
        ,sum($$||_units_col||$$) FILTER (WHERE iter = 'baseline') base
    FROM
        fc.live
    WHERE
        app_where
)
,basemix AS (
SELECT
    $$||_clist||$$
FROM
    fc.live o 
WHERE
    app_where
),
vscale AS (
    SELECT
        (SELECT vincr FROM target) AS target_increment
        ,sum($$||_units_col||') AS units'||$$
        ,(SELECT vincr FROM target)/sum($$||_units_col||$$) AS factor
    FROM
        basemix
)
,volume AS (
SELECT
    $$||_clist_vol||$$
FROM
    basemix o
    CROSS JOIN vscale
)
,pscale AS (
SELECT
    (SELECT pincr FROM target) AS target_increment
    ,sum($$||_value_col||') AS value'||$$
    ,CASE WHEN (SELECT sum($$||_value_col||$$) FROM volume) = 0 THEN
        --if the base value is -0- scaling will not work, need to generate price, factor goes to -0-
        0
    ELSE
        --if the target dollar value still does not match the target increment, make this adjustment
        ((SELECT pincr FROM target)-(SELECT sum($$||_value_col||$$) FROM volume))/(SELECT sum($$||_value_col||$$) FROM volume)
    END factor
    ,CASE WHEN (SELECT sum($$||_value_col||$$) FROM volume) = 0 THEN
        CASE WHEN ((SELECT pincr::numeric FROM target) - (SELECT sum($$||_value_col||$$) FROM volume)) <> 0 THEN
            --if the base value is -0- but the target value hasn't been achieved, derive a price to apply
            ((SELECT pincr::numeric FROM target) - (SELECT sum($$||_value_col||$$) FROM volume))/(SELECT sum($$||_units_col||$$) FROM volume)
        ELSE
            0
        END
    ELSE
        0
    END mod_price
FROM
    volume
)
,pricing AS (
SELECT
    $$||_clist_prc||$$
FROM
    volume o
    CROSS JOIN pscale
WHERE
    pscale.factor <> 0 or pscale.mod_price <> 0
)
INSERT INTO
    fc.live
SELECT * FROM volume UNION ALL SELECT * FROM pricing
$$
INTO
    _sql;

RAISE NOTICE '%', _sql;
    
INSERT INTO fc.sql SELECT 'scale', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
