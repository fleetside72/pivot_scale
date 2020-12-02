DO
$func$
DECLARE
    _clist text;
    _clist_vol text;
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
-------------------------all columns except scale-------------------------------------------
SELECT 
    string_agg(
        --create the column reference
        'o.'||format('%I',cname)||
        CASE WHEN appcol IN ('units', 'value', 'cost') THEN ' * vscale.factor' ELSE '' END,
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

SELECT
---------$$app_req$$ will hold the request body--------------------
$$WITH
req AS  (SELECT $$||'$$app_req$$::jsonb)'||$$
-----this block is supposed to test for new products that might not be in baseline etc-------
test AS (
    SELECT
        sum(app_units) FILTER WHERE (version <> 'ACTUALS') total
        ,sum(app_units) FILTER (WHERE iter = 'baseline') base
    FROM
        fc.live
    WHERE
        app_where
)
,basemix AS (
SELECT
    $$||_clist||$$
WHERE
    app_scenario
),
vscale AS (
    SELECT
        app_vincr AS target_increment
        ,sum($$||_units_col||') AS units'||$$
        ,app_vincr/sum($$||_units_col||$$) factor
)
,volume AS (
SELECT
    $$||_clist_vol||$$
FROM
    baseline
    CROSS JOIN vscale
)$$
INTO
    _sql;

RAISE NOTICE '%', _sql;
    
INSERT INTO fc.sql SELECT 'scale', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
