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


BEGIN

SELECT
$$WITH
req AS  (SELECT $$||'$$app_req$$::jsonb)'||$$
test AS (
    SELECT
        sum(app_units) FILTER WHERE (version <> 'ACTUALS') total
        ,sum(app_units) FILTER (WHERE iter = 'baseline') base
    FROM
        fc.live
    WHERE
        app_where
)


SELECT 'HI' into    _sql;

INSERT INTO fc.sql SELECT 'scale', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END
$func$;

---SELECT * FROM fc.sql;
