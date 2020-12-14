DO
$func$
DECLARE 
    _clist text;
    _targ text;
    _sql text;

BEGIN

SELECT
    MAX(tname)||' o'
INTO
    _targ
FROM
    fc.target_meta;
-------------------------------build a column list-----------------------------------------
SELECT 
    string_agg('o.'||format('%I',cname),E'\n    ,' ORDER BY opos ASC)
INTO
    _clist
FROM 
    fc.target_meta;

_sql:= $$CREATE TABLE IF NOT EXISTS fc.live AS (
SELECT
$$||_clist||$$
FROM
    $$||_targ;

RAISE NOTICE '%', _sql;
END;
$func$
