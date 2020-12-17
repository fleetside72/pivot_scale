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
    fc.target_meta
WHERE
    tname = 'rlarp.osm_dev';
-------------------------------build a column list-----------------------------------------
SELECT 
    string_agg(
        CASE WHEN m.cname IS NULL
            THEN COALESCE(a.dflt,'null::'||a.dtype)||' AS _'||a.col
            ELSE 'o.'||format('%I',COALESCE(cname,''))
        END
        ,E'\n    ,' ORDER BY opos ASC)
INTO
    _clist
FROM 
    fc.target_meta m
    FULL OUTER JOIN fc.appcols a ON
        m.appcol = a.col
        AND m.dtype = a.dtype
WHERE
    tname = 'rlarp.osm_dev';

_sql:= $$CREATE TABLE IF NOT EXISTS fc.live AS (
SELECT
$$||_clist||$$
FROM
    $$||_targ||$$
) WITH DATA;$$;

--RAISE NOTICE '%', _sql;

INSERT INTO fc.sql SELECT 'live', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END;
$func$
