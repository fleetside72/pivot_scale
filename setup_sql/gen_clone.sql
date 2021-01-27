DO
$func$
DECLARE 
    _clist text;
    _targ text;
    _sql text;

BEGIN
-----------------------------this target would be replaced with a parameter--------------
SELECT
    'rlarp.osm_dev o'
INTO
    _targ;

-------------------------------build a column list-----------------------------------------
-----------a list of required columns is in fc.appcols, if they are not present------------
-----------they will have to build included------------------------------------------------
SELECT 
    string_agg(
        --if the colum name is empty that means we are dealig with a required appcol
        --that isn't present: use the appcol.col for the name preceded by underscore
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
    tname = _targ;

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
