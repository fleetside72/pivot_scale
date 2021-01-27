DO
$func$
DECLARE 
    _clist text;
    _clone_meta text;
    _targ text;
    _sql text;

BEGIN
-----------------------------this target would be replaced with a parameter--------------
SELECT
    'rlarp.osm_dev'
INTO
    _targ;

-------------------------------build a column list-----------------------------------------
-----------a list of required columns is in fc.appcols, if they are not present------------
-----------they will have to build included------------------------------------------------
WITH
--this is full new meta rows being created, it will be used to load meta as well as 
--to create the sql that will create the clone - shoudl probably get execute here as well
col_list AS (
    SELECT
        'fc.live' tname
        --,m.cname
        ,CASE WHEN m.cname IS NULL
            THEN '_'||a.col
            ELSE m.cname
        END cname
        --if the colum name is empty that means we are dealig with a required appcol
        --that isn't present: use the appcol.col for the name preceded by underscore
        ,CASE WHEN m.cname IS NULL
            THEN COALESCE(a.dflt,'null::'||a.dtype)||' AS _'||a.col
            ELSE 'o.'||format('%I',COALESCE(cname,''))
        END cname_list
        --,m.opos
        ,row_number() OVER (ORDER BY COALESCE(to_char(opos,'FM000'),a.col)) opos
        ,m.func
        ,m.fkey
        ,m.pretty
        --,m.dtype
        ,COALESCE(m.dtype,a.dtype) dtype
        ,m.mastcol
        ,COALESCE(m.appcol,a.col) appcol
        ,m.dateref
    FROM 
        (SELECT * FROM fc.target_meta WHERE tname = _targ) m
        FULL OUTER JOIN fc.appcols a ON
            m.appcol = a.col
            AND m.dtype = a.dtype
)
--load the new columns for the clone
,load_meta AS (
    INSERT INTO
        fc.target_meta
    SELECT
        --hard-coded name of new clone table
         tname
        ,cname
        ,opos
        ,func
        ,fkey
        ,pretty
        ,dtype
        ,mastcol
        ,appcol
        ,dateref
    FROM
        col_list
    ON CONFLICT ON CONSTRAINT target_meta_pk DO UPDATE SET
        func = EXCLUDED.func
        ,pretty = EXCLUDED.pretty
        ,mastcol = EXCLUDED.mastcol
        ,appcol = EXCLUDED.appcol
        ,dateref = EXCLUDED.dateref
        ,fkey = EXCLUDED.fkey
)
--create sql to create the clone table
SELECT 
    string_agg(
        cname_list,E'\n    ,' ORDER BY opos ASC
    )
INTO
    _clist
FROM 
    col_list m;

--instead of dumping this to sql first should probably just execute it directly
--the only point of converting this to sql is debug_
_sql:= $$CREATE TABLE IF NOT EXISTS fc.live AS (
SELECT
$$||_clist||$$
FROM
    $$||_targ||' o'||$$
) WITH DATA;$$;

--RAISE NOTICE '%', _sql;

INSERT INTO fc.sql SELECT 'live', _sql ON CONFLICT ON CONSTRAINT sql_pkey  DO UPDATE SET t = EXCLUDED.t;

END;
$func$
