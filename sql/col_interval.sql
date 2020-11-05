DO
$$
DECLARE
    clist text;

BEGIN
-------------------------------build a column list----------------------------------------
SELECT 
    string_agg(
        format('%I',cname) || CASE WHEN func IN ('odate','sdate') THEN ' + interval ''1 year''' ELSE '' END,E'\n,' ORDER BY opos ASC)
INTO
    clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

RAISE NOTICE 'build list: %',clist;

CREATE TEMP TABLE sql(t text);

INSERT INTO sql SELECT clist;

END
$$;

select * from sql;
