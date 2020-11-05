DO
$$
DECLARE
    clist text;

BEGIN
-------------------------------build a column list----------------------------------------
SELECT 
    string_agg(format('%I',cname),E'\n,' ORDER BY opos ASC)
INTO
    clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

RAISE NOTICE 'build list: %',clist;

END
$$
