DO
$$
DECLARE
    clist text;
    ytdbody text;

BEGIN
-------------------------------build a column list----------------------------------------
SELECT 
    string_agg(format('%I',cname),E'\n    ,' ORDER BY opos ASC)
INTO
    clist
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version');

--RAISE NOTICE 'build list: %',clist;

SELECT 
$a$
SELECT
    $a$::text||    
    clist||
    $b$
    ,'baseline' "version"
    ,'actuals' iter
FROM
    rlarp.osm_dev o
WHERE
    (
        --base period orders booked....
        [order date column name] BETWEEN [supplied target range from date] AND [supplied target range to date]
        --...or any open orders currently booked before cutoff....
        OR ([order status column here] IN ([list of statuses indicating still open]) and [order date column name] <= [include open orders through this date])
        --...or anything that shipped in that period
        OR ([name of shipdate column] BETWEEN [supplied target range from date] AND [supplied target range to date])
    )
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
$b$::text
INTO
    ytdbody;

RAISE NOTICE '%', ytdbody;

END
$$
