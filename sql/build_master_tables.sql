--if the data is already cleansed is it necessary to even have master data tables? -> yes for adding new scenarios
--it is possible some parts not longer exist in the item master becuase they have since been deleted, so it is not possible to cleanse the data
do
$$
DECLARE 
    f record;
    _sql text;
BEGIN
    FOR f IN  
        SELECT
            -------------------------------------------create table---------------------------------------------------------------------------------------------------------
            'DROP TABLE IF EXISTS fc.'||func||'; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
            string_agg(format('%I',cname) || ' ' || dtype,', ' ORDER BY opos ASC) || 
            ', PRIMARY KEY ('||string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            -------------------------------------------populate table-------------------------------------------------------------------------------------------------------
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(format('%I',cname),', ' ORDER BY opos ASC) || ' FROM rlarp.osm_dev WHERE '||
            string_agg(format('%I',cname)||' IS NOT NULL ',' AND ') FILTER (WHERE fkey = func)||' ON CONFLICT DO NOTHING' AS pop
        FROM
            fc.target_meta
        GROUP BY
            func
        HAVING
            string_agg(cname,', ') FILTER (WHERE fkey = func) <> ''
    loop 
        EXECUTE format('%s',f.ddl);
        EXECUTE format('%s',f.pop);
    END LOOP;
END;
$$
