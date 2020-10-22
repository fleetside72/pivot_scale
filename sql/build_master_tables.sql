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
            'DROP TABLE IF EXISTS fc.'||func||'; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
            string_agg(cname || ' ' || dtype,', ' ORDER BY opos ASC) || 
            ', PRIMARY KEY ('||string_agg(cname,', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(cname,', ' ORDER BY opos ASC) || ' FROM rlarp.osm_dev WHERE '||string_agg(cname,'||') FILTER (WHERE fkey = func)||' IS NOT NULL ON CONFLICT DO NOTHING' AS pop
        FROM
            fc.target_meta
        WHERE
            func <> 'doc'
        GROUP BY
            func
    loop 
        EXECUTE format('%s',f.ddl);
        EXECUTE format('%s',f.pop);
    END LOOP;
END;
$$
