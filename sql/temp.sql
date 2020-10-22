        SELECT
            'DROP TABLE IF EXISTS fc.'||func||'; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
            string_agg(cname || ' ' || dtype,', ' ORDER BY opos ASC) || 
            ', PRIMARY KEY ('||string_agg(cname,', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(cname,', ' ORDER BY opos ASC) || ' FROM rlarp.osm_dev WHERE '||string_agg(cname,'||') FILTER (WHERE fkey = func)||' IS NOT NULL ON CONFLICT DO NOTHING' AS populate
        FROM
            fc.target_meta
        WHERE
            func <> 'doc'
        GROUP BY
            func
