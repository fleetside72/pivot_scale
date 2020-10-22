        SELECT
            'DROP TABLE IF EXISTS fc.'||func||'; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
            string_agg(format('%I',cname) || ' ' || dtype,', ' ORDER BY opos ASC) || 
            ', PRIMARY KEY ('||string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(format('%I',cname),', ' ORDER BY opos ASC) || ' FROM rlarp.osm_dev WHERE '||string_agg(format('%I',cname)||' IS NOT NULL ',' AND ') FILTER (WHERE fkey = func)||' ON CONFLICT DO NOTHING' AS pop
        FROM
            fc.target_meta
        GROUP BY
            func
        HAVING
            string_agg(cname,', ') FILTER (WHERE fkey = func) <> ''
