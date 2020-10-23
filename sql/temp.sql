        SELECT
            -------------------------------------------create table---------------------------------------------------------------------------------------------------------
            'DROP TABLE IF EXISTS fc.'||func||' CASCADE; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
            string_agg(format('%I',cname) || ' ' || dtype,', ' ORDER BY opos ASC) || 
            ', PRIMARY KEY ('||string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            -------------------------------------------populate table-------------------------------------------------------------------------------------------------------
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(format('%I',cname),', ' ORDER BY opos ASC) || ' FROM '||tname||' WHERE '||
            string_agg(format('%I',cname)||' IS NOT NULL ',' AND ') FILTER (WHERE fkey = func)||' ON CONFLICT DO NOTHING' AS pop,
            -------------------------------------------setup foreign keys---------------------------------------------------------------------------------------------------
            'ALTER TABLE fc.live ADD CONSTRAINT fk_'||func||' FOREIGN KEY ('||string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||') REFERENCES fc.'||func||' ('||
            string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||')' AS fk
        FROM
            fc.target_meta
        GROUP BY
            tname
            ,func
        HAVING
            string_agg(cname,', ') FILTER (WHERE fkey = func) <> ''
