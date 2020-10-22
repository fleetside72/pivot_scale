SELECT
    'CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
    string_agg(cname || ' ' || dtype,', ' ORDER BY opos ASC) || 
    ', PRIMARY KEY ('||string_agg(cname,', ') FILTER (WHERE fkey = func)||'))' AS ddl,
    'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(cname,', ' ORDER BY opos ASC) || ' FROM fc.target' AS populate
FROM
    fc.target_meta
WHERE
    func <> 'doc'
GROUP BY
    func;
