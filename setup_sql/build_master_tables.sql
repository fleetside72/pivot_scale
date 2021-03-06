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
            'DROP TABLE IF EXISTS fc.'||func||' CASCADE; CREATE TABLE IF NOT EXISTS fc.'||func||' (' || 
                string_agg(format('%I',cname) || ' ' || dtype,', ' ORDER BY CASE WHEN fkey IS NOT NULL THEN 0 ELSE opos END ASC) || 
            ', PRIMARY KEY ('||string_agg(format('%I',cname),', ') FILTER (WHERE fkey = func)||'));' AS ddl,
            -------------------------------------------populate table-------------------------------------------------------------------------------------------------------
            ---need to add a clause to exclude where the key is null
            'INSERT INTO fc.'||func||' SELECT DISTINCT ' || string_agg(format('%I',cname),', ' ORDER BY CASE WHEN fkey IS NOT NULL THEN 0 ELSE opos END ASC) || ' FROM '||tname||' WHERE '||
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
    loop 
        EXECUTE format('%s',f.ddl);
        EXECUTE format('%s',f.pop);
        EXECUTE format('%s',f.fk);
    END LOOP;
END;
$$
