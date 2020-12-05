SELECT DISTINCT 
    cmd
    ,x.r[1] 
FROM 
    fc.sql 
    JOIN lateral regexp_matches(t,'app_[\w_]*','g') x(r) ON TRUE 
WHERE
    cmd = 'scale'
ORDER BY 
    cmd
    ,x.r[1] ASC;
