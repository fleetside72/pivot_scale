SELECT 
    string_agg(format('%I',cname),E'\n,' ORDER BY opos ASC) cols 
FROM 
    fc.target_meta 
WHERE 
    func NOT IN ('version','iter');
