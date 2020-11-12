select distinct x.r[1] from fc.sql join lateral regexp_matches(t,'\[(.*?)\]','g') x(r)on true ORDER BY x.r[1] ASC;
