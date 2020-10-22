--assumes schema fc already exists

DROP TABLE IF EXISTS fc.target_meta;
CREATE TABLE fc.target_meta (
    tname   text
    ,cname  text
    ,opos   int
    ,func   text
    ,fkey   text
    ,pretty text
    ,dtype  text
);

--ALTER TABLE fc.target_meta DROP CONSTRAINT IF EXISTS target_meta_pk;
ALTER TABLE fc.target_meta ADD CONSTRAINT target_meta_pk PRIMARY KEY (tname, cname);

COMMENT ON TABLE fc.target_meta IS 'target table layout info';
