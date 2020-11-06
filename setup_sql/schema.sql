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
    ,mastcol text
    ,appcol text
);

--ALTER TABLE fc.target_meta DROP CONSTRAINT IF EXISTS target_meta_pk;
ALTER TABLE fc.target_meta ADD CONSTRAINT target_meta_pk PRIMARY KEY (tname, cname);

COMMENT ON TABLE fc.target_meta IS 'target table layout info';
COMMENT ON COLUMN fc.target_meta.tname IS 'schema.table_name of target sales data table';
COMMENT ON COLUMN fc.target_meta.cname IS 'column name';
COMMENT ON COLUMN fc.target_meta.opos IS 'ordinal position of column';
COMMENT ON COLUMN fc.target_meta.func IS 'a functional entity (like customer, part number) that master tables will be build from';
COMMENT ON COLUMN fc.target_meta.fkey IS 'primary key for functional entity';
COMMENT ON COLUMN fc.target_meta.pretty IS 'the presentation name of the column';
COMMENT ON COLUMN fc.target_meta.dtype IS 'data type of the sales table column';
COMMENT ON COLUMN fc.target_meta.mastcol IS 'associated field from the master data table if it is different (oseas would refer to ssyr in fc.perd)';
COMMENT ON COLUMN fc.target_meta.appcol IS 'supply column name to be used for application variables - (specifcy the order date column)';
