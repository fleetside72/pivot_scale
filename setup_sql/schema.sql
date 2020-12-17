--assumes schema fc already exists

--DROP TABLE IF EXISTS fc.target_meta;
CREATE TABLE IF NOT EXISTS fc.target_meta (
    tname       text
    ,cname      text
    ,opos       int
    ,func       text
    ,fkey       text
    ,pretty     text
    ,dtype      text
    ,mastcol    text
    ,appcol     text
    ,dateref    text
);

ALTER TABLE fc.target_meta DROP CONSTRAINT IF EXISTS target_meta_pk;
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
COMMENT ON COLUMN fc.target_meta.dateref IS 'reference to the relevant hard coded perd table column for dates';


CREATE TABLE IF NOT EXISTS fc.appcols (
    col text,
    dtype text,
    required boolean,
    dflt text
);
ALTER TABLE fc.appcols DROP CONSTRAINT IF EXISTS appcols_pkey CASCADE;
ALTER TABLE fc.appcols ADD CONSTRAINT appcols_pkey PRIMARY KEY (col, dtype);
COMMENT ON TABLE fc.appcols IS 'hard-coded columns names searched for by the application';
INSERT INTO 
    fc.appcols (col, dtype, required, dflt) 
VALUES 
    ('value'        ,'numeric',true,    null),
    ('cost'         ,'numeric',true,   '0'),
    ('units'        ,'numeric',true,   '0'),
    ('order_date'   ,'date'   ,true,    null),
    ('ship_date'    ,'date'   ,false,   null),
    ('order_status' ,'text'   ,true,   'CLOSED'),
    ('version'      ,'text'   ,true,   'ACTUALS'),
    ('iteration'    ,'text'   ,true,   'ACTUALS'),
    ('logid'        ,'integer',true,   null),
    ('tag'          ,'text'   ,true,   null),
    ('comment'      ,'text'   ,true,   null),
    ('customer'     ,'text'   ,false,   null),
    ('item'         ,'text'   ,false,   null)
ON CONFLICT ON CONSTRAINT appcols_pkey DO UPDATE SET
    dtype = EXCLUDED.dtype
    ,required = EXCLUDED.required
    ,dflt = EXCLUDED.dflt;

ALTER TABLE fc.target_meta ADD CONSTRAINT fk_appcol FOREIGN KEY (appcol,dtype) REFERENCES fc.appcols(col, dtype);

CREATE TABLE IF NOT EXISTS fc.log  (
    id int GENERATED ALWAYS AS IDENTITY
    ,doc jsonb
);

COMMENT ON TABLE fc.log IS 'forecast change log';
