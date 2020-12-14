BEGIN;

INSERT INTO
    fc.target_meta
SELECT 
    table_schema||'.'||table_name
    ,column_name
    ,ordinal_position
    ,'doc'::text func       --default function to document
    ,null::text fkey        --foreign key to a master table
    ,null::text pretty
    ,data_type::text dtype
    ,column_name mastcol
FROM 
    information_schema.columns 
WHERE 
    --target current sales table assuming it exists in current database, will need revised in short order
    table_name = 'rlarp' 
    AND table_schema = 'osm_dev'
ON CONFLICT ON CONSTRAINT target_meta_pk DO UPDATE SET
    opos = EXCLUDED.opos
    ,dtype = EXCLUDED.dtype;

END;
