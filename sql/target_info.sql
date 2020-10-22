BEGIN;

INSERT INTO
    fc.target_meta
SELECT 
    table_name
    ,column_name
    ,ordinal_position
    ,'doc'::text func
    ,null::text fkey        --foreign key to a master table
    ,null::text pretty
    ,data_type::text dtype
FROM 
    information_schema.columns 
WHERE 
    table_name = 'osm_dev' 
    AND table_schema = 'rlarp'
ON CONFLICT ON CONSTRAINT target_meta_pk DO UPDATE SET
    opos = EXCLUDED.opos
    ,dtype = EXCLUDED.dtype;

END;
