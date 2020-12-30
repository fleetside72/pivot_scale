BEGIN;
INSERT INTO
    fc.target_meta
SELECT
    --hard-coded name of new clone table
    'fc.live' tname
    ,cname
    ,opos
    ,func
    ,fkey
    ,pretty
    ,dtype
    ,mastcol
    ,appcol
    ,dateref
FROM
    fc.target_meta
WHERE
    --hard-coded original sales data with mapped fields
    tname = 'rlarp.osm_dev'
ON CONFLICT ON CONSTRAINT target_meta_pk DO UPDATE SET
    func = EXCLUDED.func
    ,pretty = EXCLUDED.pretty
    ,mastcol = EXCLUDED.mastcol
    ,appcol = EXCLUDED.appcol
    ,dateref = EXCLUDED.dateref
    ,fkey = EXCLUDED.fkey;
--SELECT * FROM fc.target_meta WHERE tname = 'fc.live';
--ROLLBACK;
COMMIT;
END;
