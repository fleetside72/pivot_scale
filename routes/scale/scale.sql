WITH
req AS  (SELECT $$app_req$$::jsonb j)
,target AS (
    SELECT
        (req.j->>'vincr')::numeric vincr   --volume
        ,(req.j->>'pincr')::numeric pincr  --price
    FROM
        req
)
-----this block is supposed to test for new products that might not be in baseline etc-------
,test AS (
    SELECT
        sum(fb_qty) FILTER (WHERE version <> 'ACTUALS') total
        ,sum(fb_qty) FILTER (WHERE iter = 'baseline') base
    FROM
        fc.live
    WHERE
        app_where
)
,basemix AS (
SELECT
    o."ddord#"
    ,o."dditm#"
    ,o."fgbol#"
    ,o."fgent#"
    ,o."diinv#"
    ,o."dilin#"
    ,o.quoten
    ,o.quotel
    ,o.dcodat
    ,o.ddqdat
    ,o.dcmdat
    ,o.fesdat
    ,o.dhidat
    ,o.fesind
    ,o.dhpost
    ,o.fspr
    ,o.ddqtoi
    ,o.ddqtsi
    ,o.fgqshp
    ,o.diqtsh
    ,o.diext
    ,o.ditdis
    ,o.discj
    ,o.dhincr
    ,o.plnt
    ,o.promo
    ,o.return_reas
    ,o.terms
    ,o.custpo
    ,o.remit_to
    ,o.bill_class
    ,o.bill_cust
    ,o.bill_rep
    ,o.bill_terr
    ,o.ship_class
    ,o.ship_cust
    ,o.ship_rep
    ,o.ship_terr
    ,o.dsm
    ,o.account
    ,o.shipgrp
    ,o.geo
    ,o.chan
    ,o.chansub
    ,o.orig_ctry
    ,o.orig_prov
    ,o.orig_post
    ,o.bill_ctry
    ,o.bill_prov
    ,o.bill_post
    ,o.dest_ctry
    ,o.dest_prov
    ,o.dest_post
    ,o.part
    ,o.styc
    ,o.colc
    ,o.colgrp
    ,o.coltier
    ,o.colstat
    ,o.sizc
    ,o.pckg
    ,o.kit
    ,o.brnd
    ,o.majg
    ,o.ming
    ,o.majs
    ,o.mins
    ,o.gldco
    ,o.gldc
    ,o.glec
    ,o.harm
    ,o.clss
    ,o.brand
    ,o.assc
    ,o.ddunit
    ,o.unti
    ,o.lbs
    ,o.plt
    ,o.plcd
    ,o.fs_line
    ,o.r_currency
    ,o.r_rate
    ,o.c_currency
    ,o.c_rate
    ,o.fb_qty
    ,o.fb_val_loc
    ,o.fb_val_loc_dis
    ,o.fb_val_loc_qt
    ,o.fb_val_loc_pl
    ,o.fb_val_loc_tar
    ,o.fb_cst_loc
    ,o.fb_cst_loc_cur
    ,o.fb_cst_loc_fut
    ,o.calc_status
    ,o.flag
    ,o.odate
    ,o.oseas
    ,o.rdate
    ,o.rseas
    ,o.sdate
    ,o.sseas
FROM
    fc.live o 
WHERE
    app_where
),
vscale AS (
    SELECT
        (SELECT vincr FROM target) AS target_increment
        ,sum(fb_qty) AS units
        ,(SELECT vincr FROM target)/sum(fb_qty) AS factor
    FROM
        basemix
)
,volume AS (
SELECT
    o."ddord#"
    ,o."dditm#"
    ,o."fgbol#"
    ,o."fgent#"
    ,o."diinv#"
    ,o."dilin#"
    ,o.quoten
    ,o.quotel
    ,o.dcodat
    ,o.ddqdat
    ,o.dcmdat
    ,o.fesdat
    ,o.dhidat
    ,o.fesind
    ,o.dhpost
    ,o.fspr
    ,o.ddqtoi
    ,o.ddqtsi
    ,o.fgqshp
    ,o.diqtsh
    ,o.diext
    ,o.ditdis
    ,o.discj
    ,o.dhincr
    ,o.plnt
    ,o.promo
    ,o.return_reas
    ,o.terms
    ,o.custpo
    ,o.remit_to
    ,o.bill_class
    ,o.bill_cust
    ,o.bill_rep
    ,o.bill_terr
    ,o.ship_class
    ,o.ship_cust
    ,o.ship_rep
    ,o.ship_terr
    ,o.dsm
    ,o.account
    ,o.shipgrp
    ,o.geo
    ,o.chan
    ,o.chansub
    ,o.orig_ctry
    ,o.orig_prov
    ,o.orig_post
    ,o.bill_ctry
    ,o.bill_prov
    ,o.bill_post
    ,o.dest_ctry
    ,o.dest_prov
    ,o.dest_post
    ,o.part
    ,o.styc
    ,o.colc
    ,o.colgrp
    ,o.coltier
    ,o.colstat
    ,o.sizc
    ,o.pckg
    ,o.kit
    ,o.brnd
    ,o.majg
    ,o.ming
    ,o.majs
    ,o.mins
    ,o.gldco
    ,o.gldc
    ,o.glec
    ,o.harm
    ,o.clss
    ,o.brand
    ,o.assc
    ,o.ddunit
    ,o.unti
    ,o.lbs
    ,o.plt
    ,o.plcd
    ,o.fs_line
    ,o.r_currency
    ,o.r_rate
    ,o.c_currency
    ,o.c_rate
    ,o.fb_qty * vscale.factor AS fb_qty
    ,o.fb_val_loc * vscale.factor AS fb_val_loc
    ,o.fb_val_loc_dis
    ,o.fb_val_loc_qt
    ,o.fb_val_loc_pl
    ,o.fb_val_loc_tar
    ,o.fb_cst_loc * vscale.factor AS fb_cst_loc
    ,o.fb_cst_loc_cur
    ,o.fb_cst_loc_fut
    ,o.calc_status
    ,o.flag
    ,o.odate
    ,o.oseas
    ,o.rdate
    ,o.rseas
    ,o.sdate
    ,o.sseas
FROM
    basemix o
    CROSS JOIN vscale
)
,pscale AS (
SELECT
    (SELECT pincr FROM target) AS target_increment
    ,sum(fb_val_loc) AS value
    ,CASE WHEN (SELECT sum(fb_val_loc) FROM volume) = 0 THEN
        --if the base value is -0- scaling will not work, need to generate price, factor goes to -0-
        0
    ELSE
        --if the target dollar value still does not match the target increment, make this adjustment
        ((SELECT pincr FROM target)-(SELECT sum(fb_val_loc) FROM volume))/(SELECT sum(fb_val_loc) FROM volume)
    END factor
    ,CASE WHEN (SELECT sum(fb_val_loc) FROM volume) = 0 THEN
        CASE WHEN ((SELECT pincr::numeric FROM target) - (SELECT sum(fb_val_loc) FROM volume)) <> 0 THEN
            --if the base value is -0- but the target value hasn't been achieved, derive a price to apply
            ((SELECT pincr::numeric FROM target) - (SELECT sum(fb_val_loc) FROM volume))/(SELECT sum(fb_qty) FROM volume)
        ELSE
            0
        END
    ELSE
        0
    END mod_price
FROM
    volume
)
,pricing AS (
SELECT
    o."ddord#"
    ,o."dditm#"
    ,o."fgbol#"
    ,o."fgent#"
    ,o."diinv#"
    ,o."dilin#"
    ,o.quoten
    ,o.quotel
    ,o.dcodat
    ,o.ddqdat
    ,o.dcmdat
    ,o.fesdat
    ,o.dhidat
    ,o.fesind
    ,o.dhpost
    ,o.fspr
    ,o.ddqtoi
    ,o.ddqtsi
    ,o.fgqshp
    ,o.diqtsh
    ,o.diext
    ,o.ditdis
    ,o.discj
    ,o.dhincr
    ,o.plnt
    ,o.promo
    ,o.return_reas
    ,o.terms
    ,o.custpo
    ,o.remit_to
    ,o.bill_class
    ,o.bill_cust
    ,o.bill_rep
    ,o.bill_terr
    ,o.ship_class
    ,o.ship_cust
    ,o.ship_rep
    ,o.ship_terr
    ,o.dsm
    ,o.account
    ,o.shipgrp
    ,o.geo
    ,o.chan
    ,o.chansub
    ,o.orig_ctry
    ,o.orig_prov
    ,o.orig_post
    ,o.bill_ctry
    ,o.bill_prov
    ,o.bill_post
    ,o.dest_ctry
    ,o.dest_prov
    ,o.dest_post
    ,o.part
    ,o.styc
    ,o.colc
    ,o.colgrp
    ,o.coltier
    ,o.colstat
    ,o.sizc
    ,o.pckg
    ,o.kit
    ,o.brnd
    ,o.majg
    ,o.ming
    ,o.majs
    ,o.mins
    ,o.gldco
    ,o.gldc
    ,o.glec
    ,o.harm
    ,o.clss
    ,o.brand
    ,o.assc
    ,o.ddunit
    ,o.unti
    ,o.lbs
    ,o.plt
    ,o.plcd
    ,o.fs_line
    ,o.r_currency
    ,o.r_rate
    ,o.c_currency
    ,o.c_rate
    ,0::numeric
    ,(CASE WHEN pscale.factor = 0 THEN o.fb_qty * pscale.mod_price ELSE o.fb_val_loc * pscale.factor END)::numeric AS fb_val_loc
    ,o.fb_val_loc_dis
    ,o.fb_val_loc_qt
    ,o.fb_val_loc_pl
    ,o.fb_val_loc_tar
    ,0::numeric
    ,o.fb_cst_loc_cur
    ,o.fb_cst_loc_fut
    ,o.calc_status
    ,o.flag
    ,o.odate
    ,o.oseas
    ,o.rdate
    ,o.rseas
    ,o.sdate
    ,o.sseas
FROM
    volume o
    CROSS JOIN pscale
WHERE
    pscale.factor <> 0 or pscale.mod_price <> 0
)
INSERT INTO
    fc.live
SELECT * FROM volume UNION ALL SELECT * FROM pricing

