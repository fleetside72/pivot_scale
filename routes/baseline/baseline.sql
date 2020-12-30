DELETE FROM fc.live WHERE version = 'forecast_name';
WITH
baseline AS (
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
    ,o._comment
    ,o._logid
    ,o._tag
    ,'forecast_name' "version"
    ,'actuals' iter
FROM
    fc.live o
WHERE
    (
        --base period orders booked....
        odate BETWEEN 'app_baseline_from_date'::date AND 'app_baseline_to_date'::date
        --...or any open orders currently booked before cutoff....
        OR (calc_status IN (app_openstatus_code) and odate <= 'app_openorder_cutoff'::date)
        --...or anything that shipped in that period
        OR (sdate BETWEEN 'app_baseline_from_date'::date AND 'app_baseline_to_date'::date)
    )
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
UNION ALL
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
    ,o.odate + interval '1 year' AS odate
    ,odate.ssyr
    ,o.rdate + interval '1 year' AS rdate
    ,rdate.ssyr
    ,o.sdate + interval '1 year' AS sdate
    ,sdate.ssyr
    ,o._comment
    ,o._logid
    ,o._tag
    ,'forecast_name' "version"
    ,'plug' iter
FROM
    fc.live o
LEFT OUTER JOIN fc.perd rdate ON
        (o.rdate + interval '1 year' )::date <@ rdate.drange
LEFT OUTER JOIN fc.perd odate ON
        (o.odate + interval '1 year' )::date <@ odate.drange
LEFT OUTER JOIN fc.perd sdate ON
        (o.sdate + interval '1 year' )::date <@ sdate.drange
WHERE
    odate BETWEEN 'app_plug_fromdate'::date AND 'app_plug_todate'::date
    --be sure to pre-exclude unwanted items, like canceled orders, non-gross sales, and short-ships
)
,incr AS (
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
    ,o.odate + interval '1 year' AS odate
    ,odate.ssyr
    ,o.rdate + interval '1 year' AS rdate
    ,rdate.ssyr
    ,o.sdate + interval '1 year' AS sdate
    ,sdate.ssyr
    ,o._comment
    ,o._logid
    ,o._tag
    ,'forecast_name' "version"
    ,'baseline' iter
FROM
    baseline o
LEFT OUTER JOIN fc.perd rdate ON
        (o.rdate + interval '1 year' )::date <@ rdate.drange
LEFT OUTER JOIN fc.perd odate ON
        (o.odate + interval '1 year' )::date <@ odate.drange
LEFT OUTER JOIN fc.perd sdate ON
        (o.sdate + interval '1 year' )::date <@ sdate.drange
)
,ins AS (
INSERT INTO
    fc.live
SELECT
    *
FROM
    incr i
WHERE
    i.odate >= 'app_first_forecast_date'::date
    OR i.sdate >= 'app_first_forecast_date'::date
RETURNING *
)
SELECT COUNT(*) num_rows  FROM ins
