#!/usr/bin/env node

require('dotenv').config();
const express = require('express');
var https = require('https');
var bodyParser = require('body-parser');
const server = express();
const pg = require('pg');
var fs = require('fs');
var readline = require('readline');

//-----------------------------point to ssl info------------------------------------
var options = {
    key: fs.readFileSync(process.env.wd + 'key.pem'),
    cert: fs.readFileSync(process.env.wd + 'cert.pem'),
    passprase: []
};

//-----------------------------create server process--------------------------------
https.createServer(options, server).listen(process.env.nodeport, () => {
    console.log('started on ' + process.env.nodeport)
});

//-----------------------------create permanent database connetion------------------
var Postgres = new pg.Client({
    user: process.env.user,
    password: process.env.password,
    host: process.env.host,
    port: process.env.port,
    database: process.env.database,
    ssl: false,
    application_name: "ps_api"
});
Postgres.connect();

//------------create a callable sql exec func that return first rows----------------
Postgres.FirstRow = function(inSQL, args, inResponse) {
    Postgres.query(inSQL, args, (err, res) => {
        if (err === null) {
            inResponse.json(res[1].rows[0]);
            return;
        }
        console.log(err.stack);
        inResponse.json(err.stack);
    });
};

//------------route to test if the process is running-------------------------------
server.get('/', (req, res) => res.send('pivotscale api is running'))

//------------build forecast baseline that is a mirror of the target period---------
server.get('/baseline', bodyParser.json(), function(req, res) {

    var sql = "";
    var path = './routes/baseline/baseline.sql';
    var args = [];
   

    var app_baseline_from_date =        req.body.app_baseline_from_date;
    var app_baseline_to_date =          req.body.app_baseline_to_date;
    var app_first_forecast_date =       req.body.app_first_forecast_date;
    var app_openorder_cutoff =          req.body.app_openorder_cutoff;
    var app_plug_fromdate =             req.body.app_plug_fromdate;
    var app_plug_todate =               req.body.app_plug_todate;
    var app_openstatus_code =           req.body.app_openstatus_code;

    var callback = function(arg) {
        sql = arg;
        console.log(new Date().toISOString() + "-------------------------baseline build-----------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        //sql = sql.replace(new RegExp("where_clause", 'g'), w)
        sql = sql.replace(new RegExp("app_baseline_from_date", 'g'),    app_baseline_from_date);
        sql = sql.replace(new RegExp("app_baseline_to_date", 'g'),      app_baseline_to_date);
        sql = sql.replace(new RegExp("app_first_forecast_date", 'g'),   app_first_forecast_date);
        sql = sql.replace(new RegExp("app_openorder_cutoff", 'g'),      app_openorder_cutoff);
        sql = sql.replace(new RegExp("app_openstatus_code", 'g'),       app_openstatus_code);
        sql = sql.replace(new RegExp("app_plug_fromdate", 'g'),         app_plug_fromdate);
        sql = sql.replace(new RegExp("app_plug_todate", 'g'),           app_plug_todate);
        //execute the sql and send the result
        args.push(req.body.app_baseline_from_date);
        console.log(sql);
        //res.send(sql); 
        Postgres.FirstRow(sql, [], res)
    };
    
    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });

})

//------------scale a selected slice by the specified amounts-----------------------
server.get('/scale', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = ""; //holds the where
    var c = 1;  //flag if body is empty
    var d = 1;
    var path = './routes/scale/scale.sql';
    var args = [];
   
    var app_pincr    =  req.body.app_pincr;
    var app_req      =  JSON.stringify(req.body);
    var app_vincr    =  req.body.app_vincr;

    var callback = function(arg) {
        sql = arg;
        ({ c, w, d } = build_where(req, c, w, d, args));
        //if there was no body sent, return with nothing
        //if (c == 1) {
        //    res.send("no body was sent");
        //    return;
        //}
        console.log(new Date().toISOString() + "-------------------------baseline build-----------------------------")
        console.log(JSON.stringify(req.body));
        //parse the where clause into the main sql statement
        //sql = sql.replace(new RegExp("where_clause", 'g'), w)
        sql = sql.replace(new RegExp("app_pincr", 'g'),    app_pincr);
        sql = sql.replace(new RegExp("app_req", 'g'),      app_req);
        sql = sql.replace(new RegExp("app_vincr", 'g'),    app_vincr);
        sql = sql.replace(new RegExp("app_where", 'g'),    w);
        //execute the sql and send the result
        args.push(req.body.app_baseline_from_date);
        console.log(sql);
        res.send(sql); 
        //Postgres.FirstRow(sql, [], res)
    };
    
    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });

})

function build_where(req, c, w, d, args) {
    //loop through each top level item expected to be a simple key/value list reflecting the column and the target value
    // "part":"XFRM500", "customer":"Sanford and Son" --> SQL -->     part = 'XFRM500'
    //                                                            AND customer = 'Sanford and Son'
    for (var i in req.body.app_scenario) {
        //console.log(i);
        ///console.log(req.body[i]);
        //this step applies the AND seperator only
        if (c > 1) {
            w = w +
                `
            AND `;
        }
        if (Array.isArray(req.body.app_scenario[i])) {
            //if the scenario key has a value that is an array of items, push it into an `IN` statement
            //iter = [stage1, stage2]   -->  SQL  -->  iter IN ('stag1', stage2')
            w = w + i + " IN (";
            for (var j in req.body.app_scenario[i]) {
                if (d > 1) {
                    w = w + ",";
                }
                w = w + "'" + req.body.app_scenario[i][j] + "'";
                d = d + 1;
            }
            w = w + ")";
        } else {
            w = w + i + " = '" + req.body.app_scenario[i] + "'";
        }
        args.push(req.body.app_scenario[i]);
        c = c + 1;
    };
    return { c, w, d };
}
