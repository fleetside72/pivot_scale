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

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });

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
})
