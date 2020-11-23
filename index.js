#!/usr/bin/env node

require('dotenv').config();
const express = require('express');
var https = require('https');
var bodyParser = require('body-parser');
const server = express();
const pg = require('pg');

//---------read sql files into variables----------------
var fs = require('fs');
var readline = require('readline');
//-------------------------------------------------------

var options = {
    key: fs.readFileSync(process.env.wd + 'key.pem'),
    cert: fs.readFileSync(process.env.wd + 'cert.pem'),
    passprase: []
};

https.createServer(options, server).listen(process.env.nodeport, () => {
    console.log('started on ' + process.env.nodeport)
});
//server.listen(3000, () => console.log('started'))

var Postgres = new pg.Client({
    user: process.env.user,
    password: process.env.password,
    host: process.env.host,
    port: process.env.port,
    database: process.env.database,
    ssl: false,
    application_name: "osm_api"
});
Postgres.connect();

Postgres.FirstRow = function(inSQL, args, inResponse) {
    Postgres.query(inSQL, args, (err, res) => {
        if (err === null) {
            inResponse.json(res.rows[0]);
            return;
        }
        console.log(err.stack);
        inResponse.json(err.stack);
    });
};

server.get('/', (req, res) => res.send('node.js express is up and running'))

server.get('/baseline', bodyParser.json(), function(req, res) {

    var sql = "";
    var path = './route_sql/baseline.sql';
    var args = [];

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });

    //list of parameters that will need to be supplied from the app
    //app_baseline_from_date
    //app_baseline_to_date
    //app_first_forecast_date
    //app_openorder_cutoff
    //app_openstatus_code
    //app_plug_fromdate
    //app_plug_todate

    var app_baseline_from_date = '2020-06-01';
    var app_baseline_to_date = '2020-09-30';
    var app_first_forecast_date = '2021-06-01';
    var app_openorder_cutoff = '2020-09-30';
    var app_openstatus_code = "'OPEN','BACKORDER'";
    var app_plug_fromdate = '2020-10-01'; 
    var app_plug_todate = '2020-05-30';

    var callback = function(arg) {
        sql = arg;

        console.log(new Date().toISOString() + "-------------------------baseline build-----------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        //sql = sql.replace(new RegExp("where_clause", 'g'), w)
        sql = sql.replace(new RegExp("app_baseline_from_date"), app_baseline_from_date);
        sql = sql.replace(new RegExp("app_baseline_to_date"), app_baseline_from_date);
        sql = sql.replace(new RegExp("app_first_forecast_date"), app_first_forecast_date);
        sql = sql.replace(new RegExp("app_openorder_cutoff"), app_baseline_from_date);
        sql = sql.replace(new RegExp("app_openstatus_code"), app_baseline_from_date);
        sql = sql.replace(new RegExp("app_plug_fromdate"), app_plug_fromdate);
        sql = sql.replace(new RegExp("app_plug_todate"), app_plug_todate);
        //execute the sql and send the result
        args.push(req.body.app_baseline_from_date);
        console.log(sql);
        res.send(sql); 
        //Postgres.FirstRow(sql, [], res)
    };
})

server.get('/scenario_package', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/scenario_package.sql';

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });

    var callback = function(arg) {
        sql = arg;

        //parse request body into a where clause
        ({ c, w, d } = build_where(req, c, w, d, args));

        //if there was no body sent, return with nothing
        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "-------------------------get scenario:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("where_clause", 'g'), w)
            //execute the sql and send the result
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    };
})

function build_where(req, c, w, d, args) {
    for (var i in req.body.scenario) {
        //console.log(i);
        ///console.log(req.body[i]);
        if (c > 1) {
            w = w +
                `
            AND `;
        }
        if (Array.isArray(req.body.scenario[i])) {
            //if the scenario key has a value that is an array of items, push it into an `IN` statement
            //iter = [stage1, stage2]   -->  SQL  -->  iter IN ('stag1', stage2')
            w = w + i + " IN (";
            for (var j in req.body.scenario[i]) {
                if (d > 1) {
                    w = w + ",";
                }
                w = w + "'" + req.body.scenario[i][j] + "'";
                d = d + 1;
            }
            w = w + ")";
        } else {
            w = w + i + " = '" + req.body.scenario[i] + "'";
        }
        args.push(req.body.scenario[i]);
        c = c + 1;
    };
    return { c, w, d };
}
