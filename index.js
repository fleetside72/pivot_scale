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

server.get('/login', (req, res) => res.sendFile(process.env.wd + 'msauth.html'))

server.get('/logs', (req, res) => res.sendFile(process.env.wd + 'changes.log'))

server.get('/pgbadger', (req, res) => res.sendFile(process.env.wd + 'logs.html'))

server.get('/totals', (req, res) => res.sendFile(process.env.wd + 'totals.log'))

server.get('/test_sql', function(req, res) {
    var path = './route_meta/scenario_package.sql'
    var callback = function(arg) {
        res.send(arg)
    };

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            callback(err);
        }
    });
});

server.get('/get_pool', bodyParser.json(), function(req, res) {

    var sql = "";
    var args = [req.body.quota_rep];
    var path = './route_sql/get_pool.sql';
    var callback = function(arg) {
        sql = arg;
        console.log(new Date().toISOString() + "-------------------------get pool:----------------------------");
        console.log(req.body.quota_rep);
        sql = sql.replace("rep_replace", req.body.quota_rep);
        console.log(sql);
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

server.get('/swap_fit', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/swap_fit.sql';

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
        console.log(new Date().toISOString() + "-------------------------get swap fit:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("replace_new_mold", 'g'), req.body.new_mold);
        //execute the sql and send the result
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    };
})

server.post('/swap', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/swap_post.sql';

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
        console.log(new Date().toISOString() + "-------------------------get swap fit:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("swap_doc", 'g'), JSON.stringify(req.body.swap));
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        //execute the sql and send the result
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    };
})

server.post('/cust_swap', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/swap_cust.sql';

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
        console.log(new Date().toISOString() + "-------------------------get swap fit:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("swap_doc", 'g'), JSON.stringify(req.body.swap));
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        //execute the sql and send the result
        console.log(sql);
        res.json(null);
        //Postgres.FirstRow(sql, [], res)
    };
})

server.get('/list_changes', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/list_changes.sql';

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
        console.log(new Date().toISOString() + "-------------------------list changes:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("where_clause", 'g'), w)
            //execute the sql and send the result
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    };
})

server.get('/undo_change', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/undo.sql';

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

        console.log(new Date().toISOString() + "-------------------------undo change:------------------------------")
        console.log(req.body);
        //parse the where clause into the main sql statement
        sql = sql.replace(new RegExp("replace_id", 'g'), JSON.stringify(req.body.logid))
            //execute the sql and send the result
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    };
})

//deprecating this route, just use _vp for volume and prive
/*
server.post('/addmonth_v', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1; //counts iterations through each scaenario key
    var d = 1; //counts cycles in scenario key values which are arrays
    var args = [];
    var path = './route_sql/addmonth_vd.sql';

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
        //buile where clause expression
        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "-----------------------------add month volume:---------------------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("scenario = target_scenario", 'g'), w);
        sql = sql.replace(new RegExp("target_increment", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("target_month", 'g'), req.body.month);
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql)
        Postgres.FirstRow(sql, [], res)
    }
})
*/

server.post('/addmonth_vp', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/addmonth_vupd.sql';

    var callback = function(arg) {
        sql = arg;

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "------------------add month volume and price:-------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("target_volume", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("target_price", 'g'), req.body.amount);
        sql = sql.replace(new RegExp("target_month", 'g'), req.body.month);
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });
})

server.post('/scale_v', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/scale_vd.sql';

    var callback = function(arg) {
        sql = arg;

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "-----------------------scale volume:------------------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("incr_qty", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });
})

server.post('/scale_p', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/scale_pd.sql';

    var callback = function(arg) {
        sql = arg;

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "--------------------scale price:-------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("target_increment", 'g'), req.body.amount);
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });
})

server.post('/scale_vp', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/scale_vupd.sql';

    var callback = function(arg) {
        sql = arg;

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "--------------------scale volume & price:-------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("target_vol", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("target_prc", 'g'), req.body.amount);
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });
})

server.post('/new_part', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/new_part.sql';

    var callback = function(arg) {
        sql = arg;

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "--------------------new part:-------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("target_vol", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("target_prc", 'g'), req.body.amount);
        sql = sql.replace(new RegExp("replace_request", 'g'), JSON.stringify(req.body));
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

    fs.readFile(path, 'utf8', function(err, data) {
        if (!err) {
            callback(data);
        } else {
            console.log("fatal error pulling sql file")
            callback(err);
        }
    });
})

server.post('/new_basket', bodyParser.json(), function(req, res) {

    var sql = "";
    var w = "";
    var c = 1;
    var d = 1;
    var args = [];
    var path = './route_sql/new_basket.sql';

    var callback = function(arg) {
        sql = arg;
        req.body.scenario.iter.push("adj volume"); //intercept the request body and force in a "adj volume" at position 1, only a "copy" iteration is being used

        ({ c, w, d } = build_where(req, c, w, d, args));

        if (c == 1) {
            res.send("no body was sent");
            return;
        }
        console.log(new Date().toISOString() + "--------------------new basket:-------------------");
        req.body.stamp = new Date().toISOString()
        console.log(req.body);
        //console.log(args);
        sql = sql.replace(new RegExp("where_clause", 'g'), w);
        sql = sql.replace(new RegExp("target_vol", 'g'), req.body.qty);
        sql = sql.replace(new RegExp("target_prc", 'g'), req.body.amount);
        sql = sql.replace(new RegExp("replace_request", 'g'), JSON.stringify(req.body));
        sql = sql.replace(new RegExp("replace_version", 'g'), req.body.scenario.version);
        sql = sql.replace(new RegExp("replace_source", 'g'), req.body.source);
        sql = sql.replace(new RegExp("replace_iterdef", 'g'), JSON.stringify(req.body));
        console.log(sql);
        Postgres.FirstRow(sql, [], res)
    }

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