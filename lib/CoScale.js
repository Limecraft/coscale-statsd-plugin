/*
 * StatsD backend
 * Flush stats to CoScale Api
 *
 * Configuration parameters
 *  backends: ['CoScale']
 *  flushInterval:
 *  coscaleApiHost:
 *  coscaleApiPath:
 *  coscaleAccessToken:
 *  coscaleAppId:
 *
 */

var util = require('util');
var http = require('http');
var https = require('https');
var protocol;
var os = require("os");
var querystring = require("querystring");

var coscaleApiHost;
var coscaleApiPath;
var coscaleAccessToken;
var coscaleHttpToken;
var coscaleAppId;
var HostName;
var flushInterval;
var debug;

var servergroup = 'statsd';
var percentiles = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 100];
var ids = {     //ids will store ids for metrics and servers to avoid unnecessary api calls
    serverList: {},
    metricList: {}
};
var coscaleStats = {};
var Buffer = {
    Metrics: {},
    Data: {}
};
var flushInProgress;
/*
 * This section contain the functions used for api calls
 */
var doHttpRequest;
//get the token from api
var loginToApi = function (callback) {
    var Data = {
        accessToken: coscaleAccessToken
    },
        location = coscaleApiPath + coscaleAppId + '/login/';
    doHttpRequest('POST', location, Data, function (statusCode, res) {
        if (statusCode === 200) {
            coscaleHttpToken = res.token;
        }
        if (callback && typeof callback === "function") {
            callback();
        }
    });
};

// This function is used on every http request
doHttpRequest = function (method, apiPath, coscaleMessage, callback) {
    var messageString = querystring.stringify(coscaleMessage),
    // setup the HTTPS request
        options = {
            host: coscaleApiHost,
            path: apiPath,
            method: method,
            headers: {}
        },
        req;
    if (coscaleHttpToken) {
        options.headers.HTTPAuthorization = coscaleHttpToken;
    }

    if (method === 'POST' || method === 'PUT') {
        options.headers["Content-Type"] = 'application/x-www-form-urlencoded';
    }

    req = protocol.request(options, function (res) {
        if (debug) {
            util.log("coscaleMessage: ", coscaleMessage, "statusCode: ", res.statusCode);
            //util.log("statusCode: ", res.statusCode);
            //util.log("headers: ", res.headers);
        }
        var body = [];
        // 4xx and 5xx errors except 409 statusCode which is for duplicate article
        if (res.statusCode >= 400 && res.statusCode !== 409) {
            if (debug) {
                util.log(res.statusCode + " error sending to CoScale Api");
            }
            coscaleStats.last_exception = Math.round(new Date().getTime() / 1000);
        }
        res.on('data', function (response) {
            body.push(response);
            if (debug) {
                util.log("CoScale Api RESPONSE: " + response);
            }
        });
        res.on('end', function () {
            if (res.statusCode === 401) { //Authentication failure
                if (debug) {
                    util.log('No valid access token procede to Authenticate');
                }
                loginToApi(function () {
                    doHttpRequest(method, apiPath, coscaleMessage, callback);
                });
            } else if (callback && typeof callback === "function") {
                callback(res.statusCode, JSON.parse(body.join('')));
            }
        });
    });
    req.on('error', function (e) {
        if (debug) {
            util.log(e);
        }
        flushInProgress = false;
    });
    req.write(messageString);
    req.end();
};

//add a group for servers
var addServerGroup = function (callback) {
    var data = {
        name: servergroup,
        description: 'Server group for ' + servergroup,
        type: 'type',
        source: servergroup
    },
        location = coscaleApiPath + coscaleAppId + '/servergroups/';
    doHttpRequest('POST', location, data, function (statusCode, res) {
        if (statusCode === 200 || res.type === 'DUPLICATE') { // if the server group was successful added or already exists, then add the server id to this group
            callback(res.id);
        }
    });
};

//put a server in a group
var addServerToGroup = function (groupId, ServerId) {
    var location = coscaleApiPath + coscaleAppId + '/servergroups/' + groupId + '/servers/' + ServerId + '/';
    doHttpRequest('POST', location, '');
};

//A server requires a server group. We will create a new server group only if the server
//that we are trying to add is not a duplicate on the api.
var addServer = function (hostName, callback) {
    if (ids.serverList[hostName]) {
        return callback();
    }
    var data = {
        name: hostName,
        description: 'Server ' + hostName,
        type: 'type',
        source: 'statsd'
    },
        location = coscaleApiPath + coscaleAppId + '/servers/';
    doHttpRequest('POST', location, data, function (statusCode, res) {
        var serverId = res.id;
        if (statusCode === 200) {  //Operation successful
            addServerGroup(function (groupId) {
                addServerToGroup(groupId, serverId);
            });
        }
        ids.serverList[hostName] = serverId;
        callback();
    });
};

//add a new group for metrics
var addMetricGroup = function (callback) {
    var data = {
        name: 'statsd',
        description: 'Metric group for ' + 'statsd',
        type: 'type',
        source: 'statsd',
        subjectType: 'SERVER'
    },
        location = coscaleApiPath + coscaleAppId + '/metricgroups/';
    doHttpRequest('POST', location, data, function (statusCode, res) {
        if (statusCode === 200 || res.type === 'DUPLICATE') {
            callback(res.id);
        }
    });
};

//put a metric in a group of metrics
var addMetricToGroup = function (groupId, metricId) {
    var location = coscaleApiPath + coscaleAppId + '/metricgroups/' + groupId + '/metrics/' + metricId + '/';
    doHttpRequest('POST', location, '');
};

//add a metric to API
var addMetric = function (metricName, dataType, callback) {
    if (ids.metricList[metricName]) {
        return callback();
    }
    var data = {
        name: metricName,
        description: metricName,
        dataType: dataType,
        period: '60',
        unit: '',
        subject: 'SERVER',
        source: 'StatsD'
    },
        location = coscaleApiPath + coscaleAppId + '/metrics/';
    doHttpRequest('POST', location, data, function (statusCode, res) {
        var metricId = res.id;
        if (statusCode === 200 || res.type === 'DUPLICATE') {
            addMetricGroup(function (groupId) {
                addMetricToGroup(groupId, metricId);
            });
        }
        ids.metricList[metricName] = metricId;
        callback();
    });
};

//Insert data into Datastore API
var postMetricValuesToApi = function (Data, callback) {
    var now = Math.round(new Date().getTime() / 1000),  //timeStamp in Unix Format
        data = {
            //source: 's' + ids.serverList[hostName],
            data: JSON.stringify(Data, function (key, value) {
                //replacer function that will transform each timestamp in -secondsAgo
                if (key === 'd' && value) {
                    var ret = [];
                    value.forEach(function (val) {
                        ret.push([val[0] - now, val[1]]);
                    });
                    return ret;
                }
                return value;
            })
        },
        location = coscaleApiPath + coscaleAppId + '/data/';
    doHttpRequest('POST', location, data, function () {
        callback();
    });
};

//return percentile values
var getPercentiles = function (values, percentiles) {
    var result = [],
        buckets = [],
        vals = values.slice(),
        valueNumber = vals.length;
    percentiles.forEach(function (percentile) {
        if (valueNumber === 0) {
            buckets.push(0);
        } else if (percentile >= 100) {
            buckets.push(vals[valueNumber - 1]);
        } else {
            buckets.push(vals[Math.floor(valueNumber * percentile / 100)]);
        }
    });
    result.push(valueNumber, percentiles[1], buckets);
    return result;
};

//Metric name will be like: servername.metricName or just metricName
var splitKey = function (key) {
    var splittedKey = key.split('.'),
        HostName,
        MetricName;
    if (splittedKey.length === 2) {
        HostName = splittedKey[0];
        MetricName = splittedKey[1];
    } else {
        MetricName = splittedKey[0];
    }
    return {
        'HostName': HostName,
        'MetricName': MetricName
    };
};

/*
 * coscale statsd backend will use a buffer to store the metrics and the values
 * this buffer automatically empties, when the values were successfuly sent to the api
 * if an error occurred, they will remain in the buffer until the first successful flush, in the limit of 10 different timestamps.
 * if this limit exceeded, then the oldest entry will be deleted
 * the next section contains functions for the buffer management
 *
*/

//addMetricsToBuffer will add the new metrics to the Buffer.Metrics
var addMetricsToBuffer = function (timestamp, metrics) {
    var counters = metrics.counters,
        gauges = metrics.gauges,
        timers = metrics.timers,
        BufferExceeded = false;

    // add counters
    Buffer.Metrics.counters = (Buffer.Metrics.hasOwnProperty('counters')) ? Buffer.Metrics.counters : {}; //create the object if doesn`t exists
    Object.keys(counters).forEach(function (metricName) {
        //if the metricName property(array) doens`t already exists in Buffer.Metrics, then create it.
        Buffer.Metrics.counters[metricName] = (Buffer.Metrics.counters.hasOwnProperty(metricName)) ? Buffer.Metrics.counters[metricName] : [];
        Buffer.Metrics.counters[metricName].push([timestamp, counters[metricName] / (flushInterval / 1000)]); //calculate "per second" rate
        // we keep the data in the buffer for only 10 timestamps/metric. When the buffer is full, delete the oldest entry
        if (Buffer.Metrics.counters[metricName].length > 10) {
            BufferExceeded = true;
            Buffer.Metrics.counters[metricName].shift();
        }
    });
    // same for gauges
    Buffer.Metrics.gauges = (Buffer.Metrics.hasOwnProperty('gauges')) ? Buffer.Metrics.gauges : {};
    Object.keys(gauges).forEach(function (metricName) {
        Buffer.Metrics.gauges[metricName] = (Buffer.Metrics.gauges.hasOwnProperty(metricName)) ? Buffer.Metrics.gauges[metricName] : [];
        Buffer.Metrics.gauges[metricName].push([timestamp, gauges[metricName]]);
        if (Buffer.Metrics.gauges[metricName].length > 10) {
            BufferExceeded = true;
            Buffer.Metrics.gauges[metricName].shift();
        }
    });
    // same for timers
    Buffer.Metrics.timers = (Buffer.Metrics.hasOwnProperty('timers')) ? Buffer.Metrics.timers : {};
    Object.keys(timers).forEach(function (metricName) {
        Buffer.Metrics.timers[metricName] = (Buffer.Metrics.timers.hasOwnProperty(metricName)) ? Buffer.Metrics.timers[metricName] : [];
        Buffer.Metrics.timers[metricName].push([timestamp, getPercentiles(timers[metricName], percentiles)]);
        if (Buffer.Metrics.timers[metricName].length > 10) {
            BufferExceeded = true;
            Buffer.Metrics.timers[metricName].shift();
        }
    });
    if (BufferExceeded && debug) {
        util.log('Maximum metrics buffer size exceeded. Dropping data...');
    }
};

var addDataToBuffer = function (metricName, data) {
    Buffer.Data[metricName] = (Buffer.Data.hasOwnProperty(metricName)) ? Buffer.Data[metricName] : []; //create the array if it doesn`t exists
    Buffer.Data[metricName].push(data);
    if (Buffer.Data[metricName].length > 10) {
        if (debug) {
            util.log('Maximum data buffer size exceeded. Dropping data...');
        }
        Buffer.Data[metricName].shift();
    }
};

//firstKey will return the first key of a object
var firstKey = function (obj) {
    var first;
    for (first in obj) {
        if (obj.hasOwnProperty(first)) {
            break;
        }
    }
    return first;
};

//check if the object is empty or not
var isEmpty = function (obj) {
    return !Object.keys(obj).length;
};

//flushToApi function will make a chain of calls to the api in order to send the metrics and values
//it will send all the metrics from the buffer, so after a metric value was added, flushToApi call itself again for all the metrics except the last one that was added
//if the server was previously added then we will skip the "add server", "add server group" and "add server to group" calls. (same for the metrics)
var flushToApi = function () {
    var metricsType = firstKey(Buffer.Metrics),
        dataType = (metricsType === 'timers') ? 'HISTOGRAM' : 'DOUBLE',
        metrics = Buffer.Metrics[metricsType],
        key = firstKey(metrics),
        splittedKey,
        hostName,
        values,
        data,
        metricName;
    if (key) {
        splittedKey = splitKey(key);
        hostName = splittedKey.HostName;
        metricName = splittedKey.MetricName;
        if (!hostName) {
            //if the HostName wasn`t found in the key, then we will use the hostName of the server
            hostName = HostName;
        }
        addServer(hostName, function () {
            addMetric(metricName, dataType, function () {
                values = metrics[key].splice(0, metrics[key].length);
                addDataToBuffer(metricName, {
                    'm': ids.metricList[metricName],
                    's': 's' + ids.serverList[hostName],
                    'd': values
                });
                delete metrics[key];
                return flushToApi();
            });
        });
    } else {
        delete Buffer.Metrics[metricsType];
        if (!isEmpty(Buffer.Metrics)) {
            return flushToApi();
        }
        if (!isEmpty(Buffer.Data)) {
            data = [];
            Object.keys(Buffer.Data).forEach(function (key) {
                data = data.concat(Buffer.Data[key]);
            });
            postMetricValuesToApi(data, function () {
                //if the values were successsfuly sent to api, then delete those values from the
                Buffer.Data = {};
                flushInProgress = false;
            });
        }
    }
};

/*Add the new metrics to the buffer and send them to the api*/
var flush_stats = function coscale_flush(timestamp, metrics) {
    if (debug) {
        util.log('Start the metrics flush');
    }
    addMetricsToBuffer(timestamp, metrics);
    if (!flushInProgress) {
        flushInProgress = true;
        flushToApi();
    }
    coscaleStats.last_flush = Math.round(new Date().getTime() / 1000);
};

var backend_status = function coscale_status(writeCb) {
    var stat;
    for (stat in coscaleStats) {
        if (coscaleStats.hasOwnProperty(stat)) {
            writeCb(null, 'coscale', stat, coscaleStats[stat]);
        }
    }
};

exports.init = function init(startup_time, config, emitter) {
    debug = config.debug;
    coscaleApiHost = config.coscaleApiHost;
    if (coscaleApiHost.indexOf('https://') !== -1) {
        coscaleApiHost = coscaleApiHost.substr(8);
        protocol = https;
    } else if (coscaleApiHost.indexOf('http://') !== -1) {
        coscaleApiHost = coscaleApiHost.substr(7);
        protocol = http;
    } else {
        util.log('Specify the protocol of coscaleApiHost in configuration file.');
        return false;
    }
    coscaleApiPath = '/api/v1/app/';
    coscaleAccessToken = config.coscaleAccessToken;
    if (!coscaleAccessToken) {
        util.log("No API Token set, CoScale API Token must be set in the configuration file before posting data");
        return false;
    }
    coscaleAppId = config.coscaleAppId;
    if (!coscaleAppId) {
        util.log("No app id set, CoScale app id must be set in the configuration file before posting data");
        return false;
    }
    coscaleStats.last_flush = startup_time;
    coscaleStats.last_exception = startup_time;
    flushInterval = config.flushInterval;
    HostName = os.hostname().toUpperCase();

    loginToApi();

    emitter.on('flush', flush_stats);
    emitter.on('status', backend_status);
    return true;
};
