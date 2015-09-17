EXAMPLE OF CONFIGURATION: config.js

{
port: 8125
, backends: ['CoScale']
, flushInterval: 60000
, coscaleApiHost: 'http://api.qa.coscale.com'
, coscaleAccessToken: '91363d34-b6da-4528-a11c-cb03c5a83d02'
, coscaleAppId: '0000f738-7b15-48f0-a777-2fd2e897cede'
}
for logging add in config.js this field: debug:true

run the statsd:
-install node.js -> http://nodejs.org/download/
-download statsd -> https://github.com/etsy/statsd/
-place node_modules folder and the config.js file in statsd root directory and start statsd daemon with:
node stats.js config.js

example of how to send a metric to statsd on ubuntu:
echo "foo:1|c" | nc -u -w0 127.0.0.1 8125