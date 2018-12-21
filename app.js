/* jshint esnext: true */
require('events').EventEmitter.prototype._maxListeners = 0;
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-q/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-q/lib/email_lib.js');
var functions = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-q/lib/func_lib.js');
var fs = require("fs");
var express = require("express");
var http = require('http');
var https = require('https');
var amqp = require('amqplib/callback_api');
var options = {
    key: fs.readFileSync(config.security.key),
    cert: fs.readFileSync(config.security.cert)
};
var app = express();
var bodyParser = require("body-parser");
var cors = require("cors");
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.raw({ limit: '50mb' }));
app.use(bodyParser.text({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: false }));
app.use(express.static("./public"));
app.use(cors());

app.use(function(req, res, next) {
    next();
});

var server = https.createServer(options, app).listen(config.port.group_e2e_q_port, function() {
    email.sendNewApiGroupE2EQIsUpEmail();
});



/**
 *   RabbitMQ connection object
 */
var amqpConn = null;


/**
 *  Subscribe api-group-e2e-c to topic to receive messages
 */
function subscribeToGroupE2EQ(topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiGroupE2EQ.*';
            var toipcName = `apiGroupE2EQ.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.assertQueue(toipcName, { exclusive: false, auto_delete: true }, function(err, q) {
                ch.bindQueue(q.queue, exchange, toipcName);
                ch.consume(q.queue, function(msg) {
                    // check if status ok or error
                     if (toipcName === `apiGroupE2EQ.${config.rabbitmq.topics.deleteOneTimePublicKeysByUUID}`){
                        functions.deleteGroupOneTimeKeysByUUIDS(msg.content.toString(), amqpConn, config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDQ);
                    } else if (toipcName === `apiGroupE2EQ.${config.rabbitmq.topics.newGroupE2EKeys}`){
                        var message = JSON.parse(msg.content.toString());
                        functions.uploadGroupPublicKeys(message, amqpConn, config.rabbitmq.topics.newGroupE2EKeysQ);
                    }  
                }, { noAck: true });
            });
        });
    }
}

/**
 *  Connect to RabbitMQ
 */
function connectToRabbitMQ() {
    amqp.connect(config.rabbitmq.url, function(err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(connectToRabbitMQ, 1000);
        }
        conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(connectToRabbitMQ, 1000);
        });
        console.log("[AMQP] connected");
        amqpConn = conn;

        // Subscribe to all the topics
        subscribeToGroupE2EQ(config.rabbitmq.topics.deleteOneTimePublicKeysByUUID);
        subscribeToGroupE2EQ(config.rabbitmq.topics.newGroupE2EKeys);
    });
}


connectToRabbitMQ();


/**
 *  Fetches group one time public keys for one group message
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
app.get("/getGroupOneTimeKeys", function(req, res) {
    functions.getGroupOneTimeKeys(req, res);
});


/**
 *  Checks if group one time public keys need to be replenished
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
app.post("/checkIfGroupKeysNeeded", function(req, res) {
    functions.checkIfGroupKeysNeeded(req, res);
});