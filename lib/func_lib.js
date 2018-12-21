/* jshint esnext: true */
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-q/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-q/lib/email_lib.js');


/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 *
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});

/**
 *  Publishes message on api-group-e2e-c topic with response on whether message has been processed successfully
 */
function publishOnGroupE2EC(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiGroupE2EC.*';
            var toipcName = `apiGroupE2EC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2EQErrorEmail('API Group E2E Q publishOnGroupE2EC AMPQ connection was null');
    }
}


/**
 * Model of group_one_time_pre_key_pair table
 * 
 */
const OneTimeGroupPreKey = sequelize.define('group_one_time_pre_key_pair', {
    user_id: { 
        type: Sequelize.INTEGER,
        allowNull: false
    },
    group_id: { 
        type: Sequelize.STRING,
        allowNull: false
    },
    group_one_time_pre_key_pair_pbk: {type: Sequelize.BLOB('long'), allowNull: false},
    group_one_time_pre_key_pair_uuid: {type: Sequelize.STRING, allowNull: false}
    }, {
        freezeTableName: true, // Model tableName will be the same as the model name
        timestamps: false,
        underscored: true
    }
);


/**
 *  Saves public one time keys into db
 * 
 * (oneTimeGroupPreKeyPairPbks Object): object that holds all the one time keys
 * (amqpConn Object): RabbitMQ connection object that is used to send api-group-e2e-c response
 * (topic string): Topic on which response is going to be published
 */
module.exports.uploadGroupPublicKeys = function(oneTimeGroupPreKeyPairPbks, amqpConn, topic){
    OneTimeGroupPreKey.bulkCreate(oneTimeGroupPreKeyPairPbks.oneTimeGroupPreKeyPairPbks, { fields: ['user_id','group_id', 'group_one_time_pre_key_pair_pbk', 'group_one_time_pre_key_pair_uuid'] }).then(() => {
        var response = {
            status: config.rabbitmq.statuses.ok
        };
        publishOnGroupE2EC(amqpConn, JSON.stringify(response), topic);
    }).error(function(err){
        email.sendApiGroupE2EQErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishOnGroupE2EC(amqpConn, JSON.stringify(response), topic);
    });
};

/**
 * Deletes all public group one time keys used for one message
 * 
 * (uuids string): Comma separated list of uuids of the public group one time keys to be deleted
 * (ampqConn Object): RabbitMQ connection object used to send messages via messaging queue
 * (topic string): Topic on which successful processing verification message is sent
 */
module.exports.deleteGroupOneTimeKeysByUUIDS = function (uuids, amqpConn, topic) {
    sequelize.query('CALL DeleteGroupOneTimePublicKeysByUUID(?)',
    { replacements: [ uuids ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishOnGroupE2EC(amqpConn, JSON.stringify(response), topic);
    }).error(function(err){
        email.sendApiGroupE2EQErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishOnGroupE2EC(amqpConn, JSON.stringify(response), topic);
    });
}

/**
 *  Fetches group one time public keys for one group message
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.getGroupOneTimeKeys = function(req,res){
    var groupOneTimeKeys = [];

    sequelize.query('CALL GetGroupOneTimeKeys(?,?)',
    { replacements: [ req.query.groupChatId, req.query.userId ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            for (var i = 0; i < result.length; i++) {
                if(parseInt(req.query.userId) !== parseInt(result[i].user_id)){
                    var groupOneTimeKey = {
                        userId: result[i].user_id,
                        groupChatId: result[i].group_id,
                        oneTimeGroupPublicKey: Array.prototype.slice.call(result[i].group_one_time_pre_key_pair_pbk, 0),
                        uuid: result[i].group_one_time_pre_key_pair_uuid
                    };
                    groupOneTimeKeys.push(groupOneTimeKey);
                }
            }
            res.json(groupOneTimeKeys);
    }).error(function(err){
        email.sendApiGroupE2EQErrorEmail(err);
        res.json(groupOneTimeKeys);
    });
};


/**
 *  Checks if group one time public keys need to be replenished
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.checkIfGroupKeysNeeded = function(req,res){
    var groupIds = [];

    sequelize.query('CALL CheckIfGroupKeysNeeded(?,?)',
    { replacements: [ req.query.groupChatIds, req.query.userId ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            for (var i = 0; i < result.length; i++) {
                if(result[i].group_keys < 100){
                    groupIds.push(result[i].group_id);
                }
            }
            res.json(groupIds);
    }).error(function(err){
        email.sendApiGroupE2EQErrorEmail(err);
        res.json(groupIds);
    });
};