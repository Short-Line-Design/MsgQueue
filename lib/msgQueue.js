/*
 * The Message Queue
 *
 * Copyright (c) 2012 Short Line Design Inc.
 * Copyright (c) 2012 Dan Prescott <danpres14@gmail.com>
 * The MIT License (MIT)
 *
 * The Message Queue is a lightweight message queue
 * designed to provide sequential processing of all tasks in
 * a message group (based on a group key) while maintaining highly
 * parallel processing across multiple message groups with disparate
 * keys.  This message queue also provides for highly parallel processing
 * of all tasks that have not been assigned to a message group (the group key is
 * undefined) such that each task is passed to a single worker's callback function.
 *
 */

var _ = require('underscore')
  , mongodb = require('mongodb')
  , redis = require('redis')
  , async = require('async')
  , util = require('util')

  , worker = require('./worker')
  , queue = require('./queue')
  , task = require('./task');

// ----------------------------------------------------------------------------------------------------
// The msg queue static functions and variables
// ----------------------------------------------------------------------------------------------------
var _msgQueueMap = {}, _msgWorkerMap = {}; _msgWorkerCount = 0;

var _addMsgQueue = function (queueName, msgQueue, callback) {
    // Make sure the msg queue does not exist
    if (_.isUndefined(_msgQueueMap[queueName])) {
        // Add the queue (with wrapper) to the map
        _msgQueueMap[queueName] = {
            queueName: queueName,
            msgQueue: msgQueue
        };
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' already exists.');
    }
    return false;
};

var _addMsgWorker = function (queueName, msgWorker, callback) {
    // Set the msg worker name (unique within this process)
    var workerName = '' + (++_msgWorkerCount) + '-' + queueName;
    // Add the worker (with wrapper) to the map
    _msgWorkerMap[workerName] = {
        msgQueueName: queueName,
        workerName: workerName,
        msgWorker: undefined,
        workerMaxTasks: 1,
        stop: false
    };
    // Set the msgWorker
    if (_.isFunction(msgWorker)) {
        _msgWorkerMap[workerName].msgWorker = msgWorker;
    } else if (_.isObject(msgWorker) && _.isFunction(msgWorker.workerCallback)) {
        _msgWorkerMap[workerName].msgWorker = msgWorker.workerCallback;
    }
    // Set the worker max tasks
    if (_.isObject(msgWorker) && _.isNumber(msgWorker.workerMaxTasks)) {
        _msgWorkerMap[workerName].workerMaxTasks = msgWorker.workerMaxTasks;
    }

    // Return success via return or callback
    if (_.isFunction(callback)) {
        return callback(undefined, _msgWorkerMap[workerName]);
    }
    return _msgWorkerMap[workerName];
};

var _getMsgQueue = function (queueName, callback) {
    // Make sure the msg queue exists
    if (!_.isUndefined(_msgQueueMap[queueName])) {
        // Return the msg queue via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgQueueMap[queueName].msgQueue);
        }
        return _msgQueueMap[queueName].msgQueue;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' does not exist.');
    }
    return undefined;
};

var _getMsgWorker = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Return the msg worker via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName]);
        }
        return _msgWorkerMap[workerName];
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _getMsgWorkerStop = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Return the msg worker stop via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName].stop);
        }
        return _msgWorkerMap[workerName].stop;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _setMsgWorkerStop = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Set the msg worker stop
        _msgWorkerMap[workerName].stop = true;
        // Return the msg worker stop via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName].stop);
        }
        return _msgWorkerMap[workerName].stop;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _removeMsgQueue = function (queueName, callback) {
    // Make sure the msg queue exists
    if (!_.isUndefined(_msgQueueMap[queueName])) {
        // Remove the queue from the map
        delete _msgQueueMap[queueName];
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' does not exist.');
    }
    return false;
};

var _removeMsgWorker = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Remove the worker from the map
        delete _msgWorkerMap[workerName];
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return false;
};

var _setMsgQueueName = function (appName, queueName) {
    // Combine the app name with the queue name
    var myMsgQueueName = (_.isString(appName) && !_.isEmpty(appName)) ? appName : 'MsgQueue';
    if (_.isString(queueName) && !_.isEmpty(queueName)) {
        myMsgQueueName += ':' + queueName;
    } else {
        myMsgQueueName += ':--';
    }
    return myMsgQueueName;
};

var _setMsgGroupName = function (appName, queueName, groupName) {
    // Combine the app name and queue name with the group name
    var myMsgGroupName = _setMsgQueueName(appName, queueName);
    if (_.isString(groupName) && !_.isEmpty(groupName)) {
        myMsgGroupName += ':' + groupName;
    } else {
        myMsgGroupName += ':--';
    }
    return myMsgGroupName;
};

// ----------------------------------------------------------------------------------------------------
// The msg queue class factory methods and the msg queue class
// ----------------------------------------------------------------------------------------------------
var createRedisMsgQueue = function (appName, queueName, redisOptions, callback) {
    if (_.isFunction(redisOptions)) {
        callback = redisOptions;
        redisOptions = undefined;
    }
    var msgQueueName = _setMsgQueueName(appName, queueName);
    if (_.isFunction(callback)) {
        // Return an existing msg queue (if one exists)
        return _getMsgQueue(msgQueueName, function (error, msgQueue) {
            if (!error) {
                // Return the existing msg queue
                return callback(undefined, msgQueue);
            }
            // Create and return a new msg queue
            return callback(undefined, new RedisMsgQueue(appName, queueName, redisOptions));
        })
    }
    // Return an existing msg queue (if one exists)
    var msgQueue = _getMsgQueue(msgQueueName);
    if (msgQueue) {
        // Return the existing msg queue
        return msgQueue;
    }
    // Create and return a new msg queue
    return new RedisMsgQueue(appName, queueName, redisOptions);
};
module.exports.createRedisMsgQueue = createRedisMsgQueue;
module.exports.getRedisMsgQueue = createRedisMsgQueue;

var destroyRedisMsgQueue = function (appName, queueName, callback) {
    var msgQueueName = _setMsgQueueName(appName, queueName);
    if (_.isFunction(callback)) {
        // Find an existing msg queue (if one exists)
        return _getMsgQueue(msgQueueName, function (error, msgQueue) {
            if (!error) {
                // Shutdown the existing msg queue
                return async.forEach(_.toArray(_msgWorkerMap), function (_msgWorker, callback) {
                    if (_msgWorker && _msgWorker.msgQueueName === queueName) {
                        _setMsgWorkerStop(_msgWorker.workerName);
                        return async.until(function() {
                            return (_getMsgWorker(_msgWorker.workerName) === undefined);
                        }, function (callback) {
                            return setTimeout(callback, 250);
                        }, function (error) {
                            return callback();
                        });
                    }
                    return callback();
                }, function () {
                    return callback();
                });
            }
            // The msg queue does not exist
            return callback();
        });
    }
    // Find an existing msg queue (if one exists)
    var msgQueue = _getMsgQueue(msgQueueName);
    if (msgQueue) {
        // Shutdown the existing msg queue
        _msgWorkerMap.forEach(function (msgWorker) {
            if (msgWorker && msgWorker.msgQueueName === msgQueueName) {
                _setMsgWorkerStop(msgWorker.workerName);
            }
        });
        return undefined;
    }
    // The msg queue does not exist
    return undefined;
};
module.exports.destroyRedisMsgQueue = destroyRedisMsgQueue;

var RedisMsgQueue = function (appName, queueName, redisOptions) {
    // Set the app and queue names for this instance of the queue
    this.appName = appName;
    this.queueName = queueName;
    this.msgQueueName = _setMsgQueueName(appName, queueName);

    // Set the poll interval, retry delay, and retry limit for this instance of the queue
    this.pollInterval = 1000;
    this.lockTimeout = 90000;
    this.retryDelay = 1.25;
    this.retryLimit = 1;

    // Set the redis options for this instance of the queue
    this.options = {};
    if (_.isObject(redisOptions)) {
        this.options = redisOptions;
    }
    this.host = this.options.host;
    this.port = this.options.port;
    this.passwd = this.options.password;

    // Add the queue to the static msg queue map (to allow clean shutdown)
    if (!_addMsgQueue(this.msgQueueName, this)) {
        console.warn('RedisMsgQueue()  The redis msg queue constructor was unable to add the queue to the msg queue map.');
    }

    // Setup the redis client and prepare the connection
    this.redisClient = redis.createClient(this.port, this.host, this.options);
    if (this.passwd) {
        this.redisClient.auth(this.passwd, function (error, reply) {
            console.warn('RedisMsgQueue()  The redis client was unable to authenticate to the redis server - ' + util.inspect(error) + '.')
        });
    }
};

// ----------------------------------------------------------------------------------------------------
// The msg queue clear method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.clear = function (group, callback) {
    return callback('The redis msg queue has not yet implemented the clear method.');
};

// ----------------------------------------------------------------------------------------------------
// The msg queue enqueue method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.enqueue = function (group, task, callback) {
    // Set the msg group name and the task string to push onto the queue
    var appName = this.appName, queueName = this.queueName, msgQueueName = this.msgQueueName;
    var msgGroupName = _setMsgGroupName(appName, queueName, group);
    var taskJsonString = JSON.stringify(task);

    // Push the task (in json string format) onto the left end of the queue
    var multi = this.redisClient.multi();
    // Push the task onto the right end of the app:queue:group list
    multi.rpush('list_' + msgGroupName, taskJsonString);
    // Increment the number of tasks on the app:queue:group hash
    multi.hincrby('hash_' + msgGroupName, 'tasks', 1);
    // Add the app:queue:group to the app:queue set - this will only
    // add the app:queue:group if it does not already exist in the set
    multi.sadd('set_' + msgQueueName, msgGroupName);
    return multi.exec(function (error, replies) {
        if (error) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - error ' + util.inspect(error) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }

        // Make sure each step was successful
        if (!_.isArray(replies) || replies.length !== 3) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[0]) || replies[0] === 0) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the list reported a size of zero after pushing the task onto the list, reply ' + util.inspect(replies[0]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[1]) || replies[1] <= 0) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the hash reported a zero or negative number of tasks after adding this task to the number, reply ' + util.inspect(replies[1]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[2]) || (replies[2] !== 0 && replies[2] !== 1)) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the set reported adding more or less than zero or one message group to the message groups, reply ' + util.inspect(replies[2]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        // Return success via callback
        return callback(null, task);
    });
};

// ----------------------------------------------------------------------------------------------------
// The msg queue register worker method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.register = function (worker, callback) {
    // Add the worker to the static worker map (to allow clean shutdown)
    worker = _addMsgWorker(this.queueName, worker);
    if (!_.isObject(worker) || !_.isString(worker.workerName) || _.isEmpty(worker.workerName)) {
        console.warn('RedisMsgQueue()  The redis msg queue ' + this.msgQueueName + ' was unable to register the worker ' +
                     util.inspect((worker) ? worker.workerName : worker) + ' with the msg worker map.');
        return callback('The redis msg queue ' + this.queueName + ' was unable to register the worker.');
    }

    // Return success via callback - do not
    // return execution along with the callback!
    callback();

    // Setup a new redis client and prepare the connection - an individual client
    // is required for each worker so that the watch mechanism will function properly
    worker.workerRedis = redis.createClient(this.port, this.host, this.options);
    if (this.passwd) {
        worker.workerRedis.auth(this.passwd, function (error, reply) {
            console.warn('RedisMsgQueue()  The worker redis client was unable to authenticate to the redis server - ' + util.inspect(error) + '.')
        });
    }

    // Start each worker (as a service) allowing
    // each worker to run until the process exits.
    return this._timeoutProcess(worker.workerName, worker.workerRedis, worker.msgWorker, worker.workerMaxTasks, this, this.pollInterval);
};

// ----------------------------------------------------------------------------------------------------
// The msg queue timed task processing (private)
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype._timeoutProcess = function (workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, timeout) {
    if (timeout) {
        return setTimeout(function () {
            return msgQueue._process(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
        }, timeout);
    }
    return process.nextTick(function () {
        return msgQueue._process(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
    });
};

// ----------------------------------------------------------------------------------------------------
// The msg queue task processing (private)
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype._process = function (workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue) {
    // Exit the processing loop if the worker has been told to stop
    if (_getMsgWorkerStop(workerName)) {
        if (!_removeMsgWorker(workerName)) {
            console.warn('RedisMsgQueue._process()  The redis msg worker ' + msgWorkerName +
                         ' was unable to remove itself from the worker map.');
        }
        //console.log('The worker %s is done.', workerName);
        return 'This worker is done.';
    }

    // Pull a random member (app:queue:group) from the app:queue set
    return workerRedis.srandmember('set_' + msgQueue.msgQueueName, function (error, reply) {
        if (error) {
            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                         ' was unable to pull a random member (app:queue:group) from the set ' +
                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
        }

        // If the reply is null or undefined, the set does not exist
        if (_.isUndefined(reply) || _.isNull(reply)) {
            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
        }

        // Setup the watch for the app:queue:group key - this is the 'reply' from the last query
        var randAppQueueGroup = reply;
        return workerRedis.watch('hash_' + randAppQueueGroup, function(error, reply) {
            if (error || reply !== 'OK') {
                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                             ' was unable to watch the app:queue:group hash ' + randAppQueueGroup + ' - reply ' +
                             util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                if (!error) {
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    });
                }
                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
            }

            // Read the number of tasks and the locked datetime in the app:queue:group
            return workerRedis.hmget('hash_' + randAppQueueGroup, 'tasks', 'locked', function (error, replies) {
                if (error || !_.isArray(replies) || replies.length !== 2) {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' number of tasks and the locked datetime in the app:queue:group hash ' + randAppQueueGroup +
                                 ' - replies ' + util.inspect(replies) + ', error ' + util.inspect(error) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    });
                }

                // Run the process loop again if the number of tasks is zero or if the locked datetime is older than the lock timeout
                var tasks = undefined;
                if (_.isNull(replies[0])) {
                    // Do nothing - null values are valid
                } else if (_.isString(replies[0]) && !_.isEmpty(replies[0]) && _.isFinite(Number(replies[0]))) {
                    tasks = Number(replies[0]);
                } else {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' number of tasks in the app:queue:group hash ' + randAppQueueGroup + ' - reply[0] ' + util.inspect(replies) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    });
                }
                var locked = undefined;
                if (_.isNull(replies[1])) {
                    // Do nothing - null values are valid
                } else if (_.isString(replies[1]) && !_.isEmpty(replies[1]) && _.isFinite(Date.parse(replies[1]))) {
                    locked = new Date(Date.parse(replies[1]));
                } else {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' locked datetime in the app:queue:group hash ' + randAppQueueGroup + ' - reply[1] ' + util.inspect(replies) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    });
                }
                if (_.isDate(locked) && (msgQueue.lockTimeout === 0 ||
                    locked.getTime() >= ((new Date()).getTime() - msgQueue.lockTimeout))) {
                    //console.log('Worker ' + workerName + ' - test point - locked');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                    });
                }
                if (_.isUndefined(tasks) || (_.isFinite(tasks) && tasks === 0)) {
                    // console.log('Worker ' + workerName + ' - test point - clean up tasks === 0');
                    // Cleanup the data store if there are no more tasks in the app:queue:group
                    var multi = workerRedis.multi();
                    // Delete the app:queue:group hash
                    multi.del('hash_' + randAppQueueGroup);
                    // Remove the app:queue:group from the set
                    multi.srem('set_' + msgQueue.msgQueueName, randAppQueueGroup);
                    return multi.exec(function (error, replies) {
                        if (error) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - error ' + util.inspect(error) + '.');
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                        }
                        // Check for non-execution due to watched key changing
                        if (_.isNull(replies)) {
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                        }
                        // Make sure each step was successful
                        if (!_.isArray(replies) || replies.length !== 2) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                        }
                        var reply = replies[0];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the delete app:queue:group hash indicated more or less than one delete, reply ' + util.inspect(replies) + '.');
                        }
                        reply = replies[1];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the remove app:queue:group from the set indicated more or less than one remove, reply ' + util.inspect(replies) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                    });
                }

                // console.log('Worker ' + workerName + ' - test point - just before read and pop task');
                // Read and pop the task (in json string format) from the right end of the queue
                var stepCount = 3; // First three steps - lrange, ltrim, and hincrby
                var multi = workerRedis.multi();
                // Read and pop the task or tasks from the left end of the app:queue:group list
                var numTasksReadPop = Math.min(tasks, workerMaxTasks);
                multi.lrange('list_' + randAppQueueGroup, 0, numTasksReadPop - 1);
                multi.ltrim('list_' + randAppQueueGroup, numTasksReadPop, -1);
                // Decrement the number of tasks on the app:queue:group hash
                multi.hincrby('hash_' + randAppQueueGroup, 'tasks', 0 - numTasksReadPop);
                // Set the locked datetime only if the group was specified with the task
                if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName))
                {
                    stepCount += 1; // Lock step (if locking) - hset
                    multi.hset('hash_' + randAppQueueGroup, 'locked', new Date().toISOString());
                }
                return multi.exec(function (error, replies) {
                    if (error) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - error ' + util.inspect(error) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    }
                    // Check for non-execution due to watched key changing
                    if (_.isNull(replies)) {
                        // console.log('Worker ' + workerName + ' - test point - multi returned null multi-bulk due to watch');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                    }
                    // Make sure each step was successful
                    if (!_.isArray(replies) || replies.length !== stepCount) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    }
                    if (!_.isArray(replies[0]) || replies[0].length !== numTasksReadPop) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read ' + numTasksReadPop + 'task from the msg group ' + randAppQueueGroup +
                                     ' - the list returned an invalid set of tasks, reply[0] ' + util.inspect(replies) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue, msgQueue.pollInterval);
                    }
                    var reply = replies[1];
                    if (!_.isString(reply) || reply !== 'OK') {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the list returned an invalid number of tasks, reply[1] ' + util.inspect(replies) + '.');
                    }
                    reply = replies[2];
                    if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                        reply = parseInt(reply);
                    }
                    if (!_.isFinite(reply)) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the hash set tasks returned an invalid number of tasks remaining, reply[2] ' + util.inspect(replies) + '.');
                    }
                    if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName)) {
                        reply = replies[3];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                         ' - the hash key \'locked\' indicated that it was not set, reply[2] ' + util.inspect(replies) + '.');
                        }
                    }

                    // Convert the array of tasks to javascript objects
                    var taskArray = [];
                    _.each(replies[0], function (task) {
                        taskArray.push(JSON.parse(task));
                    }, this);

                    // console.log('Worker ' + workerName + ' - test point - just before execute worker');
                    // Execute the worker with the task
                    var task = (workerMaxTasks === 1) ? taskArray[0] : taskArray;
                    return msgWorker(task, function(error) {
                        if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName)) {
                            return workerRedis.hdel('hash_' + randAppQueueGroup, 'locked', function (error, reply) {
                                if (error) {
                                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                                 ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                                 ' - the hash key \'locked\' was not deleted, error ' + util.inspect(error) + '.');
                                }
                                if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                                 ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                                 ' - the hash key \'locked\' indicated more or less than one remove, reply ' + util.inspect(reply) + '.');
                                }
                                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                            });
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, workerMaxTasks, msgQueue);
                    });
                });
            });
        });
    });
};
