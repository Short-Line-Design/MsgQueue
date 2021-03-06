/*
 * The Message Queue - node.js example application
 *
 * Copyright (c) 2012 Short Line Design Inc.
 * Copyright (c) 2012 Dan Prescott <danpres14@gmail.com>
 * The MIT License (MIT)
 *
 */

var _ = require('underscore'),
    async = require('async'),
    util = require('util'),

    msgQueue = require('../index');

// Setup the example app, queues, and groups
var app = 'example', unqueue = null, queue = null
  , queues = [ { name: 'first' }, { name: 'second' } ]
  , groups = [ { name: 'alpha' }, { name: 'bravo' }, { name: 'charlie' } ]

// ----------------------------------------------------------------------------------------------------
// Setup the ungrouped message queue
// ----------------------------------------------------------------------------------------------------
console.log('Setup the ungrouped message queue.');
msgQueue.createRedisMsgQueue(app, 'unqueue', function (error, myQueue) {
    if (error) {
        console.warn('The example code was unable to create the ungrouped message queue - ' + util.inspect(error) + '.');
    }
    // Assign the returned queue to the example ungrouped message queue
    unqueue = { msgQueue: myQueue };

    // Register the message queue workers
    for (var i = 0; i < 2; i++) {
        unqueue.msgQueue.register(function (myTask, callback) {
            console.log('One of five ungrouped workers - processing task ' + util.inspect(myTask) + '.');

            // Assume that some work is being done at this time
            return setTimeout(function (myTask, callback) {
                console.log('One of five ungrouped workers - processing task ' + util.inspect(myTask) + '.');
                if (myTask.number) {
                    // Add a new tasks to the ungrouped message queue from within the worker
                    var task = { name:'un-worker-task ' + myTask.number };
                    unqueue.msgQueue.enqueue(undefined, task, function (error, myTask) {
                        if (error) {
                            console.warn('The example was unable to add the worker task ' + util.inspect(task) +
                                         ' to the ungrouped message queue - ' + util.inspect(myTask) +
                                         ', ' + util.inspect(error) + '.');
                        }
                    });
                }
                return callback();
            }, 2500, myTask, callback);

        }, function (error, worker) {
            if (error) {
                console.warn('The example code was unable to register the ungrouped message queue worker ' + i +
                             ' - ' + util.inspect(error) + '.');
            }
            console.log('Registered ungrouped message queue worker ' + i + '.');
        });
    }

    // Add a few tasks to the example ungrouped message queue
    console.log('Adding a few tasks to the ungrouped message queue.');
    for (var j = 0; j < 50; j++) {
        var task = { name:'un-task ' + j, number: j };
        unqueue.msgQueue.enqueue(undefined, task, function (error, myTask) {
            if (error) {
                console.warn('The example was unable to add the task ' + util.inspect(task) +
                             ' to the ungrouped message queue - ' + util.inspect(myTask) +
                             ', ' + util.inspect(error) + '.');
            }
        });
    }
});

// ----------------------------------------------------------------------------------------------------
// Setup the abc123 grouped message queue
// ----------------------------------------------------------------------------------------------------
console.log('Setup the abc123 grouped message queue.');
msgQueue.createRedisMsgQueue(app, 'abc123', function (error, myQueue) {
    if (error) {
        console.warn('The example code was unable to create the abc123 grouped message queue - ' + util.inspect(error) + '.');
    }
    // Assign the returned queue to the example grouped message queue
    queue = myQueue;

    // Register the abc123 message queue worker
    queue.register(function (myTask, callback) {
        console.log('The abc123 grouped worker - processing task ' + util.inspect(myTask) + '.');
        if (myTask.number && myTask.number < 5) {
            // Add a new tasks to the abc123 grouped message queue from within the worker
            var task = { name:'abc123-worker-task ' + (myTask.number + 1), number: (myTask.number + 1) };
            queue.enqueue('xyz789', task, function (error, myTask) {
                if (error) {
                    return console.warn('The example was unable to add the worker task ' + util.inspect(task) +
                                        ' to the abc123 grouped message queue - ' + util.inspect(myTask) +
                                        ', ' + util.inspect(error) + '.');
                }
                return console.log('The abc123 grouped worker added task ' + util.inspect(myTask) + ' to the abc123 grouped message queue.');
            });
        }

        // Assume that some work is being done at this time
        return setTimeout(function (myTask, callback) {
            console.log('The abc123 grouped worker - completed processing task ' + util.inspect(myTask) + '.');
            if (myTask.number && myTask.number < 5) {
                // Add a new tasks to the abc123 grouped message queue from within the worker
                var task = { name:'abc123-worker-task ' + (myTask.number + 101), number: (myTask.number + 101) };
                queue.enqueue('xyz789', task, function (error, myTask) {
                    if (error) {
                        return console.warn('The example was unable to add the worker task ' + util.inspect(task) +
                                            ' to the abc123 grouped message queue - ' + util.inspect(myTask) +
                                            ', ' + util.inspect(error) + '.');
                    }
                    return console.log('The abc123 grouped worker added task ' + util.inspect(myTask) + ' to the abc123 grouped message queue.');
                });
            } else if (myTask.number && myTask.number >= 100 && myTask.number < 105) {
                // Add a new tasks to the abc123 grouped message queue from within the worker
                var task = { name:'abc123-worker-task ' + (myTask.number + 1001), number: (myTask.number + 1001) };
                queue.enqueue('xyz789', task, function (error, myTask) {
                    if (error) {
                        return console.warn('The example was unable to add the worker task ' + util.inspect(task) +
                                            ' to the abc123 grouped message queue - ' + util.inspect(myTask) +
                                            ', ' + util.inspect(error) + '.');
                    }
                    return console.log('The abc123 grouped worker added task ' + util.inspect(myTask) + ' to the abc123 grouped message queue.');
                });
            }
            return callback();
        }, 250, myTask, callback);
    }, function (error, worker) {
        if (error) {
            return console.warn('The example code was unable to register the abc123 grouped message queue worker ' +
                                ' - ' + util.inspect(error) + '.');
        }
        return console.log('Registered abc123 grouped message queue worker.');
    });

    // Add a single task to the example abc123 grouped message queue
    console.log('Adding a single task to the abc123 grouped message queue.');
    var task = { name:'abc123-task 1', number: 1 };
    return myQueue.enqueue('xyz789', task, function (error, myTask) {
        if (error) {
            return console.warn('The example was unable to add the task ' + util.inspect(task) +
                                ' to the abc123 grouped message queue - ' + util.inspect(myTask) +
                                ', ' + util.inspect(error) + '.');
        }
        return console.log('Added task ' + util.inspect(myTask) + ' to the abc123 grouped message queue.');
    });
});

// ----------------------------------------------------------------------------------------------------
// Setup the grouped message queues
// ----------------------------------------------------------------------------------------------------
console.log('Setup each grouped message queue.');
queues.forEach(function (queue) {
    queue.msgQueue = msgQueue.createRedisMsgQueue(app, queue.name);

    // Register the message queue workers
    for (var i = 0; i < 2; i++) {
        queue.msgQueue.register(function (myTask, callback) {
            console.log('One of five grouped workers for ' + queue.name  + ' - processing task ' + util.inspect(myTask) + '.');

            // Assume that some work is being done at this time
            return setTimeout(function (myTask, callback) {
                console.log('One of five grouped workers for ' + queue.name  + ' - processing task ' + util.inspect(myTask) + '.');
                if (myTask.number) {
                    // Add a new tasks to the ungrouped message queue from within the worker
                    var task = { name:'worker-task ' + myTask.number };
                    queue.msgQueue.enqueue(undefined, task, function (error, myTask) {
                        if (error) {
                            console.warn('The example was unable to add the worker task ' + util.inspect(task) +
                                         ' to the ungrouped message queue - ' + util.inspect(myTask) +
                                         ', ' + util.inspect(error) + '.');
                        }
                    });
                }
                return callback();
            }, 500, myTask, callback);

        }, function (error, worker) {
            if (error) {
                console.warn('The example code was unable to register the grouped message queue worker ' + i +
                             ' - ' + util.inspect(error) + '.');
            }
            console.log('Registered ' + queue.name + ' grouped message queue worker ' + i + '.');
        });
    }

    // Add a few tasks to the each grouped message queue
    console.log('Adding a few tasks to the grouped message queue.');
    groups.forEach(function (group) {
        for (var j = 0; j < 25; j++) {
            var task = { name:'task ' + j, number: j, queue: queue.name, group: group };
            queue.msgQueue.enqueue(group, task, function (error, myTask) {
                if (error) {
                    console.warn('The example was unable to add the task ' + util.inspect(task) +
                                 ' to the grouped message queue - ' + util.inspect(myTask) +
                                 ', ' + util.inspect(error) + '.');
                }
            });
        }
    });
});

// ----------------------------------------------------------------------------------------------------
// Shutdown some of the message queues
// ----------------------------------------------------------------------------------------------------
process.on('SIGINT', function () {
    // Shutdown the message queues
    async.parallel([
        function (callback) {
            return msgQueue.destroyRedisMsgQueue(app, 'unqueue', callback);
        },
        function (callback) {
            return msgQueue.destroyRedisMsgQueue(app, 'abc123', callback);
        }
    ], function (error) {
        // Exit the process
        process.exit();
    });
});

process.on('exit', function () {
    console.log('The example is now exiting.');
});
