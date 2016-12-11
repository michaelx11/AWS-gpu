'use strict';

const AWS = require('aws-sdk');

const SQS = new AWS.SQS({ apiVersion: '2012-11-05' });
const OpsWorks = new AWS.OpsWorks({ apiVersion: '2013-02-18' });
const Lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });


// The SQS queue we receive messages from
const RX_QUEUE_URL = process.env.rxQueueUrl;
// The SQS queue we append outgoing messages to
const TX_QUEUE_URL = process.env.txQueueUrl;

const PROCESS_MESSAGE = 'process-message';

// Messages are JSON

/*
 * RX Message format
 * {
 *   botDataUrl: String (s3 bucket url),
 *   userId: String (universal user ID),
 * }
 */

// TX Message format is identical to RX - this lambda function acts as a filter
// to boot the worker instances if necessary

function invokePoller(functionName, message) {
    const payload = {
        operation: PROCESS_MESSAGE,
        message,
    };
    const params = {
        FunctionName: functionName,
        InvocationType: 'Event',
        Payload: new Buffer(JSON.stringify(payload)),
    };
    return new Promise((resolve, reject) => {
        Lambda.invoke(params, (err) => (err ? reject(err) : resolve()));
    });
}

/*
 * Boots the appropriate worker instance for given config.
 * 
 * TODO: this lambda callback should keep state to dynamically
 * determine how many GPU instances to boot.
 */
function bootWorkerInstance(config) {
}

// TODO: Should probably use sendMessageBatch as an optimization
function processMessage(message, callback) {
    console.log(message);

    // forward message to job SQS queue
    const txQueueParams = {
        QueueUrl: TX_QUEUE_URL,
        MessageBody: message.MessageBody
    };
    // enqueue message on TX SQS queue
    SQS.sendMessage(txQueueParams, function(err, data) {
      if (err) {
        console.log(err, err.stack);
        return;
      }
      console.log(data);
      // delete message if we've successfully forwarded it
      const rxQueueParams = {
          QueueUrl: RX_QUEUE_URL,
          ReceiptHandle: message.ReceiptHandle,
      };
      SQS.deleteMessage(params, (err) => callback(err, message));
    });
}

function poll(functionName, callback) {
    const params = {
        QueueUrl: RX_QUEUE_URL,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 30,
    };
    // batch request messages
    SQS.receiveMessage(params, (err, data) => {
        if (err) {
            return callback(err);
        }
        // for each message, reinvoke the function
        const promises = data.Messages.map((message) => invokePoller(functionName, message));
        // complete when all invocations have been made
        Promise.all(promises).then(() => {
            const result = `Messages received: ${data.Messages.length}`;
            console.log(result);
            callback(null, result);
        });
    });
}

exports.handler = (event, context, callback) => {
    try {
        if (event.operation === PROCESS_MESSAGE) {
            // invoked by poller
            processMessage(event.message, callback);
        } else {
            // invoked by schedule
            poll(context.functionName, callback);
        }
    } catch (err) {
        callback(err);
    }
};

