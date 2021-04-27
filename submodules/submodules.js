/**
 * This file contains all the submodules used in firebase and firestore APIs
 * Managed by Karanjeet Singh
 */
/* #region  Initialization of all required modules  */

var hasOwnProperty = Object.prototype.hasOwnProperty;
var zlib = require('zlib');
var moment = require('moment');
let constant = require("../config/constant");
var firebase = require("firebase-admin");
const serverName = constant.SERVERNAME;
//AWS
var AWS = require('aws-sdk');
AWS.config.update({ accessKeyId: constant.AWS_ACCESS_KEY, secretAccessKey: constant.AWS_SECRET_KEY });
AWS.config.update({ region: 'eu-west-1' });
var sqs = new AWS.SQS();
/* #endregion  Initialization of all required modules  */

module.exports = {
    /**
     * isEmpty - to check if passed structure is empty or not 
     * returns - true or false depending on value
     */
    isEmpty: function (obj) {
        // null and undefined are "empty"
        if (obj == null)
            return true;

        // if it has a length property with a non-zero value
        if (obj.length > 0)
            return false;
        if (obj.length === 0)
            return true;

        // If it isn't an object at this point
        if (typeof obj !== "object")
            return true;
        // Otherwise, does it have any properties of its own?
        for (var key in obj) {
            if (hasOwnProperty.call(obj, key))
                return false;
        }
        return true;
    },

    /**
     * Data is a collection if
     *  - it has a odd depth
     *  - contains only objects or contains no objects.
     */
    isCollection: function (data, path, depth) {
        if (
            typeof data != 'object' ||
            data == null ||
            data.length === 0 ||
            this.isEmpty(data)
        ) {
            return false;
        }
        for (const key in data) {
            if (typeof data[key] != 'object' || data[key] == null) {
                // If there is at least one non-object item then it data then it cannot be collection.
                return false;
            }
        }
        return true;
    },

    executeDates: function (shared_capacity_id, startDate, endDate) {

        let start = new Date(startDate);
        let end = new Date(endDate);
        var node = "ticket/availabilities/" + shared_capacity_id;
        var updates = {};
        while (start <= end) {

            var date = new Date(start).toISOString();
            var dump = date.split("T");
            start.setDate(start.getDate() + 1);
            updates[dump[0]] = null;
        }
        firebase.database().ref(node).update(updates);
    },

    /*
     * getSpclRef
     * @param {array} headers
     * @returns {spcl_ref}
     * @purpose it is used to find proper spcl ref to be used in graylogs
     */
    getSpclRef: function (headers) {
        var spcl_ref = '';
        if (headers['museum_id'] && headers['user_id']) {
            spcl_ref = headers['museum_id'] + "_" + headers['user_id'];
        } else if (headers['hotel_id'] && headers['user_id']) {
            spcl_ref = headers['hotel_id'] + "_" + headers['user_id'];
        } else if (headers['museum_id'] && headers['ticket_id']) {
            spcl_ref = headers['museum_id'] + "_" + headers['ticket_id'];
        } else if (headers['hotel_id'] && headers['ticket_id']) {
            spcl_ref = headers['hotel_id'] + "_" + headers['ticket_id'];
        } else if (headers['museum_id']) {
            spcl_ref = headers['museum_id'];
        } else if (headers['hotel_id']) {
            spcl_ref = headers['hotel_id'];
        } else if (headers['user_id']) {
            spcl_ref = headers['user_id'];
        } else if (headers['ticket_id']) {
            spcl_ref = headers['ticket_id'];
        }
        return spcl_ref;
    },

    /*
     * writeLog
     * @param {string} url
     * @param {string} api
     * @param {string} ip
     * @param {array} data
     * @param {string} ref
     * @param {string} spcl_ref
     *@purpose it is used to send logs  to the graylogs server
     */
    writeLog: function (url, api, ip, status, data, ref = '', spcl_ref = '') {
        try {
            var logs_data = data;
            var http_request;
            if (spcl_ref) {
                http_request = url + ' - ' + spcl_ref;
            } else {
                http_request = url;
            }
            var msg = [{
                "source_name": "firebase",
                "source_ip": ip,
                "source": url,
                "request_reference": ref,
                "short_message": api,
                "full_message": JSON.stringify(data),
                "created_datetime": moment().format('YYYY-MM-DD:hh:mm:ss:SSS'),
                "server": serverName,
                "host_name": "firebase.prioticket.com",
                "http_status": status,
                "http_request": http_request,
                "processing_time": 0,
                "http_method": 'POST'
            }];

            var graylogs = { 'LOGS_ENABLED': 1 };
            var logs_enable = firebase.database().ref("graylogs/");
            logs_enable.once('value').then(function (graylogs_val) {
                if (graylogs_val.val() != null) {
                    graylogs = graylogs_val.val();
                }
                if (graylogs['LOGS_ENABLED'] != 0) {
                    if (graylogs["LOGS_" + api] != 0) {
                        zlib.deflate(JSON.stringify(msg), (err, buffer) => {
                            if (!err) {
                                var compressedString = buffer.toString('base64');
                                var sqsParams = {
                                    MessageBody: compressedString,
                                    QueueUrl: graylogs['LOGS_QUEUE']
                                };
                                if (Buffer.byteLength(compressedString) <= 256000) { //250 KB allowed
                                    sqs.sendMessage(sqsParams, function (sendmsgerr, sndmsgdata) {
                                        if (sendmsgerr) {
                                            console.log('ERR', sendmsgerr);
                                            return true;
                                        }
                                        if (sndmsgdata.MessageId) {
                                            var params = {
                                                Message: graylogs['LOGS_QUEUE'], // required 
                                                TopicArn: graylogs['LOGS_SNS']
                                            };

                                            //Create promise and SNS service object
                                            var publishTextPromise = new AWS.SNS().publish(params).promise();

                                            // Handle promise's fulfilled/rejected states
                                            publishTextPromise.then(
                                                function (prmsdata) {
                                                }).catch(
                                                    function (promiseerr) {
                                                        console.error(promiseerr, promiseerr.stack);
                                                    });
                                        }
                                    });
                                } else {
                                    delete logs_data['response'];
                                    logs_data['response'] = "response size is greater than 250 KB, so removing that response from logs";
                                    msg[0]['full_message'] = JSON.stringify(logs_data);
                                    zlib.deflate(JSON.stringify(msg), (decrypterr, shorten_msgbuffer) => {
                                        if (!decrypterr) {
                                            compressedString = shorten_msgbuffer.toString('base64');
                                            sqsParams = {
                                                MessageBody: compressedString,
                                                QueueUrl: graylogs['LOGS_QUEUE']
                                            };
                                            sqs.sendMessage(sqsParams, function (queueerr, queuedata) {
                                                if (queueerr) {
                                                    console.log('ERR', queueerr);
                                                    return true;
                                                }
                                                if (queuedata.MessageId) {
                                                    var params = {
                                                        Message: graylogs['LOGS_QUEUE'], // required 
                                                        TopicArn: graylogs['LOGS_SNS']
                                                    };

                                                    //Create promise and SNS service object
                                                    var publishTextPromise = new AWS.SNS().publish(params).promise();

                                                    // Handle promise's fulfilled/rejected states
                                                    publishTextPromise.then(
                                                        function (prmsdata) {
                                                        }).catch(
                                                            function (prmserr) {
                                                                console.error(prmserr, prmserr.stack);
                                                            });
                                                }
                                            });
                                        } else {
                                            // handle error
                                            console.log(decrypterr);
                                        }
                                    });
                                }
                            } else {
                                // handle error
                                console.log(err);
                            }
                        });
                    }
                    return true;
                }
            })


        } catch (e) {
            console.log(e);
        }
    }
}