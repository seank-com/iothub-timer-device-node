'use strict';

// When running in production, forever doesn't set the working directory
// correctly so we need to adjust it before trying to load files from the
// requires below. Since we know we are run as the node user in production
// this is an easy way to detect that state.
if (process.env.USER === "node") {
  process.chdir("/var/node");
}

require("dotenv").config();
const Protocol = require("azure-iot-device-mqtt").Mqtt;
const Client = require("azure-iot-device").Client;
const Message = require("azure-iot-device").Message;
const uuid = require("uuid");

var timings = {};

var stats = {
  C2D: { 
    "last": 0,
    "total": 0,
    "count": 0,
    "average": 0 
  },
  D2C: {
    "last": 0,
    "total": 0,
    "count": 0,
    "average": 0 
  },
  RT: {
    "last": 0,
    "total": 0,
    "count": 0,
    "average": 0 
  },
  ACK: {
    "last": 0,
    "total": 0,
    "count": 0,
    "average": 0 
  }
};

var timingInterval = 5*1000; // every 5 seconds
var sendInterval = null;

console.log("Connecting using ", process.env.IOT_DEVICE_CONNECTIONSTRING);

// Create IoT Hub client
var client = Client.fromConnectionString(process.env.IOT_DEVICE_CONNECTIONSTRING, Protocol);

//
// sendTime
//
// sends a D2C message with timestamp
//
// {
//   "type": "time",
//   "time": 
// }
//
function sendTime() {
  var timestamp = new Date(),
    msg = Object.assign({ 
      "type": "time",
      "time": timestamp,
      "deviceId": "hanford-sim"
    }),
    id = uuid.v4(),
    message = new Message(JSON.stringify(msg));

  message.messageId = id;
  timings[id] = timestamp;

  client.sendEvent(message, (err, result) => {
    if (err) {
      console.log("send error:", err);
      process.exit();
    }
    console.log("Sent message");
  });
};

//
// parseStatus
//
// parses the following hub to device message
//
// {
//   "type": "status",
//   "time":
//   "id": 
//   "D2C": {},
//   "ACK": {}
// }
//
function parseStatus(msg) {
  if (!msg.time || !msg.id || !msg.D2C || !msg.ACK || msg.type !== "status") {
    throw new Error("parseStatus error: invalid message format");
    return;
  }

  var now = new Date();
  msg.time = new Date(msg.time);

  // Only consider messages from this run.
  if (timings[msg.id]) {
    stats.C2D.last = now.valueOf() - msg.time.valueOf();
    stats.C2D.total += stats.C2D.last;
    stats.C2D.count += 1;
    stats.C2D.average = stats.C2D.total / stats.C2D.count;
  
    stats.D2C = msg.D2C;
    stats.ACK = msg.ACK;
    stats.D2C.average = stats.D2C.total / stats.D2C.count;
    stats.ACK.average = stats.ACK.total / stats.ACK.count;

    stats.RT.last = now.valueOf() - timings[msg.id].valueOf();
    stats.RT.total += stats.RT.last;
    stats.RT.count += 1;
    stats.RT.average = stats.RT.total / stats.RT.count;

    delete timings[msg.id];
  }
  
  console.log("D2C:%dms C2D:%dms RT:%dms ACK:%dms", stats.D2C.average, stats.C2D.average, stats.RT.average, stats.ACK.average);
};

client.open((err, result) => {
  if (err) {
    console.log("open error:", err);
  } else {

    console.log("Setting reporting interval every %dms", timingInterval);

    sendInterval = setInterval(sendTime, timingInterval);

    client.on("message", (message) => {
      var msg = {
        messageId: message.messageId
      };
      Object.assign(msg, JSON.parse(message.getData()));

      try {
        if (!!msg && !!msg.type) {
          switch(msg.type) {
          case "status":
            parseStatus(msg);
            break;
          default:
            throw new Error("request not recognized");
            break;
          }
        } else {
          throw new Error("json does not contain request");
        }
        client.complete(message, (err) => {
          if (err) {
            console.log("complete error:", err);
          }
        });
      }
      catch (err) {
        console.log('client message error:', err);
        client.reject(message, (err) => {
          if (err) {
            console.log("reject error:", err);
          }
        });
      }
    });

    client.on("error", (err) => {
      console.log("client error:", err);
      if (sendInterval) clearInterval(sendInterval);
      client.close();
    });
  }
});
