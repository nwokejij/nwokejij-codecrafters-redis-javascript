const { parseRequest } = "./utils.js";
const net = require("net");
const portIndex = process.argv.indexOf("--port");
const isSlave = process.argv.indexOf("--replicaof");
const PORT = portIndex != -1 ? process.argv[portIndex + 1] : 6379;
const replicas = [];
let masterPort = 0;

if (isSlave != -1) {
    masterPort = process.argv[isSlave + 1];
    masterPort = masterPort.split("localhost ")[1];
} else {
    masterPort = PORT;
}

const replicaDict = {};
let buffer = '';

const client = net.createConnection({ port: masterPort, host: 'localhost' }, () => {
    console.log('Connected to master');
    client.write("*1\r\n" + getBulkString("PING"));
});

client.on('data', async (data) => {
    let commands = Buffer.from(data).toString().split("\r\n");
    console.log(`Command received by replica:`, commands);
    // for (const request of requests){
    //     console.log("Request:" + request);
    //     if (request.startsWith('*')){
    //     const parsedRequest = parseRequest(request);
    //     const command = parsedRequest[0];
    //     const args = parsedRequest.slice(1);
    //     if (command.toLowerCase() === 'replconf' && args[0] === 'GETACK') {
    //         client.write("*3/r/n" + getBulkString("REPLCONF") + getBulkString("ACK")+ getBulkString("0"));
    //         continue
    //       }
    //     } else if (request.indexOf("FULLRESYNC") != -1){
    //             client.write("*3/r/n" + getBulkString("REPLCONF") + getBulkString("ACK")+ getBulkString("0"));
    //         }
    //     }
    buffer = data.toString('utf8');
    let messages = buffer.split('\r\n');
    let resData = buffer; // will use to handle the handshake responses
    buffer = messages.pop(); // resets the buffer with ""
    messages.forEach((message) => {
        console.log(`Received message: ${message.trim()}`);
        if (message.startsWith('> REPLCONF GETACK')) {
            console.log('Received REPLCONF GETACK');
            // Handle REPLCONF GETACK message
            // Never reaches this block
        }
    });

    if (resData) {
        const resp = resData.split('\r\n')[0];
        console.log('Parsed response:', resp);

        if (resp === "+PONG") {
            client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("listening-port") + getBulkString(PORT));
            client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("capa") + getBulkString("psync2")); 
        } else if (resp === "+OK") {
            client.write("*3\r\n" + getBulkString("PSYNC") + getBulkString("?") + getBulkString("-1")); 
        }
    }
    console.log("Requests array:" + requests);
    console.log("End of data processing block");
});

client.on('end', () => {
    console.log('Disconnected from master');
});

client.on('error', (err) => {
    if (err.code === 'EPIPE') {
        console.error('EPIPE error: attempting to write to a closed stream');
    } else {
        console.error('Connection error:', err);
    }
});



function parseEvents(events) {
    let stEvent = null
    const parsedEvents = []
    while (stEvent !== -1) {
      const nxtStEvent = events.indexOf('*', stEvent + 1)
      const l = stEvent === null ? 0 : stEvent
      const r = nxtStEvent === -1 ? events.length : nxtStEvent
      parsedEvents.push(events.substring(l, r))
      stEvent = nxtStEvent
    }
  
    return parsedEvents
  }




// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");


const server = net.createServer((connection) => {
  // Handle connection
    connection.on('data', (data) => {
        const command = data.toString();
        console.log("Data Received To Master: " + command);
        const message = parseRedisResponse(command);
        connection.write(message);
        if (command.indexOf("PSYNC") != -1){
            const hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            const buffer = Buffer.from(hex, 'hex');

// Calculate the length of the file in bytes
const bytes = buffer.length;

// Create the RDB file header with the length of the file
let rdbFileHeader = `$${bytes}\r\n`;

// Combine the header and the buffer into a single buffer
const rdbFileBuffer = Buffer.concat([Buffer.from(rdbFileHeader, 'ascii'), buffer]);
            connection.write(rdbFileBuffer);
            replicas.push(connection);
        }
        if (command.indexOf("SET") != -1){
            replicas.forEach((replica) => {
                replica.write(command);
            })
        }
        
        
    })
    connection.write("Hello");


});





const dictionary = {};
function parseRedisResponse(data) {
    const type = data.charAt(0);
    const content = data.slice(1).trim();

    switch (type) {
        case '+': // Simple string
            return content;
        case '-': // Error
            return new Error(content);
        case ':': // Integer
            return parseInt(content, 10);
        case '$': // Bulk string
            const length = parseInt(content, 10);
            // console.log("Length: " + length);
            return data.slice(data.indexOf('\r\n') + 2, data.indexOf('\r\n') + 2 + length);
        case '*': // Array
            delimiter = data.indexOf('\r\n');
            bulkStrings = data.slice(delimiter+2); 
            stringArray = bulkStrings.split('\r\n');
            stringArrayLen = stringArray.length;
            noNewLine = [];
            
            for (let i = 0; i < stringArrayLen; i++){
                if (stringArray[i] == "INFO"){
                    if (isSlave != -1){
                        return getBulkString("role:slave");
                    }
                    return getBulkString("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0");
                } else if (stringArray[i] == "REPLCONF"){
                    return "+OK\r\n";
                } else if (stringArray[i] == "PSYNC"){
                    return "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
                }
                else if (stringArray[i] == "ECHO"){
                    noNewLine.pop();
                    continue;
                } else if (stringArray[i] == "PING"){
                    return "+PONG\r\n";
                } else if (stringArray[i] == "SET"){
                    dictionary[stringArray[i+2]] = stringArray[i + 4];
                    if (i + 6 < stringArrayLen){
                        if (stringArray[i+6] == "px"){
                            setTimeout(() => {
                                delete dictionary[stringArray[i + 2]];
                                }, parseInt(stringArray[i + 8])
                            )
                        }
                    }
                    return "+OK\r\n";
                }else if (stringArray[i] == "GET"){
                    if (!(stringArray[i+2] in dictionary) && !(stringArray[i+2] in replicaDict)) {
                        console.log("Should not see this");
                        return getBulkString(null);
                    } else if (stringArray[i +2] in replicaDict){
                        return getBulkString(replicaDict[stringArray[i+2]]);
                    }
                    return getBulkString(dictionary[stringArray[i+2]]);
                } else {
                    noNewLine.push(stringArray[i]);
                }
                }
            strings = noNewLine.join("\r\n");
            // console.log(strings);
            return strings;
        default:
            throw new Error('Invalid Redis response');
    }
}

function getBulkString(string){
    if (string == null){
        return "$-1\r\n"
    }

    return `\$${string.length}\r\n${string}\r\n`
}
function parseRedisResponseFromMaster(data, replicaDict){
    const type = data.charAt(0);
    switch(type) {
        case '+':
            break;
        case '*': // Array
        console.log("Data recieved" + data);
            delimiter = data.indexOf('\r\n');
            bulkStrings = data.slice(delimiter+2); 
            stringArray = bulkStrings.split('\r\n');
            stringArrayLen = stringArray.length;
            noNewLine = [];
            if ((stringArray.indexOf("REPLCONF") != -1) && (stringArray.indexOf("GETACK") != -1) && (stringArray.indexOf("*") != -1)){
                return "*3/r/n" + getBulkString("REPLCONF") + getBulkString("ACK")+ getBulkString("0");
            }
            for (let i = 0; i < stringArrayLen; i++){
                if (stringArray[i] == "SET"){
                    replicaDict[stringArray[i+2]] = stringArray[i + 4];
                    if (i + 6 < stringArrayLen){
                        if (stringArray[i+6] == "px"){
                            setTimeout(() => {
                                delete replicaDict[stringArray[i + 2]];
                                }, parseInt(stringArray[i + 8])
                            )
                        }
                    }
                } else {
                    noNewLine.push(stringArray[i]);
                }
                }
    }


}

server.listen(PORT, "127.0.0.2");

// const net = require("net");
// const PORT = process.argv[2] === "--port" ? process.argv[3] : 6379;

// let server_info = {
//   role: "master",
//   master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
//   master_repl_offset: "0",
// };

// const replicaList = [];

// let empty_rdb =
//   "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

// const handleHandshake = (host, port) => {
//   const hsclient = net.createConnection({ host: host, port: port }, () => {
//     console.log("connected to master", "Host: ", host, "Port: ", port);
//     hsclient.write("*1\r\n$4\r\nPING\r\n");

//     let repl1 = false;

//     hsclient.on("data", (data) => {
//       let commands = Buffer.from(data).toString().split("\r\n");
//       console.log(`Command recieved by replica:`, commands);
//       let queries = data.toString();
//       while (queries.length > 0) {
//         let index = queries.indexOf("*", 1);
//         let query;
//         if (index == -1) {
//           query = queries;
//           queries = "";
//         } else {
//           query = queries.substring(0, index);
//           queries = queries.substring(index);
//         }

//         commands = Buffer.from(query).toString().split("\r\n");

//         if (commands[0] == "+PONG") {
//           hsclient.write(
//             `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${PORT}\r\n`
//           );
//         } else if (commands[0] == "+OK") {
//           if (repl1 == false) {
//             hsclient.write(
//               `*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`
//             );
//             repl1 = true;
//           } else hsclient.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
//         } else if (commands[2] == "SET") {
//           const key = commands[4];
//           const value = commands[6];
//           db[key] = value;

//           if (commands[8] == "px")
//             setTimeout(() => {
//               delete db[key];
//             }, commands[10]);
//         } else if (commands[2] == "GET") {
//           const answer = db[commands[4]];
//           if (answer) {
//             const l = answer.length;
//             hsclient.write("$" + l + "\r\n" + answer + "\r\n");
//           } else {
//             hsclient.write("$-1\r\n");
//           }
//         } else if (commands[2] == "REPLCONF") {
//           if (commands[4] == "GETACK") {
//             return hsclient.write(
//               `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n`
//             );
//           }
//         }
//       }
//     });
//   });
// };

// const propagateToReplicas = (command) => {
//   if (server_info.role != "master" || replicaList.length == 0) return;

//   for (const replicaCon of replicaList) {
//     replicaCon.write(command);
//   }
// };
// if (process.argv[4] == "--replicaof") {
//   server_info.role = "slave";
//   let replicaofArray = process.argv[5].split(" ");
//   let masterhost = replicaofArray[0];
//   let masterport = replicaofArray[1];

//   if (masterhost && masterport) {
//     handleHandshake(masterhost, masterport);
//   }
// }

// const db = {};
// // You can use print statements as follows for debugging, they'll be visible when running tests.
// console.log("Logs from your program will appear here!");

// // Uncomment this block to pass the first stage
// const server = net.createServer((connection) => {
//   // Handle connection

//   connection.on("data", (data) => {
//     const commands = Buffer.from(data).toString().split("\r\n");
//     // *2\r\n $5 \r\n ECHO \r\n $3 \r\n hey \r\n
//     console.log(`Command:`, commands);
//     if (commands.includes("PING")) {
//       return connection.write("+PONG\r\n");
//     } else if (commands[2] == "ECHO") {
//       const str = commands[4];
//       const l = str.length;
//       return connection.write("$" + l + "\r\n" + str + "\r\n");
//     } else if (commands[2] == "SET") {
//       const key = commands[4];
//       const value = commands[6];
//       db[key] = value;

//       if (commands[8] == "px")
//         setTimeout(() => {
//           delete db[key];
//         }, commands[10]);

//       connection.write("+OK\r\n");
//       propagateToReplicas(data);
//     } else if (commands[2] == "GET") {
//       const answer = db[commands[4]];
//       if (answer) {
//         const l = answer.length;
//         return connection.write("$" + l + "\r\n" + answer + "\r\n");
//       } else {
//         return connection.write("$-1\r\n");
//       }
//     } else if (commands[2] == "INFO") {
//       let response = "";
//       for (let key in server_info) {
//         response += `${key}:${server_info[key]},`;
//       }

//       return connection.write(
//         `$` + `${response.length}\r\n` + response + `\r\n`
//       );
//     } else if (commands[2] == "REPLCONF") {
//       if (commands.includes("listening-port")) {
//         return connection.write(`+OK\r\n`);
//       } else {
//         return connection.write(`+OK\r\n`);
//       }
//     } else if (commands[2] == "PSYNC") {
//       if (commands[4] == "?" && commands[6] == "-1") {
//         connection.write(`+FULLRESYNC ${server_info.master_replid} 0\r\n`);

//         //Empty RBD send to replica
//         const bufferRDB = Buffer.from(empty_rdb, "base64");
//         const res = Buffer.concat([
//           Buffer.from(`$${bufferRDB.length}\r\n`),
//           bufferRDB,
//         ]);
//         console.log(res);
//         connection.write(res);
//         replicaList.push(connection);
//       }
//     }
//   });
// });

// server.listen(PORT, "127.0.0.1");