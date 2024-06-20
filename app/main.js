const net = require("net");
const portIndex = process.argv.indexOf("--port");
const isSlave = process.argv.indexOf("--replicaof");
const PORT = portIndex != -1 ? process.argv[portIndex + 1] : 6379;
const replicas = [];
let masterPort = 0;
const replicaDict = {};
if (isSlave != -1){
    masterPort = process.argv[isSlave + 1];
    masterPort = masterPort.split("localhost ")[1];
} else {
    masterPort = PORT;
}

 const client = net.createConnection({ port: masterPort, host: 'localhost'}, () => {
        client.write("*1\r\n" + getBulkString("PING"));
        client.on('data', (data) => {
            // if (resData){
            //     // const resp = resData.split('\r\n')[0];
            //     // if (resp === "+PONG"){
            //     //     client.write("*3\r\n"+ getBulkString("REPLCONF") + getBulkString("listening-port") + getBulkString(PORT));
            //     //     client.write("*3\r\n"+ getBulkString("REPLCONF") + getBulkString("capa") + getBulkString("psync2")); 
            //     // } else if (resp == "+OK"){
            //     //     client.write("*3\r\n" + getBulkString("PSYNC") + getBulkString("?")+ getBulkString("-1"));
            //     // }

            // }
            const command = data.toString();
            parseRedisResponse(command);
            
        });
        
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
    replicas.push(client);






// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");
// const replica = net.createServer((connection) => {

//     connection.on('data', (data) => {
//         // const command = data.toString();
//         // parseRedisResponseFromMaster(command);
//         connection.write(data);
//     })
// });

// const connectToMaster = (port) => {
//     const client = new net.Socket();
//     client.connect(port, 'localhost', () => {
//       console.log(`Replica connected to master on port ${port}`);
//       replicas.push(client);
//     });
  
//     client.on('data', (data) => {
//       console.log(`Replica received from master: ${data}`);
//       // Handle the received command
//       const command = data.toString();
//       parseRedisResponseFromMaster(command);
//     });
  
//     client.on('close', () => {
//       console.log('Connection to master closed');
//     });
//   };
  
const server = net.createServer((connection) => {
  // Handle connection
    connection.on('data', (data) => {
        const command = data.toString();
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
        }
        if (command.indexOf("SET") != -1){
            replicas.forEach((replica) => {
                console.log("# of Replics: " + replicas.length);
                console.log("Data from client to be sent to Replica:" + data);
                connection.write(data);
            })
        }
        
    })

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
                    // create a replica based off of the port
                    //store the replica in a list
                    // for set commands, pass the message to the replica
                    if (stringArray[i + 2] == "listening-port"){
                        
                    }
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
                    if (!(stringArray[i+2] in dictionary)) {
                        console.log("Should not see this");
                        return getBulkString(null);
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
function parseRedisResponseFromMaster(data){
    const type = data.charAt(0);
    const content = data.slice(1).trim();
    console.log("Data received:" + data);
    console.log("Type:" + type);
    switch(type){
        case '*':
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
                    // create a replica based off of the port
                    //store the replica in a list
                    // for set commands, pass the message to the replica
                    if (stringArray[i + 2] == "listening-port"){
                        
                    }
                    break;
                } else if (stringArray[i] == "PSYNC"){
                    return "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
                }
                else if (stringArray[i] == "ECHO"){
                    noNewLine.pop();
                    continue;
                } else if (stringArray[i] == "PING"){
                    return "+PONG\r\n";
                } else if (stringArray[i] == "SET"){
                    console.log("Have we even entered here?");
                    replicaDict[stringArray[i+2]] = stringArray[i + 4];
                    console.log("ReplicaDict: " + replicaDict);
                    if (i + 6 < stringArrayLen){
                        if (stringArray[i+6] == "px"){
                            setTimeout(() => {
                                delete dictionary[stringArray[i + 2]];
                                }, parseInt(stringArray[i + 8])
                            )
                        }
                    }
                    return replicaDict;
                }else if (stringArray[i] == "GET"){
                    if (!(stringArray[i+2] in dictionary)) {
                        console.log("Should not see this");
                        return getBulkString(null);
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

server.listen(PORT, "127.0.0.1");

