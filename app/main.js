const net = require("net");
const portIndex = process.argv.indexOf("--port");
const isSlave = process.argv.indexOf("--replicaof");
const PORT = portIndex != -1 ? process.argv[portIndex + 1] : 6379;
console.log("port:" + PORT);
if (isSlave != -1){
    console.log("Called: once");
    masterPort = process.argv[isSlave + 1];
    masterPort = masterPort.split("localhost ")[1];
    const client = net.createConnection({ port: masterPort, host: 'localhost'}, () => {
        client.write("*1\r\n" + getBulkString("PING"));
        client.on('data', (data) => {
            setTimeout(() => {
                client.write("*3\r\n" + getBulkString("PSYNC") + getBulkString("?")+ getBulkString("-1"));
            }, 2);
            client.write("*3\r\n"+ getBulkString("REPLCONF") + getBulkString("listening-port") + getBulkString(PORT));
            client.write("*3\r\n"+ getBulkString("REPLCONF") + getBulkString("capa") + getBulkString("psync2")); 
            
            
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
    // client.end();
}
// function firstAsyncOperation(client) {
//     return new Promise((resolve) => {
        
//         resolve();
//     });
// }

// function secondAsyncOperation(client) {
//     return new Promise((resolve) => {
        
//         resolve();
//     });
// }

// async function executeOperations(client) {
//     await firstAsyncOperation(client);
//     console.log('First operation done, moving to second');
//     await secondAsyncOperation(client);
//     console.log('Second operation done');
// }




// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  // Handle connection
    connection.on('data', (data) => {
        const command = data.toString();
        const message = parseRedisResponse(command);
        connection.write(message);
    })

});
// server.on('error', (err) => {
//     console.error("Already Used Port");
//     server.listen(PORT + 1, "127.0.0.1");
// })



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
                } else if (stringArray[i] == "REPL"){
                    return "+0K\r\n";
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
function getBulkString(string){
    if (string == null){
        return "$-1\r\n"
    }

    return `\$${string.length}\r\n${string}\r\n`
}

server.listen(PORT, "127.0.0.1");

