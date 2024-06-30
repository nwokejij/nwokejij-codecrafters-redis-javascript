const { parseRequest } = "./utils.js";


const replicaDict = {};
let buffer = '';
const handleHandshake = (port) => {
const client = net.createConnection({ port: port, host: 'localhost' }, () => {
    console.log('Connected to master');
    client.write("*1\r\n" + getBulkString("PING"));
    client.on('data', (data) => {
        let commands = Buffer.from(data).toString().split("\r\n");
        console.log(`Command received by replica:`, commands);
        let queries = data.toString();
        print("Raw queries:" + queries);
      while (queries.length > 0) {
        let index = queries.indexOf("*", 1);
        let query;
        if (index == -1) {
          query = queries;
          queries = "";
        } else {
          query = queries.substring(0, index);
          console.log("query:" + query);
          queries = queries.substring(index);
          console.log("queries:" + queries);
        }
    }
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
        console.log("End of data processing block");
    });
    // client.on('end', () => {
    //     console.log('Disconnected from master');
    // });
    
    // client.on('error', (err) => {
    //     if (err.code === 'EPIPE') {
    //         console.error('EPIPE error: attempting to write to a closed stream');
    //     } else {
    //         console.error('Connection error:', err);
    //     }
    // });
});
}

const net = require("net");
const portIndex = process.argv.indexOf("--port");
const isSlave = process.argv.indexOf("--replicaof");
const PORT = portIndex != -1 ? process.argv[portIndex + 1] : 6379;
const replicas = [];
let masterPort = 0;

if (isSlave != -1) {
    masterPort = process.argv[isSlave + 1];
    masterPort = masterPort.split(" ")[1];
    handleHandshake(masterPort);
} else {
    masterPort = PORT;
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