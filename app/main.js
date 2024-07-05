const replicaDict = {};

const handleHandshake = (port) => {
    const client = net.createConnection({ host: "localhost", port: port }, async () => {
      console.log("connected to master", "Port: ", port);
      client.write("*1\r\n$4\r\nPING\r\n");
      let firstAck = false;
      let offset = 0;
      let repl1 = false;
      client.on("data", async (data) => {
        dat = await readData(data);
        let message = Buffer.from(dat).toString();
        let commands = message.split("\r\n");
        console.log(`Command recieved by replica:`, commands);
        console.log("Raw message", message);
        while (message.length > 0) {
            let index = message.indexOf("*", 1);
            let query;
            if (index == -1) {
              query = message;
             message = "";
            } else {
              query = message.substring(0, index);
             message = message.substring(index);
            }
            commands = Buffer.from(query).toString().split("\r\n");
          if (commands[0] == "+PONG") {
            client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("listening-port")+ getBulkString(PORT));
          } 
          if (commands[0] == "+OK") {
            if (repl1 == false) {
              client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("capa") + getBulkString("psync2"));
              repl1 = true;
            } else client.write("*3\r\n" + getBulkString("PSYNC") + getBulkString("?")+ getBulkString("-1"));
          } 
          if (commands.includes("PING") ){
            if (firstAck){
                offset += 14;
                console.log("Offset in Ping Block:" + offset);
            }
            
        }
        if (commands.includes("REPLCONF")) {
            client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("ACK") + getBulkString(offset.toString()));
            firstAck = true;
            if (commands.includes("REPLCONF")){
                offset += 37;
            } 
            console.log("Offset IN REPLCONF Block: " + offset);
        }
        if (commands.includes("SET") || commands.includes("GET")) {

            parseRedisResponseFromMaster(message, replicaDict);
        }
        
            
          }
        })
            })
          }

async function readData(data){

    return new Promise((resolve, reject) => {
        try{
            dat = data.toString();
            resolve(dat);
        } catch (e){
            reject(e);
        }
    })

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
    console.log("Master Port:" + masterPort)
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
    console.log("Data received to Client", data);
    const type = data.charAt(0);
    switch(type) {
        case '+':
        case '*': // Array
            delimiter = data.indexOf('\r\n');
            bulkStrings = data.slice(delimiter+2); 
            stringArray = bulkStrings.split('\r\n');
            stringArrayLen = stringArray.length;
            noNewLine = [];
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

server.listen(PORT, "127.0.0.1");