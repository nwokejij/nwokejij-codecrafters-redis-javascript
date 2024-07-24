const net = require('net');
const fs = require('fs');
const replicaDict = {};
const path = require('path');

const handleHandshake = (port) => {
    const client = net.createConnection({ host: "localhost", port: port }, async () => {
      console.log("connected to master", "Port: ", port);
      
      client.write("*1\r\n$4\r\nPING\r\n");
      let firstAck = false;
      let offset = 0;
      let repl1 = false;
      client.on("data", async (data) => {
        try {
        dat = await readData(data);
        console.log("This is the data", dat);
        let message = Buffer.from(dat).toString();
        let commands = message.split("\r\n");
        console.log("Commands",commands);
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
            }
            
        }
        if (commands.includes("SET") || commands.includes("GET")) {
            if (firstAck){
                offset += query.toString().length;
            }
            parseRedisResponseFromMaster(query, replicaDict);
        }

        if (commands.includes("REPLCONF")) {
            client.write("*3\r\n" + getBulkString("REPLCONF") + getBulkString("ACK") + getBulkString(offset.toString()));
            firstAck = true;
            offset += 37;
        
        }
        
            
          }
        } catch (e){
            console.error(e);
            client.destroy();
        }
        })

            })
          }
async function readData(data){

    return new Promise((resolve, reject) => {
        try{
            let dat = data.toString();
            resolve(dat);
        } catch (e){
            reject(e);
        }
    })

}
const portIndex = process.argv.indexOf("--port");
const isSlave = process.argv.indexOf("--replicaof");
const dir = process.argv.indexOf("--dir");
const dbfilename = process.argv.indexOf("--dbfilename");
const config = {}

if (dir != -1){
    config["dir"] = process.argv[dir + 1];
}
if (dbfilename != -1){
    config["dbfilename"] = process.argv[dbfilename + 1];
}
const PORT = portIndex != -1 ? process.argv[portIndex + 1] : 6379;
let masterPort = 0;

if (isSlave != -1) {
    masterPort = process.argv[isSlave + 1];
    masterPort = masterPort.split(" ")[1];
    handleHandshake(masterPort);
    console.log("Master Port:" + masterPort)
} else {
    masterPort = PORT;
}
let listOfRBKeys = [];
let isRead = false;
// You can use print statements as follows for debugging, they'll be visible when running tests.
function readRDBFile(dir, dbfile){
    if (!dir || !dbfile || isRead){
        return;
    }
    let file = dbfile;
    let rdbPath = path.join(dir, file);
    let rdbFileBuffer = fs.readFileSync(rdbPath);
    keyBufferArray = []; // stores each Buffer/character of key string 
    valueBufferArray = []; // stores each Buffer/character of value string
    let key = ""
    let val = ""
    let hasExpiry = false;
    let isFC = false;
    for (let i = 0; i < rdbFileBuffer.length; i++){
        if (rdbFileBuffer[i]== "251"){ // ASCII for FB: hashtable size information
            let start = i;
            let noOfPairs = parseInt(rdbFileBuffer[start+ 1].toString(10), 10);
            console.log("noOfPairs", noOfPairs);
            let noOfHashes = parseInt(rdbFileBuffer[start + 2].toString(10), 10);
            console.log("noOfHashes", noOfHashes);
            let currentBuffer = start + 3;
            while (noOfPairs > 0){
                hasExpiry = false;
                isFC = false;
                console.log("First Buffer", rdbFileBuffer[currentBuffer])
                if (rdbFileBuffer[currentBuffer] == "252" || rdbFileBuffer[currentBuffer] == "253"){ // ASCII for FC and FD
                    hasExpiry = true;
                    expiryBuffer = []
                    if (rdbFileBuffer[currentBuffer] == "252"){ // FC
                        isFC = true;
                        for (let i = currentBuffer + 1; i < currentBuffer + 9; i += 1){
                            console.log("Current Byte", rdbFileBuffer[i]);
                            console.log("Current Byte in Hex", rdbFileBuffer[i].toString(16));
                            expiryBuffer.push(rdbFileBuffer[i].toString(16))
                        }
                        currentBuffer += 9
                        
                    } else{ // FD
                        for (let i = currentBuffer + 1; i < currentBuffer + 5; i += 1){
                            console.log("Current Byte", rdbFileBuffer[i]);
                            console.log("Current Byte in Hex", rdbFileBuffer[i].toString(16));
                            expiryBuffer.push(rdbFileBuffer[i].toString(16))
                        }
                        currentBuffer += 6
                        
                    }
                    noOfHashes -= 1
                    let exp = expiryBuffer.reverse().join("");
                    console.log("Joined Expiry", exp);
                    expiry = parseInt(exp, 16);

                    console.log("Expiry", expiry);
                }
                currentBuffer += 1
                let keyLength = parseInt(rdbFileBuffer[currentBuffer].toString(10), 10); // length of key string
                for (let i = currentBuffer + 1; i < currentBuffer + keyLength + 1; i++){
                    keyBufferArray.push(rdbFileBuffer[i]); // push each Key character Buffer to keyBufferArray
                }
                currentBuffer += keyLength + 1;
                let valueLength = parseInt(rdbFileBuffer[currentBuffer].toString(10), 10);
                valStart = currentBuffer + 1;
                
                for (let i = valStart; i < valStart + valueLength; i++){
                    valueBufferArray.push(rdbFileBuffer[i]);
                }
                currentBuffer += valueLength + 1
                key = Buffer.from(keyBufferArray).toString('ascii'); // from Character Buffers to String
                listOfRBKeys.push(key); // push key to list of all RB file keys
                keyBufferArray = []; // reset the key Buffer Array for the next key
                val = Buffer.from(valueBufferArray).toString('ascii'); // from Character Buffers to String
                valueBufferArray = []; // reset the value Buffer Array for the next value
                dictionary[key] = val;
                console.log("Key\n", key);
                console.log("Value\n", val);
                if (hasExpiry){
                    if (isFC){
                        console.log("Current Epoch Time", Date.now())
                            setTimeout(() => {
                                console.log(`${key}`, "has been executed")
                                delete dictionary[key]
                                console.log("Should be null", dictionary[key])
                            }, expiry - Date.now())
                        } else{
                            setTimeout(() => {
                                console.log(`${key}`, "has been executed")
                                delete dictionary[key]
                                console.log("Should be null", dictionary[key])
                            }, 1000 * (expiry- Date.now()));
                            
                        }
                }
                noOfPairs -= 1;

            }
            break;
        }

    }
}
console.log("Logs from your program will appear here!");
const replicas = [];
let propagatedCommands = 0;
let numOfAcks = 0;
const dictionary = {};
let handshakes = 0;
const server = net.createServer((connection) => {
    connection.type = 'client'; // Default type is client
    connection.on('data', (data) => {
    const command = data.toString();
    let commands = command.slice(3).split('\r\n');
    console.log("Commands", commands);
    if (commands.includes("KEYS")){
        try {
        readRDBFile(config["dir"], config["dbfilename"]);
        isRead = true;
        connection.write(getBulkArray(listOfRBKeys));
    } catch (error){
        console.error(error.message);
    }
    } else if (commands.includes("CONFIG")){
        if (commands.includes("dir")){
            connection.write("*2\r\n" + getBulkString("dir") + getBulkString(config["dir"]));
        }
        if (commands.includes("dbfilename")){
            connection.write("*2\r\n" + getBulkString("dbfilename") + getBulkString(config["dbfilename"]));
        }
    } else
    if (commands.includes("INFO")) {
        if (isSlave != -1) {
            connection.write(getBulkString("role:slave"));
        } else {
            connection.write(getBulkString("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"));
        }
    } else if (commands.includes("REPLCONF")) {
        connection.write("+OK\r\n");
    } else if (commands.includes("ECHO")) {
        let index = commands.indexOf("ECHO");
        connection.write(commands.slice(index + 1).join("\r\n"));
    } else if (commands.includes("PING")) {
        connection.write("+PONG\r\n");
    } else if (commands.includes("SET")) {
        let index = commands.indexOf("SET");
        console.log("This is the index of SET", index);
        dictionary[commands[index + 2]] = commands[index + 4];
        console.log(dictionary);
        if (commands.includes("px")) {
            let px = commands.indexOf("px");
            console.log("Hello", dictionary[commands[index + 2]], commands[px + 2]);
            setTimeout(() => {
                delete dictionary[commands[index + 2]];
            }, parseInt(commands[px + 2]));
        }
        propagateToReplicas(command);
        if (connection.type === 'client') {
            connection.write("+OK\r\n");
        }
    } else if (commands.includes("GET")) {
        let index = commands.indexOf("GET");
        readRDBFile(config["dir"], config["dbfilename"]);
        isRead = true;
        if (!(commands[index + 2] in dictionary) && !(commands[index + 2] in replicaDict)) {
            connection.write(getBulkString(null));
        } else if (commands[index + 2] in replicaDict) {
            connection.write(getBulkString(replicaDict[commands[index + 2]]));
        } else {
            connection.write(getBulkString(dictionary[commands[index + 2]]));
        }
    } else if (commands.includes("WAIT")) {
        let index = commands.indexOf("WAIT");
        let noOfReps = parseInt(commands[index + 2]);
        let time = parseInt(commands[index + 4]);
        waitCommand(noOfReps, time, connection);
    }
    if (commands.includes("PSYNC")) {
        connection.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");
        const hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        const buffer = Buffer.from(hex, 'hex');

        // Calculate the length of the file in bytes
        const bytes = buffer.length;

        // Create the RDB file header with the length of the file
        let rdbFileHeader = `$${bytes}\r\n`;

        // Combine the header and the buffer into a single buffer
        const rdbFileBuffer = Buffer.concat([Buffer.from(rdbFileHeader, 'ascii'), buffer]);
        connection.write(rdbFileBuffer);
        connection.type = 'replica'; // Set type as replica
        replicas.push(connection);
        handshakes += 1;
    }
    });
});

    


const propagateToReplicas = (command) => {
    if (replicas.length === 0) {
        return;
    }
    replicas.forEach((replica) => {
        console.log("Command to be Propagated", command);
        replica.write(command);

        // Remove any previous 'data' event handler
        replica.removeAllListeners('data');

        // Add new 'data' event handler
        replica.once("data", (data) => {
            const commands = data.toString().split('\r\n');
            if (commands.includes("ACK")) {
                numOfAcks += 1;
            }
        });
    });
    propagatedCommands += 1;
};

function getBulkString(string){
    if (string == null){
        return "$-1\r\n"
    }
    return `\$${string.length}\r\n${string}\r\n`
}

function getBulkArray(array){
    if (array == null){
        return "$-1\r\n"
    }
    let bulkResponse = `*${array.length}\r\n`
    for (let element of array){
        bulkResponse += getBulkString(element);
    }
    return bulkResponse
}

const waitCommand = (howMany, time, connection) => {
    numOfAcks = 0;
    if (propagatedCommands == 0){
        connection.write(`:${handshakes}\r\n`);
    }
    if (propagatedCommands > 0) {
        propagateToReplicas("*3\r\n" + getBulkString("REPLCONF") + getBulkString("GETACK") + getBulkString("*"));
        setTimeout(() => {
            connection.write(`:${numOfAcks > howMany ? howMany : numOfAcks}\r\n`);
        }, time);
    }
};

function parseRedisResponseFromMaster(data, replicaDict){
    console.log("Data received to Client", data);
    const type = data.charAt(0);
    switch(type) {
        case '+':
            break;
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

