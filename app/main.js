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
// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");


const replicas = [];
let propagatedCommands = 0;
let numOfAcks = 0;
const dictionary = {};

let handshakes = 0;
const pythonScriptPath = path.join("../", 'redis-rdb-tools', 'rdbtools', 'read_rdb.py');
const server = net.createServer((connection) => {
    connection.type = 'client'; // Default type is client
    connection.on('data', (data) => {
    const command = data.toString();
    let commands = command.slice(3).split('\r\n');
    console.log("Commands", commands);
    if (commands.includes("KEYS")){
        try {
        
        let file = config["dbfilename"];
        let rdbPath = path.join(config["dir"], file);
        let rdbFileBuffer = fs.readFileSync(rdbPath);
        // console.log("Buffer Read", rdbFileBuffer);
        for (let i = 0; i < 50; i++){
            byte =  rdbFileBuffer[i];
            if (byte == "254"){
                console.log("Hello");
            }
            console.log("Buffer char", byte);
            buff = Buffer.from([byte]);
            console.log("Buffer String", buff);
        }
    } catch (error){
        console.error(error.message);
    }
        // readRdbFile(rdbFilePath, (data) => {
        //     console.log("Have we reached this block");
        //     console.log("Parsed RDB data", data);
        // })

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
        console.log("This is is in the commands list after index", commands[index], commands[index +1], commands[index + 2], commands[index + 3], commands[index + 4]);
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

function readRdbFile(rdbFilePath, callback) {
    const command = `python ${pythonScriptPath} ${rdbFilePath}`;
    exec(command, (error, stdout, stderr) => {
        if (error) {
            console.error(`Error: ${error.message}`);
            return;
        }
        if (stderr) {
            console.error(`stderr: ${stderr}`);
            return;
        }
        callback(stdout);
    });
}



// Read and output the key-value from the RDB file

server.listen(PORT, "127.0.0.1");

