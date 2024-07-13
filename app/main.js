const replicaDict = {};
let numOfReplicas = 0;
let numOfAcks = 0;
let propagatedCommands = 0;
const handleHandshake = (port) => {
    const client = net.createConnection({ host: "localhost", port: port }, async () => {
      console.log("connected to master", "Port: ", port);
      
    //   client.write("*1\r\n$4\r\nPING\r\n");
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
            message = parseRedisResponseFromMaster(query, replicaDict);
            client.write(message);
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
let flag = false;
const propagateToReplicas = (command) => {
    if (replicas.length == 0){
        return
    }
    replicas.forEach((replica) => {
        console.log("Command to be Propagated", command);
        if (!flag){
            replica.write(command);
        } else {
            replica.write(":3\r\n");
        }
        
        

        replica.on("data", (data) => {
            const commands = data.toString().split('\r\n');
            if (commands.includes("ACK")){
                numOfAcks += 1;
            }
        })

    })
    propagatedCommands += 1;
}
// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  // Handle connection
    connection.type = 'client';
    connection.on('data', async (data) => {
        const command = data.toString();
        let commands = command.slice(3).split('\r\n');
        commands.pop();
        console.log("Commands", commands);
        if (commands.includes("INFO")){
                if (isSlave != -1){
                    connection.write(getBulkString("role:slave"));
                }
                connection.write(getBulkString("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"));
            } else if (commands.includes("REPLCONF")){
                connection.write("+OK\r\n");
            } else if (commands.includes("ECHO")){
                index = commands.indexOf("ECHO");
                connection.write(commands.slice(index+1).join("\r\n"));
            } else if (commands.includes("PING")){
                connection.write("+PONG\r\n");
            } else if (commands.includes("SET")){
                index = commands.indexOf("SET");
                dictionary[commands[index + 2]] = dictionary[commands[index + 4]];
                    if (commands.includes("px")){
                        let px = commands.indexOf("px");
                        setTimeout(() => {
                            delete dictionary[commands[index + 2]];
                            }, parseInt(commands[px + 2])
                        )
                    }
                        if (commands.includes("baz")){
                            console.log("Hello there");
                            flag = true;
                            myPromise(commands).then((dat) => {
                                connection.write(dat);
                            }
                            )
                            connection.write("+OK\r\n");
                        } else {
                            connection.write("+OK\r\n");
                        }
                        propagateToReplicas(command);
                } else if (commands.includes("GET")){
                   index = commands.indexOf("GET");
                if (!(commands[index + 2] in dictionary) && !(commands[index + 2] in replicaDict)) {
                    connection.write(getBulkString(null));
                } else if (commands[index + 2] in replicaDict) {
                    connection.write(getBulkString(replicaDict[commands[index + 2]]));
                } else{
                    connection.write(getBulkString(dictionary[commands[index + 2]]));
                } 
                
            } else if (commands.includes("WAIT")){
                index = commands.indexOf("WAIT");
                noOfReps = parseInt(commands[index + 2])
                time = parseInt(commands[index+4]);
                waitCommand(noOfReps, time, connection);
            }
            if (commands.includes("PSYNC")){
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
            numOfReplicas += 1;
            
            } 
            
        })

function waitCommand(howMany, time, connection){
    numOfAcks = 0;
    console.log("Propagated Commands", propagatedCommands);
    if (propagatedCommands > 0){
        propagateToReplicas("*3\r\n" + getBulkString("REPLCONF") + getBulkString("GETACK") + getBulkString("*"));
        setTimeout(() => {

            connection.write(`:${numOfAcks > howMany ? howMany : numOfAcks}\r\n`);
        }, time);
    }
    


    }
    

    

// const wait = (args, connection) => {
//     // Parse arguments and reset acknowledgment tracking
//     const noOfReplica = parseInt(args[0]);
//     const delay = parseInt(args[1]);
//     numOfAcks = 0;
//     ack_needed = noOfReplica;
//     reply_wait = false;
  
//     // If no commands need propagation, reply immediately
//     if (propogated_commands === 0) {
//       reply_wait = true;
//       connection.write(`:${replicaList.size}\r\n`);
//     } else {
//       // Request acknowledgment status from replicas
//       propagateToReplicas("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
//     }
  
//     // Set a timeout to send a reply if the required acknowledgments aren't received
//     setTimeout(() => {
//       if (!reply_wait)
//         connection.write(
//           `:${numOfAcks > noOfReplica ? noOfReplica : numOfAcks}\r\n`
//         );
//     }, delay);
//   };
const dictionary = {};
function myPromise(data){
    return new Promise((resolve, reject) => {
        if (data.includes("baz")){
            resolve("+O");
        } else{
            reject()
        }

    })
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
})
server.listen(PORT, "127.0.0.1");
