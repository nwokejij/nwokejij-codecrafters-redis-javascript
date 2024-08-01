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
const CURRENT_YEAR = 2024;
const FC = "252";
const FB = "251";
const FD = "253";
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
    let year = 2024;
    let isFC = false;
    for (let i = 0; i < rdbFileBuffer.length; i++){
        if (rdbFileBuffer[i]== FB){ // ASCII for FB: hashtable size information
            let start = i;
            let noOfPairs = parseInt(rdbFileBuffer[start+ 1].toString(10), 10);
            console.log("noOfPairs", noOfPairs);
            let noOfHashes = parseInt(rdbFileBuffer[start + 2].toString(10), 10);
            console.log("noOfHashes", noOfHashes);
            let currentBuffer = start + 3; // moving on to the Expiry  and Key-Value Section
            while (noOfPairs > 0){
                hasExpiry = false;
                isFC = false;
                console.log("First Buffer", rdbFileBuffer[currentBuffer])
                if (rdbFileBuffer[currentBuffer] == FC || rdbFileBuffer[currentBuffer] == FD){ // ASCII for FC and FD
                    hasExpiry = true;
                    expiryBuffer = []
                    if (rdbFileBuffer[currentBuffer] == FC){ // FC
                        isFC = true;
                        for (let i = currentBuffer + 1; i < currentBuffer + 9; i += 1){
                            console.log("Orig Buffer", rdbFileBuffer[i]);
                            hexBuffer = rdbFileBuffer[i].toString(16);
                            hexBufferLength = hexBuffer.length;
                            if (hexBufferLength < 2){
                                hexBuffer = "0" + hexBuffer; // need to pad with 0 for single digit numbers for accurate epoch conversion
                            }
                            expiryBuffer.push(hexBuffer)
                        }
                        currentBuffer += 9 // move to the Key-Value Section
                        
                    } else{ // FD
                        for (let i = currentBuffer + 1; i < currentBuffer + 5; i += 1){
                            console.log("Orig Buffer", rdbFileBuffer[i]);
                            console.log("Hex Buffer", rdbFileBuffer[i].toString(16));
                            expiryBuffer.push(rdbFileBuffer[i].toString(16))
                        }
                        currentBuffer += 6 // moving to the Key-Value Section
                        
                    }
                    let exp = expiryBuffer.reverse().join("");
                    console.log("Joined Expiry", exp);
                    expiry = parseInt(exp, 16);
                    console.log("Expiry", expiry);
                    let date = new Date(expiry);
                    let readableDate = date.toLocaleString();
                    console.log("readableDate", readableDate);
                    year = readableDate.split(',')[0].split('/')[2] // extracting the year
                }
                currentBuffer += 1 // skipping the String Encoded Value 00
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
                        if (year < CURRENT_YEAR){
                            console.log("Key to be deleted",  key)
                            delete dictionary[key]
                        }
                        } 
                }
                noOfPairs -= 1;

            }
            break;
        }

    }
}
console.log("Logs from your program will appear here!");
class Stream {
    constructor(key, id){
        this.key = key;
        this.id = id;
        this.pairs = [];
    }
}
const replicas = [];
let propagatedCommands = 0;
let numOfAcks = 0;
const dictionary = {};
let handshakes = 0;
const streamKey = {};
const streamArray = [];
let prevStreamID = null;
let timeToVersion = {}
let notCalled = false;
const server = net.createServer((connection) => {
    connection.type = 'client'; // Default type is client
    connection.on('data', async (data) => {
    const command = data.toString();
    let commands = command.slice(3).split('\r\n');
    commands.pop(); // remove last empty spot
    for (let i = 1; i < commands.length; i+= 2){
        commands[i] = commands[i].toLowerCase();
    }
    console.log("Commands", commands);
    if (commands.includes("xread")){
        queries = commands.slice(commands.indexOf("streams") + 1);
        idStart = queries.length / 2;
        collectKeys = [];
        collectIDs = [];
        for (let i = 1; i < idStart; i += 2){
            collectKeys.push(queries[i]);
        }
        for (let j = idStart + 1; j < queries.length; j += 2){
            collectIDs.push(queries[j])
        }
        if (commands.includes("block")){
            if (!notCalled){
                timeIndex = parseInt(commands[commands.indexOf("block") + 2], 10);
                if (timeIndex == 0){
                    await awaitChange(collectKeys, collectIDs);
                    
            }
                res = await xreadStreams(collectKeys, collectIDs, timeIndex)
                connection.write(getBulkArray(res));
                notCalled = true;
            } else {
                connection.write(getBulkString(null));
            }
            
        } else {
            res = await xreadStreams(collectKeys, collectIDs);
            connection.write(getBulkArray(res));
        }
        // if commands.includes block
        // anytime a new xadd command happens, need to call xreadStreams
        // limited time to add 
        // res = xreadStreams(collectKeys, collectIDs);
        // connection.write(getBulkArray(res));
    } else if (commands.includes("xrange")){
    
        index = commands.indexOf("xrange");
        leftBound = commands[index + 4].toString();
        let fromTheStart = false;
        if (leftBound == "-"){
            fromTheStart = true;
        }
        leftBoundTime = parseInt(leftBound.split("-")[0], 10);
        rightBound = commands[index + 6].toString();
        rightBoundTime = parseInt(rightBound.split("-")[0], 10);
        containsVersionLeft = leftBound.includes("-");
        containsVersionRight = rightBound.includes("-");
    
        // array containning arrays, where each array contains two elements, the id, and an array of the properties associated with that id (excluding the stream key)
        let withinRange = [];
     
        let shouldInclude = false;
        for (let stream of streamArray){
            console.log("Stream Pairs", stream.pairs);
            parsed_stream_id = stream.id.split("-")
            let time = parseInt(parsed_stream_id[0], 10);
            console.log("time", time);
            let version = parseInt(parsed_stream_id[1], 10);
            console.log("version", version);
            if (fromTheStart){
                shouldInclude = true;
            } else if (time == leftBoundTime){
                if (containsVersionLeft){
                    leftBoundVersion = parseInt(leftBound.split("-")[1], 10);
                    if (version < leftBoundVersion){
                        shouldInclude = false;
                    } else {
                        shouldInclude = true;
                    }
                } else{
                    shouldInclude = true;
                }
            }
            if(time == rightBoundTime){
                if (containsVersionRight){
                    rightBoundVersion = parseInt(rightBound.split("-")[1], 10); //
                    if (shouldInclude && version > rightBoundVersion){
                        shouldInclude = false;
                    } 
                } else {
                    shouldInclude = true;
                } 
            }
            if (time > leftBoundTime && time < rightBoundTime){
                shouldInclude = true;
            } else if (time > rightBoundTime){
                shouldInclude = false;
            }

            console.log("ShouldInclude", shouldInclude);

            if (shouldInclude){
                withinRange.push([stream.id, stream.pairs.join(",").split(",")]);
            }

            }
            console.log("WithinRange", withinRange);
            console.log("bulkArray", getBulkArray(withinRange));
            connection.write(getBulkArray(withinRange));
        }
    else if (commands.includes("xadd")){
        let cmd = commands.indexOf("xadd");
        console.log("First Entry point");
        
        stream_id = commands[cmd + 4];
        if (stream_id == "*"){
            date = Date.now().toString() + "-0";
            connection.write(getBulkString(date));
        } else {
        let milliseconds = stream_id.split("-")[0];
        let version = stream_id.split("-")[1];
        let auto = false;
        if (milliseconds == "0" && version == "0"){
            connection.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
        } else if ((prevStreamID) && ((milliseconds != "*") && (milliseconds < prevStreamID.split("-")[0]) || ((milliseconds == prevStreamID.split("-")[0]) && (version != "*" && version <= prevStreamID.split("-")[1])))){
                connection.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            } else {
                if (version == "*"){
                    auto = true;
                    if (milliseconds in timeToVersion){
                        leng = timeToVersion[milliseconds].length
                        let newVersion = parseInt(timeToVersion[milliseconds][leng - 1], 10) + 1
                        version = newVersion.toString();
                    } else {
                        if (milliseconds == "0"){
                            version = "1"
                        } else {
                            version = "0"
                        }
                    }
                } 
                console.log("Second Entry point")
                if (!(milliseconds in timeToVersion)){
                    timeToVersion[milliseconds] = []
                }
                timeToVersion[milliseconds].push(version);
                stream_key = commands[cmd + 2];
                let stream = new Stream(stream_key, stream_id);
                keyCounter = cmd + 4; // id of stream
                while (keyCounter < commands.length - 1){
                    let keyVal = [];
                    keyCounter += 2
                    keyVal.push(commands[keyCounter])
                    keyCounter += 2;
                    keyVal.push(commands[keyCounter])
                    stream.pairs.push(keyVal);
                }
                if (!(stream_key in streamKey)){
                    streamKey[stream_key] = [];
                }
                streamKey[stream_key].push(stream);
                streamArray.push(stream);
                prevStreamID = stream_id
                console.log("Fourth entry point")
                if (auto){
                    let auto_reply = `${milliseconds}-${version}`
                    connection.write(getBulkString(auto_reply));
                    auto = false;
                } else {
                    console.log("Fifth Entry Point")
                    connection.write(getBulkString(stream_id));
                }
            } 
        }
            
            
            }
    else if (commands.includes("type")){

        let type = commands.indexOf("type");
        let key = commands[type + 2];
        if (!(key in dictionary) && !(key in streamKey)){
            connection.write("+none\r\n")
        } else{
            if (key in streamKey){
                connection.write("+stream\r\n")
            }
            let typeValue = typeof dictionary[key];
            connection.write(`+${typeValue}\r\n`)
        }
    }
    else if (commands.includes("keys")){
        try {
        readRDBFile(config["dir"], config["dbfilename"]);
        isRead = true;
        connection.write(getBulkArray(listOfRBKeys));
    } catch (error){
        console.error(error.message);
    }
    } else if (commands.includes("config")){
        if (commands.includes("dir")){
            connection.write("*2\r\n" + getBulkString("dir") + getBulkString(config["dir"]));
        }
        if (commands.includes("dbfilename")){
            connection.write("*2\r\n" + getBulkString("dbfilename") + getBulkString(config["dbfilename"]));
        }
    } else
    if (commands.includes("info")) {
        if (isSlave != -1) {
            connection.write(getBulkString("role:slave"));
        } else {
            connection.write(getBulkString("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"));
        }
    } else if (commands.includes("replconf")) {
        connection.write("+OK\r\n");
    } else if (commands.includes("echo")) {
        let index = commands.indexOf("echo");
        connection.write(getBulkString(commands[index+2]));
    } else if (commands.includes("ping")) {
        connection.write("+PONG\r\n");
    } else if (commands.includes("set")) {
        let index = commands.indexOf("set");
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
    } else if (commands.includes("get")) {
        let index = commands.indexOf("get");
        readRDBFile(config["dir"], config["dbfilename"]);
        isRead = true;
        if (!(commands[index + 2] in dictionary) && !(commands[index + 2] in replicaDict)) {
            connection.write(getBulkString(null));
        } else if (commands[index + 2] in replicaDict) {
            connection.write(getBulkString(replicaDict[commands[index + 2]]));
        } else {
            connection.write(getBulkString(dictionary[commands[index + 2]]));
        }
    } else if (commands.includes("wait")) {
        let index = commands.indexOf("wait");
        let noOfReps = parseInt(commands[index + 2]);
        let time = parseInt(commands[index + 4]);
        waitCommand(noOfReps, time, connection);
    }
    if (commands.includes("psync")) {
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


async function xreadStreams(keys, ids, delay = 0){
    return new Promise((resolve, reject) => {
        
        setTimeout(() => {
            res = [];
    // loop through the keys and ids iteratively
    for (let i = 0; i < keys.length; i += 1){
        key = keys[i]
        id = ids[i]
        minTime = parseInt(id.split("-")[0], 10);
        minVersion = parseInt(id.split("-")[1], 10);
        for (let strm of streamKey[key]){
           time = parseInt(strm.id.split("-")[0], 10);
           version = parseInt(strm.id.split("-")[1], 10);
           if (time > minTime || (time == minTime && version > minVersion)){
            res.push([strm.key, [[strm.id, strm.pairs.join(",").split(",")]]])
           }
        }
    }
        resolve(res);
        }, delay)


    })
}
    


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
            console.log("Command inside replica", commands);
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
async function awaitChange(keys){
    return new Promise((resolve, reject) => {
        console.log("streamKey",keys[0]);
        console.log("streamKey Pairs", streamKey[keys[0]]);
        console.log("id", streamKey[keys[0]].id);
        blockedStreamPairs = streamKey[keys[0]].pairs;
        console.log("Type", blockedStreamPairs);
        blockedStreamCopy = blockedStreamPairs.slice();
        console.log("Copy", blockedStreamCopy);
        let intervalId = setInterval(()=> {
            if (blockedStreamCopy.length < streamKey[keys[0]].pairs.length){
                clearInterval(intervalId);
                resolve();
            }
        }, 1000);
        
    })
    
    
}
function getBulkArray(array){
    if (array == null){
        return "$-1\r\n"
    }
    let bulkResponse = `*${array.length}\r\n`
    for (let element of array){
        if (Array.isArray(element)){
            bulkResponse += getBulkArray(element);
        } else{
            bulkResponse += getBulkString(element);
        }
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

