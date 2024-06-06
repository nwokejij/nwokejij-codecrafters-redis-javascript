const { Console } = require("console");
const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  // Handle connection
    connection.on('data', (data) => {
        // connection.write("+PONG\r\n");

        const command = data.toString();
        const message = parseRedisResponse(command);
        connection.write(message);
    })


});

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
        //*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n$11\r\n<11char>\r\n
        //how to parse this
        // *2 $4 ECHO $3 hey $X <word>
        // 2, 4, 6
        // $4\r\nECHO
        // *1\r\n$4\r\nPING\r\n
        // *1 $4 PING
            words = data.split('\r\n');
            // const elements = parseInt(content, 10);
            console.log("Data:" + data);
            delimiter = data.indexOf('\r\n');
            console.log("Index: " + delimiter);
            bulkStrings = data.slice(delimiter+1);
            console.log("string:" + bulkStrings);
            stringArray = bulkStrings.split('\r\n');
            stringArrayLen = stringArray.length;
            noNewLine = [];
            console.log("Message:" + stringArray[0]);
            for (let i = 0; i < stringArrayLen; i++){
                if (stringArray[i] == "ECHO"){
                    noNewLine.pop();
                    continue;
                } else {
                    noNewLine.push(stringArray[i]);
                }
            }
            strings = noNewLine.join("\r\n");
            console.log(strings);
            // let index = data.indexOf('\r\n'); // 3
            
            // console.log("BulkStrings: " + bulkStrings + ".");
            // const subResponse = parseRedisResponse(bulkStrings);
            return strings;
        default:
            throw new Error('Invalid Redis response');
    }
}
server.listen(6379, "127.0.0.1");
