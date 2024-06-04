const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  // Handle connection
    connection.on('data', (data) => {
        // connection.write("+PONG\r\n");

        const command = data.toString().trim();
        console.log(command);
        const message = parseRedisResponse(command.substring(5));
        console.log("Message: "+ message);
    })


});

function parseRedisResponse(data) {
    const type = data.charAt(0);
    console.log("Type:" + type);
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
            return data.slice(data.indexOf('\r\n') + 2, data.indexOf('\r\n') + 2 + length);
        case '*': // Array
            const elements = parseInt(content, 10);
            const arrayData = [];
            let index = data.indexOf('\r\n');
            for (let i = 0; i < elements; i++) {
                const subResponse = parseRedisResponse(data.slice(index));
                arrayData.push(subResponse);
                index += subResponse.length + 4; // +4 for "$\r\n" or "*\r\n"
            }
            return arrayData;
        default:
            throw new Error('Invalid Redis response');
    }
}
server.listen(6379, "127.0.0.1");
