"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("./client");
async function main() {
    const client = new client_1.RedisClient({
        socket: {
            port: 6380,
            host: 'hvergelmir-test.redis.cache.windows.net',
            tls: true,
        },
        password: 'AJDWr1TuNnUF4KIoV5JYDJJR7mKsRoFiBAzCaDAj2N4=',
        groupName: 'mygroup',
        clientName: 'client1',
    });
    await client.connect();
    client.on('error', err => console.log('Redis Client Error', err));
    console.log('connected!');
    const consumer = client.consumer;
    consumer.count = 2;
    const streams = [
        { name: 'mystream', executable: logData },
        { name: 'mystream2', executable: logData2 },
    ];
    consumer.listen(streams);
}
main();
function logData(data) {
    console.log('Knallen maar!', data);
    throw new Error('test');
}
function logData2(message) {
    console.log('dit is function 2!', message);
}
