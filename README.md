# redis-streams-nodejs

Simple node package for easy use of Redis Streams functionality. This package allows for creation of a Redis consumer and producer.

## Installation

Make sure you have NodeJs installed, then:

```bash
npm install @secrid/redis-streams-nodejs
```

## Usage

### Basic example

```typescript
import { RedisClient } from 'mypackage'

(async () => {
  // Client name must be unique per client
  const client = new RedisClient({
    groupName: 'mygroup',
    clientName: 'myclient1'
  });

  client.on('error', (err) => console.log('Redis Client Error', err));

  await client.connect();

  const consumer = client.createConsumer();

  // Redis stream to listen to and processable function
  const stream = {
    name: 'mystream',
    executable: (data, stream) => console.log('Redis message for stream ' + stream, data);
  }

  // Listen for new messages and process them according the
  // defined executable function
  consumer.listen(stream);
})()
```

When creating the Redis client, make sure to define a group and client name. Note, the client name must be
unique in order for Redis to distinguish each individual client within the consumer group.

### Class RedisClient

_Constructor_ : `new RedisClient(options)`

- `options` [RedisClientOptions](#redisclientoptions)
- extends the `node-redis` client constructor

The `RedisClient` is an extension of the original client from the [node-redis](https://www.npmjs.com/package/redis) package. All constructor options within the `node-redis` package are available to this class as well.

#### Example

```typescript
// Connect client to Redis server with TLS enabled
const client = new RedisClient({
  socket: {
    port: 6380,
    host: 'myredis.server.com',
    tls: true,
  },
  password: 'mysupersecurepassword',
  groupName: 'mygroup',
  clientName: 'client1',
});
```

#### RedisClientOptions

| Parameters | Description                                   | Required |
| ---------- | --------------------------------------------- | -------- |
| groupName  | Name of the consumer group                    | Yes      |
| clientName | Name of the client, must be unique per client | Yes      |

Other options can be found in the official `node-redis` github repository over [here](https://github.com/redis/node-redis/blob/master/docs/client-configuration.md).

#### Methods

For all available methods, please look in the official `node-redis` repository over [here](https://github.com/redis/node-redis/blob/master/README.md).

`createConsumer(options)`

- `options` [RedisConsumerOptions](#redisconsumeroptions)
- Returns a _RedisConsumer_

`createProducer()`

- Returns a _RedisProducer_

`streamExists(key)`

- `key` key name of the stream
- Returns a _boolean_

`groupExists(key)`

- `key` name of the stream
- Returns a _boolean_

`createGroup(key)`

- `key` name of the stream
- Returns a _string_

### Class RedisConsumer

_Constructor_ : `client.createConsumer(options)`

- `options` [RedisConsumerOptions](#redisconsumeroptions)

The `RedisConsumer` is able to listen for incomming message in a stream. You can define an object or an array of objects in which you can define the name of the stream to listen for and which function should be executed for processing of the message. The consumer has a build-in retry mechanism which triggers an event `retry-failed` if all retries were unsuccessfull.

When a message is successfully processed (also in retry state), the consumer will send an acknowledgement signal to the Redis server. When the acknowlegdement is performed, the message will be removed from the pending list for that consumer group.

When the consumer starts, it will process all remaining pending messages at first before listening for new incomming messsage. However, you can overrule this behaviour by defining your own starting id.

#### Example

```typescript
  const consumer = client.createConsumer({
    COUNT: 3,
    retries: 1,
    retryTime: ['5s'],
  });

  const streams = [
    {
      name: 'mystream',
      id: '>',
      executable: (data) => console.log('Only listen to new messages', data.message);
    },
    {
      name: 'myssecondstresm',
      executable: (data, stream) => console.log('Message for stream ' + stream, data.message);
    }
  ]


  consumer.client.on('retry-failed', ({ stream, message, timestamps, retries }) => {
    console.error('Failed processing message in stream ' + stream + '. Amount of retries: ' + retries, message);
    console.log(timestamps);
  })

  consumer.listen(streams);
});
```

#### RedisConsumerOptions

| Parameters | Description                                       | Required | Default              |
| ---------- | ------------------------------------------------- | -------- | -------------------- |
| COUNT      | Number of elements to read                        | No       | 1                    |
| BLOCK      | Time in miliseconds to block while reading stream | No       | 0                    |
| retries    | Amount of retries for processing messages         | No       | 3                    |
| retryTime  | Time interval between each retry                  | No       | ['15s', '1m', '15m'] |

More information about the `BLOCK` and `COUNT` parameters can be found at the official [docs](https://redis.io/documentation) of Redis.

The `retryTime` is an array of time strings. Seconds, minutes and hours are supported ('s', 'm', 'h'). When there are less items in the `retryTime` array than the amount of retries, the last time string item is used.

If you want to disable the retry mechanism, select a value of 0 for `retries`.

#### Methods

`listen(streams)`

- `streams` [StreamToListen](#streamtolisten-object) | Array([StreamToListen](#streamtolisten-object))

`addAckMessage(stream, id)`

- `stream` key name of the stream
- `id` id of the message

Adds the message to the acknowlegdement list.

#### StreamToListen Object

```typescript
{
  name: 'mystream',                                        // Keyname of the Redis stream
  executable: (message, stream) => console.log(message),   // Message processing function to be executed
  id: '>'                                                  // Optional, start listining from the message id. Defaults to '0-0'
}
```

### Class RedisProducer

_Constructor_ : `client.createProducer()`

The `RedisProducer` is used to add new messages to the Redis stream.

#### Example

```typescript
const message = {
  firstName: 'John',
  lastName: 'Doe',
};

const producer = client.createProducer();
producer.add('mystream', message);
```

#### Methods

`add(stream, message)`

- `stream` key name of the stream
- `message` object/message to add to the stream

### Events

| Event name   | Description                                                                                                                                                                                    |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| retry-failed | Event is triggered on the `RedisConsumer` when retry of the message has failed/ended. A data object with properties `stream`, `message`, `retries` and `timestamps` is forwarded to the event. |

## Typescript

This package has full Typescript support. See the example below on how to define a processing function with typed message data.

```typescript
import { RedisClient, StreamsToListen, StreamMessageReply } from 'package';

const client = new RedisClient({
  groupName: 'mygroup',
  clientName: 'myclient1',
});
await client.connect();

const streams: StreamsToListen = [
  {
    name: 'mystream',
    executable: processing,
  },
];

const consumer = client.createConsumer();
consumer.listen(streams);

// Define interface of your message data
interface MyMessage {
  firstName: string;
  lastName: string;
}

function processing(data: StreamMessageReply<MyMessage>) {
  const message = data.message;
  const fullName = message.firstName + ' ' + message.lastName; // Full typeing of message

  console.log('Hello, my name is ' + fullName);
}
```
