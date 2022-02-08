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
    executable: message => console.log('Redis message', message);
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

#### Methods

For all available methods, please look in the official `node-redis` repository over [here](https://github.com/redis/node-redis/blob/master/README.md).

**`createConsumer(options)`**

- `options` RedisConsumerOptions
- Returns a <RedisConsumer>

**`createProducer()`**

- Returns a <RedisProducer>

**`streamExists(key)`**

- `key` key name of the stream
- Returns a <boolean>

**`groupExists(key)`** -`key` name of the stream

- Returns a <boolean>

**`createGroup(key)`**

- `key` name of the stream
- Returns a <string>

#### RedisClientOptions

| Parameters | Description                                   | Required |
| ---------- | --------------------------------------------- | -------- |
| groupName  | Name of the consumer group                    | Yes      |
| clientName | Name of the client, must be unique per client | Yes      |

Other options can be found in the official `node-redis` github repository over [here](https://github.com/redis/node-redis/blob/master/docs/client-configuration.md).
