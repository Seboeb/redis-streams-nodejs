import { RedisClient } from '../src/client';
import { StreamsToListen, StreamMessageReply } from '../src/consumer';

interface MyStreamMessage {
  name: string;
  age: string;
  country: string;
}

async function main() {
  const client = new RedisClient({
    socket: {
      port: 6380,
      host: 'my.redis.server.com',
      tls: true,
    },
    password: 'ultrasecurepassword',
    groupName: 'mygroup',
    clientName: 'client1',
  });

  client.on('error', err => console.error('Sometime went wrong', err));
  await client.connect();

  const producer = client.createProducer();
  const consumer = client.createConsumer({
    BLOCK: 15 * 1000, // 15 seconds
  });

  consumer.client.on('retry-failed', ({ stream, message, _timestamps, _retries }) => {
    console.log('Retry failed, sending message to failed stream');

    // Add message to new 'failed-messages' stream
    producer.add('failed-messages', message);

    // Remove failed message from pending list of original stream
    consumer.addAckMessage(stream, message.id);
  });

  const streams: StreamsToListen = [
    {
      name: 'mystream',
      executable: processData,
    },
    {
      name: 'yourstream',
      executable: processData,
    },
  ];

  consumer.listen(streams);
}

function processData(data: StreamMessageReply<MyStreamMessage>, stream) {
  const id = data.id;
  const message = data.message;

  console.log('Start processing message with id ' + id);

  switch (stream) {
    case 'mystream':
      // ... processing stuff
      break;
    case 'yourstream':
      // ... processing other stuff
      break;
    default:
      console.log('Stream not supported!');
      break;
  }

  console.log('Message was processed!', message);
}

main();
