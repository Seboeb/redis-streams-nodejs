import { RedisScripts } from 'redis';
import { StreamMessageData, StreamMessageId } from '.';
import { RedisClient } from './client';

export class RedisProducer<S extends RedisScripts> {
  private client: RedisClient<S>;

  constructor(client: RedisClient<S>) {
    this.client = client;
  }

  add(stream: StreamMessageId, message: StreamMessageData) {
    this.client.xAdd(stream, '*', message);
  }
}
