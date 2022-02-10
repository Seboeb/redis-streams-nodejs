import { RedisScripts } from 'redis';
import { RedisClient } from './client';

import { TuplesObject } from '@node-redis/client/dist/lib/commands/generic-transformers';

export class RedisProducer<S extends RedisScripts> {
  private client: RedisClient<S>;

  constructor(client: RedisClient<S>) {
    this.client = client;
  }

  add(stream: string, message: TuplesObject) {
    this.client.xAdd(stream, '*', message);
  }
}
