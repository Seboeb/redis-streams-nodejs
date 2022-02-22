import { StreamMessageReply } from '@node-redis/client/dist/lib/commands/generic-transformers';

import { PropType } from './helpers';

export * from 'redis';
export { RedisClient } from './client';
export * from './consumer';
export * from './producer';
export { RetryFailedMessage } from './retry-processor';

export type StreamMessageId = PropType<StreamMessageReply, 'id'>;
export type StreamMessageData = PropType<StreamMessageReply, 'message'>;
