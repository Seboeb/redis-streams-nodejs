import { StreamProcessingFunction, RedisConsumer } from './consumer';
import { StreamMessageReply } from '@node-redis/client/dist/lib/commands/generic-transformers';
import { RedisCommandArgument } from '@node-redis/client/dist/lib/commands';
import { timeout } from './helpers';
import { RedisScripts } from 'redis';
import { EventEmitter } from 'stream';

interface RetryState {
  lastError: Error;
  timestamps: number[];
  retries: number;
  message: StreamMessageReply;
  stream: string;
  executable: StreamProcessingFunction<any>;
}

export type RetryFailedMessage = Omit<RetryState, 'executable' | 'lastError'>;
export type RetryMessage = Omit<RetryFailedMessage, 'timestamps' | 'lastError'> & { timestamp: number };

interface RetryProcessorOptions {
  maxRetry: number;
  retryTime?: string[];
}

export class RetryProcessor<S extends RedisScripts = RedisScripts> extends EventEmitter {
  private consumer: RedisConsumer<S>;
  private state: Map<RedisCommandArgument, RetryState> = new Map();

  private retryTime: string[];
  private maxRetry: number;

  constructor(consumer: RedisConsumer<S>, options: RetryProcessorOptions) {
    super();
    this.consumer = consumer;

    this.retryTime = options.retryTime || ['15s', '1m', '15m'];
    this.maxRetry = options.maxRetry;
  }

  add(error: Error, stream: string, message: StreamMessageReply, executable: StreamProcessingFunction<any>) {
    const id = message.id;

    if (this.state.has(id)) return;

    this.state.set(id, { lastError: error, timestamps: [], retries: 0, message, stream, executable });
    this.processRetry(id);
  }

  private async processRetry(id: RedisCommandArgument) {
    const stateObj = this.state.get(id)!;

    if (stateObj.retries >= this.maxRetry) {
      this.state.delete(id);
      this.emitRetryFail(stateObj);
      return;
    }

    stateObj.retries++;
    const timestamp = new Date().getTime();
    stateObj.timestamps.push(timestamp);
    const timeoutTime = this.calcTimeoutTime(stateObj);
    await timeout(timeoutTime);

    const fnc = stateObj.executable;
    const message = stateObj.message;
    try {
      this.emitRetry(stateObj);
      await fnc(message, stateObj.stream);
      this.consumer.addAckMessage(stateObj.stream, id);
      this.state.delete(id);
    } catch (err) {
      if (err instanceof Error) {
        stateObj.lastError = err;
      } else {
        const newErr = new Error(String(err));
        stateObj.lastError = newErr;
      }

      this.emitProcessFailed(stateObj);
      this.processRetry(id);
    }
    return;
  }

  private calcTimeoutTime(stateObj: RetryState) {
    const retry = stateObj.retries;
    const index = (retry > this.retryTime.length ? this.retryTime.length : retry) - 1;

    const timeString = this.retryTime[index];

    let hours = +timeString.replace(/(\d+)h/, '$1');
    let minutes = +timeString.replace(/(\d+)m/, '$1');
    let seconds = +timeString.replace(/(\d+)s/, '$1');

    if (isNaN(hours)) hours = 0;
    if (isNaN(minutes)) minutes = 0;
    if (isNaN(seconds)) seconds = 0;

    let timeoutTime = hours * 3600 * 1000 + minutes * 60 * 1000 + seconds * 1000;
    return timeoutTime;
  }

  private emitRetryFail({ lastError, stream, message, retries, timestamps }: RetryState) {
    this.consumer.client.emit('retry-failed', lastError, { stream, message, retries, timestamps });
  }

  private emitRetry({ stream, message, retries, timestamps }: RetryState) {
    const timestamp = timestamps[timestamps.length - 1];
    this.consumer.client.emit('retry', { stream, message, retries, timestamp });
  }

  private emitProcessFailed({ lastError, stream, message, retries }: RetryState) {
    this.consumer.client.emit('process-error', lastError, { stream, message, retries });
  }
}
