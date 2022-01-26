import { StreamProcessingFunction, RedisConsumer } from './consumer';
import { StreamMessageReply } from '@node-redis/client/dist/lib/commands/generic-transformers';
import { timeout } from './helpers';
import { RedisScripts } from 'redis';

interface RetryState {
  retries: number;
  message: StreamMessageReply;
  stream: string;
  executable: StreamProcessingFunction<any>;
}

interface RetryProcessorOptions {
  maxRetry: number;
  retryTime?: string[];
}

export class RetryProcessor<S extends RedisScripts> {
  private consumer: RedisConsumer<S>;
  private state: Map<string, RetryState> = new Map();

  private retryTime: string[];
  private maxRetry: number;

  constructor(consumer: RedisConsumer<S>, options: RetryProcessorOptions) {
    this.consumer = consumer;

    this.retryTime = options.retryTime || ['15s', '1m', '15m'];
    this.maxRetry = options.maxRetry;
  }

  add(stream: string, message: StreamMessageReply, executable: StreamProcessingFunction<any>) {
    const id = message.id;

    if (this.state.has(id)) return;

    this.state.set(id, { retries: 0, message, stream, executable });
    this.retry(id);
  }

  private async retry(id: string) {
    const stateObj = this.state.get(id)!;

    if (stateObj.retries >= this.maxRetry) {
      this.state.delete(id);
      this.consumer.addAckMessage(stateObj.stream, id);
      return;
    }

    stateObj.retries++;
    const fnc = stateObj.executable;
    const message = stateObj.message;

    const timeoutTime = this.calcTimeoutTime(stateObj);
    await timeout(timeoutTime);

    try {
      await fnc(message);
      this.consumer.addAckMessage(stateObj.stream, id);
      this.state.delete(id);
    } catch (err) {
      this.retry(id);
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
}
