import { StreamProcessingFunction } from './consumer';
import { StreamMessageReply } from '@node-redis/client/dist/lib/commands/generic-transformers';
import { timeout } from './helpers';

interface RetryState {
  retries: number;
  maxRetry: number;
  message: StreamMessageReply;
  executable: StreamProcessingFunction<any>;
}

export class RetryProcessor {
  private state: Map<string, RetryState> = new Map();
  private timeoutMap = ['1s', '5s', '10s', '1m'];

  constructor() {}

  add(message: StreamMessageReply, executable: StreamProcessingFunction<any>, maxRetry: number) {
    const id = message.id;

    if (this.state.has(id)) return;

    this.state.set(id, { retries: 0, maxRetry, message, executable });
    console.log('New entry for retry processor: ' + id);

    this.retry(id);
  }

  private async retry(id: string) {
    const stateObj = this.state.get(id)!;

    if (stateObj.retries >= stateObj.maxRetry) {
      console.log('Removing retry from state');
      this.state.delete(id);
      console.log(this.state);
      return;
    }

    stateObj.retries++;
    const fnc = stateObj.executable;
    const message = stateObj.message;
    try {
      console.log('RETRYING...');

      const timeoutTime = this.calcTimeoutTime(stateObj);

      console.log('Waittime', timeoutTime);
      await timeout(timeoutTime);

      await fnc(message);
      this.state.delete(id);
    } catch (err) {
      this.retry(id);
    }
    return;
  }

  private calcTimeoutTime(stateObj: RetryState) {
    const index = stateObj.retries;
    const timeString = this.timeoutMap[index - 1];

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
