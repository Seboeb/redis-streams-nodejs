import { RedisScripts } from 'redis';
import {
  StreamMessageReply,
  StreamMessagesReply,
} from '@node-redis/client/dist/lib/commands/generic-transformers';
import { RedisClient } from './client';

export type StreamMessage<T = { [key: string]: string }> = Omit<StreamMessageReply, 'message'> & {
  message: T;
};

type StreamProcessingFunction<T> = (data: StreamMessage<T>) => void;

interface State {
  lastId: string;
  executable: StreamProcessingFunction<any>;
  recovering: boolean;
}

type RedisConsumerState = Record<string, State>;

interface XGroupReadInput {
  key: string;
  id: string;
}

export interface StreamToListen {
  name: string;
  executable: StreamProcessingFunction<any>;
  lastId?: string;
}

export type StreamsToListen = StreamToListen[];

export class RedisConsumer<S extends RedisScripts> {
  private client: RedisClient<S>;
  private state: RedisConsumerState;
  private BLOCK = 0;
  private COUNT = 1;

  constructor(client: RedisClient<S>) {
    this.client = client;
    this.state = {};
  }

  set block(block: number) {
    this.BLOCK = block;
  }

  set count(count: number) {
    this.COUNT = count;
  }

  get settings() {
    return { block: this.BLOCK, count: this.COUNT };
  }

  async listen(streams: StreamToListen | StreamsToListen) {
    if (!Array.isArray(streams)) {
      streams = [streams];
    }

    for (const stream of streams) {
      const groupExists = await this.client.groupExists(stream.name);
      if (!groupExists) {
        await this.client.createGroup(stream.name);
      }

      if (this.hasStreamState(stream.name)) continue;
      this.initStreamState(stream);
    }

    this.listenForStreams();
  }

  private async listenForStreams() {
    const state = this.state;

    const streamsToListen: XGroupReadInput[] = [];
    for (const stream in state) {
      streamsToListen.push({ key: stream, id: state[stream].lastId });
    }

    const streamsMessages = await this.readStreams(streamsToListen);
    if (!streamsMessages) {
      console.log('Something went wrong, no messages returned from client');
      return;
    }

    for (const streamMessages of streamsMessages) {
      const result = await this.processStreamMessages(streamMessages);
      console.log(result);
      // ACKNOWLEDGEMENT
    }

    this.listenForStreams();
  }

  private hasStreamState(name: string): boolean {
    return !!this.getStreamState(name);
  }

  private getStreamState(name: string): State | null {
    const state = this.state[name];
    if (state) {
      return state;
    } else {
      return null;
    }
  }

  private initStreamState(stream: StreamToListen): void {
    const name = stream.name;
    const executable = stream.executable;
    let lastId: string;
    if (stream.lastId) {
      lastId = stream.lastId;
    } else {
      lastId = '0-0';
    }
    this.state[name] = { lastId, executable, recovering: true };
  }

  private async readStreams(streamsToListen: XGroupReadInput[]) {
    const messages = await this.client.xReadGroup(
      this.client.groupName,
      this.client.clientName,
      streamsToListen,
      { BLOCK: this.BLOCK, COUNT: this.COUNT }
    );

    if (!messages) return null;
    else return messages;
  }

  private async processStreamMessages(streamMessages: { name: string; messages: StreamMessagesReply }) {
    try {
      const stream = streamMessages.name;
      const state = this.getStreamState(stream);
      if (!state) throw new Error('No state was found for stream processing of ' + stream);

      const fnc = state.executable;
      const messages = streamMessages.messages;
      for (const message of messages) {
        console.log('Executing processing function...');
        await fnc(message);
      }

      const recovering = state.recovering;
      if (recovering && messages.length === 0) {
        state.lastId = '>';
        state.recovering = false;
      } else if (recovering) {
        const lastMessage = messages.slice(-1);
        const lastId = lastMessage[0].id;
        state.lastId = lastId;
      }
      return true;
    } catch (err) {
      console.error(err);
      return false;
    }
  }
}
