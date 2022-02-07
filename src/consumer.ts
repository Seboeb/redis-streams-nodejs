import { RedisClientType, RedisScripts } from 'redis';
import {
  StreamMessageReply,
  StreamMessagesReply,
} from '@node-redis/client/dist/lib/commands/generic-transformers';
import { RedisClient } from './client';
import { RetryProcessor } from './retry-processor';

export type StreamMessage<T = { [key: string]: string }> = Omit<StreamMessageReply, 'message'> & {
  message: T;
};

export type StreamProcessingFunction<T> = (data: StreamMessage<T>) => void;

export interface StreamToListen {
  name: string;
  executable: StreamProcessingFunction<any>;
  id?: string;
}

export type StreamsToListen = StreamToListen[];

interface State {
  nextId: string;
  lastSuccessId: string;
  executable: StreamProcessingFunction<any>;
  recovering: boolean;
}

type RedisConsumerState = Record<string, State>;

interface XGroupReadInput {
  key: string;
  id: string;
}

export interface ConsumerOptions {
  COUNT?: number;
  BLOCK?: number;
  retries?: number;
  retryTime?: string[];
}

export class RedisConsumer<S extends RedisScripts> {
  public client: RedisClientType<any, any>;

  private originalClient: RedisClient<S>;

  private state: RedisConsumerState;
  private retryProcessor: RetryProcessor<S>;
  private successfullMessages: Map<string, string[]> = new Map();

  private BLOCK: number;
  private COUNT: number;
  private RETRIES: number;

  constructor(client: RedisClient<S>, options: ConsumerOptions = {}) {
    this.originalClient = client;
    this.client = client.duplicate();
    this.state = {};

    this.COUNT = options.COUNT ?? 1;
    this.BLOCK = options.BLOCK ?? 0;
    this.RETRIES = options.retries ?? 3;

    this.retryProcessor = new RetryProcessor(this, {
      retryTime: options.retryTime,
      maxRetry: this.RETRIES,
    });

    this.client.connect();
  }

  set block(block: number) {
    this.BLOCK = block;
  }

  set count(count: number) {
    this.COUNT = count;
  }

  set retries(retries: number) {
    this.RETRIES = retries;
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

  addAckMessage(stream: string, id: string) {
    if (this.successfullMessages.has(stream)) {
      const ackMessages = this.successfullMessages.get(stream)!;
      ackMessages.push(id);
    } else {
      this.successfullMessages.set(stream, [id]);
    }

    this.acknowlegdeMessages();
  }

  private async listenForStreams() {
    const state = this.state;

    const streamsToListen: XGroupReadInput[] = [];
    for (const stream in state) {
      streamsToListen.push({ key: stream, id: state[stream].nextId });
    }

    const streamsMessages = await this.readStreams(streamsToListen);
    if (!streamsMessages) {
      console.log('No messages returned from client');
      await this.acknowlegdeMessages();
      this.listenForStreams();
      return;
    }

    for (const streamMessages of streamsMessages) {
      await this.processStreamMessages(streamMessages);
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
    let lastSuccessId: string;
    if (stream.id) {
      lastSuccessId = stream.id;
    } else {
      lastSuccessId = '0-0';
    }
    let nextId = lastSuccessId;

    this.state[name] = { nextId, lastSuccessId, executable, recovering: true };
  }

  private async readStreams(streamsToListen: XGroupReadInput[]) {
    const messages = await this.client.xReadGroup(
      this.originalClient.groupName,
      this.originalClient.clientName,
      streamsToListen,
      { BLOCK: this.BLOCK, COUNT: this.COUNT }
    );

    if (!messages) return null;
    else return messages;
  }

  private async processStreamMessages(streamMessages: { name: string; messages: StreamMessagesReply }) {
    const stream = streamMessages.name;
    const state = this.getStreamState(stream);

    if (!state) throw new Error('No state was found for stream processing of ' + stream);

    const fnc = state.executable;
    const messages = streamMessages.messages;

    for (const message of messages) {
      try {
        await fnc(message);
        this.addAckMessage(stream, message.id);
      } catch (err) {
        if (this.RETRIES === 0) continue;
        this.retryProcessor.add(stream, message, fnc);
      }
    }

    await this.acknowlegdeMessages();

    const recovering = state.recovering;
    if (recovering && messages.length === 0) {
      state.nextId = '>';
      state.recovering = false;
    } else if (recovering) {
      const lastMessage = messages.slice(-1);
      const lastId = lastMessage[0].id;
      state.nextId = lastId;
    }
    return true;
  }

  private acknowlegdeMessages() {
    this.successfullMessages.forEach((value, key) => {
      const stream = key;
      const ackMessages = value;

      this.originalClient.xAck(stream, this.originalClient.groupName, ackMessages);
    });
  }
}
