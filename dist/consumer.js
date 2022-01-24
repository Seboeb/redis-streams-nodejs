"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisConsumer = void 0;
class RedisConsumer {
    constructor(client) {
        this.BLOCK = 0;
        this.COUNT = 1;
        this.RETRIES = 3;
        this.client = client;
        this.state = {};
    }
    set block(block) {
        this.BLOCK = block;
    }
    set count(count) {
        this.COUNT = count;
    }
    get settings() {
        return { block: this.BLOCK, count: this.COUNT };
    }
    async listen(streams) {
        if (!Array.isArray(streams)) {
            streams = [streams];
        }
        for (const stream of streams) {
            const groupExists = await this.client.groupExists(stream.name);
            if (!groupExists) {
                await this.client.createGroup(stream.name);
            }
            if (this.hasStreamState(stream.name))
                continue;
            this.initStreamState(stream);
        }
        this.listenForStreams();
    }
    async listenForStreams() {
        const state = this.state;
        const streamsToListen = [];
        for (const stream in state) {
            streamsToListen.push({ key: stream, id: state[stream].lastId });
        }
        const streamsMessages = await this.readStreams(streamsToListen);
        if (!streamsMessages) {
            console.log('No messages returned from client');
            this.listenForStreams();
            return;
        }
        for (const streamMessages of streamsMessages) {
            const result = await this.processStreamMessages(streamMessages);
            console.log(result);
        }
        this.listenForStreams();
    }
    hasStreamState(name) {
        return !!this.getStreamState(name);
    }
    getStreamState(name) {
        const state = this.state[name];
        if (state) {
            return state;
        }
        else {
            return null;
        }
    }
    initStreamState(stream) {
        const name = stream.name;
        const executable = stream.executable;
        let lastId;
        if (stream.lastId) {
            lastId = stream.lastId;
        }
        else {
            lastId = '0-0';
        }
        this.state[name] = { lastId, executable, recovering: true, failed: [] };
    }
    async readStreams(streamsToListen) {
        const messages = await this.client.xReadGroup(this.client.groupName, this.client.clientName, streamsToListen, { BLOCK: this.BLOCK, COUNT: this.COUNT });
        if (!messages)
            return null;
        else
            return messages;
    }
    async processStreamMessages(streamMessages) {
        const stream = streamMessages.name;
        const state = this.getStreamState(stream);
        if (!state)
            throw new Error('No state was found for stream processing of ' + stream);
        const fnc = state.executable;
        const messages = streamMessages.messages;
        for (const message of messages) {
            console.log('Executing processing function...');
            try {
                await fnc(message);
            }
            catch (err) {
                console.error(err);
                const failed = state.failed.find(item => item.id === message.id);
                if (!failed) {
                    state.failed.push({ id: message.id, retried: 0 });
                    return false;
                }
                else if (failed.retried >= this.RETRIES) {
                    console.log('Retries FAILED! Continue process rest of the messages');
                }
                else {
                    failed.retried++;
                    return false;
                }
            }
        }
        const recovering = state.recovering;
        if (recovering && messages.length === 0) {
            state.lastId = '>';
            state.recovering = false;
        }
        else if (recovering) {
            const lastMessage = messages.slice(-1);
            const lastId = lastMessage[0].id;
            state.lastId = lastId;
        }
        return true;
    }
}
exports.RedisConsumer = RedisConsumer;
