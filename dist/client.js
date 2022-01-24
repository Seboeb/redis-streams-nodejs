"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisClient = void 0;
const client_1 = __importDefault(require("@node-redis/client/dist/lib/client"));
const consumer_1 = require("./consumer");
const InstRedisClient = client_1.default.extend();
class RedisClient extends InstRedisClient {
    constructor(options) {
        super(options);
        this.groupName = options.groupName;
        this.clientName = options.clientName;
        this.consumer = new consumer_1.RedisConsumer(this);
    }
    async streamExists(name) {
        return await this.exists(name);
    }
    async groupExists(stream) {
        if (!(await this.streamExists(stream)))
            return false;
        const groupInfo = await this.xInfoGroups(stream);
        return groupInfo.length > 0;
    }
    async createGroup(stream) {
        const result = await this.xGroupCreate(stream, this.groupName, '$', { MKSTREAM: true });
        return result;
    }
}
exports.RedisClient = RedisClient;
