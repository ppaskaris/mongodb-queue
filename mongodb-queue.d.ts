import { MongoClient } from "mongodb";

declare module "mongodb-queue" {

    interface QueueOptions {
        visibility?: number;
        delay?: number;
        deadQueue?: Queue;
        maxRetries?: number;
        cleanAfterSeconds?: number;
    }

    interface AddOptions {
        delay?: number;
        debounce?: string;
    }

    interface Message {
        id: string;
        ack: string;
        payload: any;
        tries: number;
    }

    interface GetOptions {
        visibility?: number;
    }

    interface PingOptions {
        visibility?: number;
    }

    class Queue {
        constructor(mongoDbClient: MongoClient, name: string, opts?: QueueOptions);
        createIndexes(callback: (err: Error, indexname: string) => void): void;
        migrate(callback: (err: Error, modifiedCount: number) => void): void;
        add(payload: any | any[], callback: (err: Error, id: string) => void): void;
        add(payload: any | any[], opts: AddOptions, callback: (err: Error, id: string) => void): void;
        get(callback: (err: Error, msg: Message) => void): void;
        get(opts: GetOptions, callback: (err: Error, msg: Message) => void): void;
        ping(ack: string, callback: (err: Error, id: string) => void): void;
        ping(ack: string, callback: (err: Error, id: string) => void): void;
        ack(ack: string, opts: PingOptions, callback: (err: Error, id: string) => void): void;
        clean(callback: (err: Error) => void): void;
        total(callback: (err: Error, count: number) => void): void;
        size(callback: (err: Error, count: number) => void): void;
        inFlight(callback: (err: Error, count: number) => void): void;
        done(callback: (err: Error, count: number) => void): void;
    }

    function createQueue(mongoDbClient: MongoClient, name: string, opts?: QueueOptions): Queue;

    export = createQueue;

}
