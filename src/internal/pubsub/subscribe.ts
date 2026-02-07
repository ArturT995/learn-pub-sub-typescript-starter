import amqp from "amqplib";
import { SimpleQueueType, declareAndBind } from "./publish.js";
import { ExchangePerilDirect, PauseKey } from "../routing/routing.js";




export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType, // durable || transient enum
    handler: (data: T) => void,
): Promise<void> {
    const [ch, queue] = await declareAndBind(
        conn,
        exchange,
        queueName,
        key,
        queueType,
      );

    if (!queue) throw new Error("queue not found");
    
    ch.consume(queue.queue, 
        function callback(msg: amqp.ConsumeMessage | null) {
            if (!msg) return;
            const body = msg.content.toString()
            const message = JSON.parse(body)
            handler(message)
            ch.ack(msg)
        }
    )
}


