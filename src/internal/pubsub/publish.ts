import type { ConfirmChannel} from "amqplib";
import amqp, { type Channel }  from "amqplib";




export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const content = Buffer.from(JSON.stringify(value));

  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/json" },
      (err) => {
        if (err) return reject(err);
        resolve();
      },
    );
  });
}

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  
  const ch = await conn.createChannel()
  
  const queue = await ch.assertQueue(queueName, {
  durable: queueType === SimpleQueueType.Durable,
  autoDelete: queueType === SimpleQueueType.Transient,
  exclusive: queueType === SimpleQueueType.Transient,
  });

  
  ch.bindQueue(queue.queue, exchange, key)

  return [ch, queue]


};