import amqp from "amqplib";
import { clientWelcome, 
  getInput, 
  printClientHelp,
  getMaliciousLog,
  printQuit,
  commandStatus,
} from "../internal/gamelogic/gamelogic.js"
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js"
import { ArmyMovesPrefix, 
  ExchangePerilDirect, 
  ExchangePerilTopic, 
  GameLogSlug, 
  PauseKey,
  WarRecognitionsPrefix, } from "../internal/routing/routing.js"
import { GameState } from "../internal/gamelogic/gamestate.js"
import { commandSpawn } from "../internal/gamelogic/spawn.js"
import { commandMove } from "../internal/gamelogic/move.js"
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { subscribeJSON, SimpleQueueType } from "../internal/pubsub/consume.js";
import type { GameLog } from "../internal/gamelogic/logs.js";



async function main() {
  console.log("Starting Peril client...");
  let cString = "amqp://guest:guest@localhost:5672/";

  
  const conn = await amqp.connect(cString);
  const publishCh = await conn.createConfirmChannel();
  const username = await clientWelcome();
  const gs = new GameState(username)
  
  //pause/resume
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );

  //movement
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, publishCh),
  );
  
  //war
    await subscribeJSON(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs, publishCh),
  );

  //REPL start
  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;
    const command = words[0];
    
    if (command === "spawn") {
      try {
        commandSpawn(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "move") {
      try {
        const move = commandMove(gs, words);
        publishJSON(
          publishCh, 
          ExchangePerilTopic, 
          `${ArmyMovesPrefix}.${username}`, 
          move
        );
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gs)
    } else if (command === "help") {
      printClientHelp()

    } else if (command === "spam") {
      if (words.length < 2) {
      console.log("usage: spam <number>");
      continue;
      }
       const n = Number(words[1]);
      if (Number.isNaN(n)) {
        console.log("usage: spam <n>");
        continue;
      }
        for (let i = 0; i < n; i++) {
        const log = getMaliciousLog();
        publishGameLog(publishCh, username, log)
      }
      console.log(`Published ${n} malicious logs!!`)
    } else if (command === "quit") {
      printQuit()
      process.exit(0);
    } else {
      console.log("Invalid command")
      continue;
    }
  }

};


export async function publishGameLog(
    ch: amqp.ConfirmChannel,
    username: string,
    message: string,
    ): Promise<void> {
      const log: GameLog = {
      username,
      message,
      currentTime: new Date(),
    };
    
    return publishMsgPack(
      ch,
      ExchangePerilTopic,
      `${GameLogSlug}.${username}`,
      log,
    );
  }


main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

