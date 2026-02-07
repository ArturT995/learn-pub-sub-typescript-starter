import amqp from "amqplib";
import { clientWelcome, 
  getInput, 
  printClientHelp,
  getMaliciousLog, //TBA
  printQuit,
  commandStatus,
} from "../internal/gamelogic/gamelogic.js"
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/publish.js"
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js"
import { GameState } from "../internal/gamelogic/gamestate.js"
import { commandSpawn } from "../internal/gamelogic/spawn.js"
import { commandMove } from "../internal/gamelogic/move.js"
import { handlerPause } from "./handlers.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";



async function main() {
  console.log("Starting Peril client...");
  let cString = "amqp://guest:guest@localhost:5672/";

  
  const conn = await amqp.connect(cString);
  const username = await clientWelcome();
  const queueName = `${PauseKey}.${username}`;
  
  

  await declareAndBind(
    conn,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    SimpleQueueType.Transient,
  );


  const gs = new GameState(username)
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );


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
        commandMove(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gs)
    } else if (command === "help") {
      printClientHelp()

    } else if (command === "spam") {
      console.log("Spamming not allowed yet!")
      //getMaliciousLog()
    } else if (command === "quit") {
      printQuit()
      process.exit(0);
    } else {
      console.log("Invalid command")
      continue;
    }
  }

};



main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
