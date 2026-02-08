import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js"
import { ExchangePerilDirect, PauseKey, ExchangePerilTopic, GameLogSlug } from "../internal/routing/routing.js"
import type { PlayingState } from "../internal/gamelogic/gamestate.js"
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js"
import { SimpleQueueType, subscribeMsgPack } from "../internal/pubsub/consume.js"
import { handlerLog } from "./handlers.js";


async function main() {
  
  console.log("Starting Peril server...");
  
  let cString = "amqp://guest:guest@localhost:5672/";
  
  const conn = await amqp.connect(cString);
  console.log("connection was succesful")
  
  const confirm = await conn.createConfirmChannel();
  
  process.on('SIGINT', () => {
    console.log(`Shutting down...`);
    conn.close();
    process.exit(0);
  });

  const state: PlayingState = {
  isPaused: true,
  };
  

  const publishCh = await conn.createConfirmChannel();

  subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    handlerLog(),
  );
  
  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }
  
  printServerHelp();

  while (true) {
    const input = await getInput();
    if (input.length === 0) continue;
      
    else if (input[0] === "pause") {
      console.log("Sending pause message.")
      state.isPaused = true;
      await publishJSON(confirm, ExchangePerilDirect, PauseKey, state);
    }
    else if (input[0] === "resume") {
      console.log("Sending resume message.")
      state.isPaused = false
      await publishJSON(confirm, ExchangePerilDirect, PauseKey, state);
    }
    else if (input[0] === "quit") {
      console.log("Exiting.")
      process.exit(0);
    }
    else {
      console.log("Invalid command")
      continue
    }
  };
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
