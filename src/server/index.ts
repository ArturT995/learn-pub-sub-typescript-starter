import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js"
import { ExchangePerilDirect, PauseKey, RoutingKey, ExchangePerilTopic, GameLogSlug } from "../internal/routing/routing.js"
import type { PlayingState } from "../internal/gamelogic/gamestate.js"
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js"
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/publish.js"


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
  

  const RouteKey = `${GameLogSlug}.${RoutingKey}`;
  await declareAndBind(
      conn,
      ExchangePerilTopic,
      GameLogSlug,
      RouteKey,
      SimpleQueueType.Durable,
    );
  

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
