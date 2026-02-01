import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js"
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js"
import type { PlayingState } from "../internal/gamelogic/gamestate.js"
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js"

async function main() {
  
  console.log("Starting Peril server...");
  let cString = "amqp://guest:guest@localhost:5672/";
  
  const conn = await amqp.connect(cString);
  console.log("connection was succesful")
  
  const confirm = await conn.createConfirmChannel();

  const state: PlayingState = {
  isPaused: true,
  };

  const gamestate = await publishJSON(
    confirm,
    ExchangePerilDirect,
    PauseKey,
    state,
  )

  printServerHelp();

  let shouldRun = 0;

  while (shouldRun < 1) {
    const input = await getInput();
    if (input.length === 0){
      continue
    }
    else if (input[0] === "pause") {
      console.log("Sending pause message.")
      gamestate
    }
    else if (input[0] === "resume") {
      console.log("Sending resume message.")
      state.isPaused = false
      gamestate
    }
    else if (input[0] === "quit") {
      console.log("Exiting.")
      break
    }
    else {
      console.log("Invalid command")
      continue
    }

  };

  process.on('SIGINT', () => {
    console.log(`Shutting down...`);
    conn.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
