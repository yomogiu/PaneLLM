(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  BG.RELAY_POLL_TIMEOUT_MS = 25_000;
  BG.RELAY_INITIAL_BACKOFF_MS = 1_000;
  BG.RELAY_MAX_BACKOFF_MS = 15_000;
  BG.RELAY_COMMAND_LOOP_VERSION = "0.2.0";

  BG.relayLoopStarted = false;
  BG.relayBackoffMs = BG.RELAY_INITIAL_BACKOFF_MS;
  BG.relayClientId = `ext_${crypto.randomUUID()}`;

  async function startBrokerCommandLoop() {
    if (BG.relayLoopStarted) {
      return;
    }
    BG.relayLoopStarted = true;
    const relayRequestOptions = { suppressConnectionError: true };

    while (true) {
      try {
        await BG.brokerRequest("POST", "/extension/register", {
          client_id: BG.relayClientId,
          version: BG.RELAY_COMMAND_LOOP_VERSION,
          platform: "chrome-sidepanel"
        }, relayRequestOptions);

        const next = await BG.brokerRequest(
          "GET",
          `/extension/next?client_id=${encodeURIComponent(BG.relayClientId)}&timeout_ms=${BG.RELAY_POLL_TIMEOUT_MS}`,
          null,
          relayRequestOptions
        );

        if (next?.command) {
          await executeAndReportBrokerCommand(next.command);
        }

        BG.relayBackoffMs = BG.RELAY_INITIAL_BACKOFF_MS;
      } catch (error) {
        if (error?.code !== "broker_unreachable") {
          console.warn("[secure-panel] broker command loop error:", String(error?.message || error));
        }
        await BG.delay(BG.relayBackoffMs);
        BG.relayBackoffMs = Math.min(BG.relayBackoffMs * 2, BG.RELAY_MAX_BACKOFF_MS);
      }
    }
  }

  async function executeAndReportBrokerCommand(command) {
    const commandId = command?.command_id || command?.commandId;
    if (!commandId) {
      return;
    }

    let success = true;
    let data = null;
    let error = null;

    try {
      data = await BG.executeBrokerCommand(command.method, command.args || {});
    } catch (commandError) {
      success = false;
      error = {
        message: String(commandError?.message || commandError || "Command execution failed.")
      };
    }

    await BG.brokerRequest("POST", "/extension/result", {
      client_id: BG.relayClientId,
      command_id: commandId,
      success,
      data,
      error
    }, {
      suppressConnectionError: true
    });
  }

  BG.startBrokerCommandLoop = startBrokerCommandLoop;
  BG.executeAndReportBrokerCommand = executeAndReportBrokerCommand;
})();
