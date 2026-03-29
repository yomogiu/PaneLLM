(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  BG.BROKER_URL = "http://127.0.0.1:7777";
  BG.BROKER_CLIENT_HEADER = "chrome-sidepanel-v1";

  async function brokerRequest(method, path, body = null, options = {}) {
    const headers = {
      "X-Assistant-Client": BG.BROKER_CLIENT_HEADER
    };
    if (body !== null) {
      headers["Content-Type"] = "application/json";
    }

    let response;
    try {
      response = await fetch(`${BG.BROKER_URL}${path}`, {
        method,
        headers,
        body: body === null ? undefined : JSON.stringify(body),
        signal: options.signal
      });
    } catch (error) {
      if (error?.name === "AbortError") {
        throw error;
      }
      const detail = String(error?.message || error || "unknown fetch failure");
      if (!options.suppressConnectionError) {
        console.error("[secure-panel] broker request failed to reach local broker:", {
          method,
          path,
          detail
        });
      }
      const message =
        `Could not reach the local broker at ${BG.BROKER_URL}${path}. `
        + "Make sure broker/local_broker.py is running and the extension can access localhost.";
      const wrapped = new Error(message);
      wrapped.code = "broker_unreachable";
      throw wrapped;
    }
    let parsed = null;
    try {
      parsed = await response.json();
    } catch {
      parsed = null;
    }
    if (!response.ok) {
      const detail = parsed?.error ? ` ${parsed.error}` : "";
      const error = new Error(`Broker request failed (${response.status}).${detail}`);
      if (parsed?.error_code) {
        error.code = String(parsed.error_code);
      }
      if (parsed?.error_data && typeof parsed.error_data === "object") {
        error.data = parsed.error_data;
      }
      throw error;
    }
    return parsed || {};
  }

  async function checkBrokerHealth() {
    return await brokerRequest("GET", "/health");
  }

  BG.brokerRequest = brokerRequest;
  BG.checkBrokerHealth = checkBrokerHealth;
})();
