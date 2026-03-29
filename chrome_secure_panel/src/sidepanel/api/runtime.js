window.PLLM_SP = window.PLLM_SP || {};
const runtimeNamespace = window.PLLM_SP;

function sendRuntimeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(message, (response) => {
      const lastError = chrome.runtime.lastError;
      if (lastError) {
        reject(new Error(lastError.message));
        return;
      }
      if (!response) {
        reject(new Error("No response from background worker."));
        return;
      }
      resolve(response);
    });
  });
}

function runtimeMessage(type, payload = {}) {
  return sendRuntimeMessage({ type, ...payload });
}

runtimeNamespace.sendRuntimeMessage = sendRuntimeMessage;
runtimeNamespace.runtime = {
  sendRuntimeMessage,
  runtimeMessage,
};
