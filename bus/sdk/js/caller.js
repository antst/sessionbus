// SPDX-License-Identifier: MIT

"use strict";

const ACTIONS = Object.freeze(["list", "send", "spawn", "describe", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack"]);
const ACTION_METHODS = Object.freeze({ list: "session.list", send: "message.send", spawn: "lane.spawn", describe: "lane.describe", run: "turn.run", start: "turn.start", status: "turn.status", wait: "turn.wait", interrupt: "turn.interrupt", close: "session.close", forget: "session.close" });

// Run references and results belong to the resident worker, not this caller.
class Caller {
  constructor(connection) { this.connection = connection; }
  list(request = {}, cancel) { return this.connection.call("session.list", request, cancel); }
  send(request, cancel) { return this.connection.call("message.send", request, cancel); }
  describe(request, cancel) { return this.connection.call("lane.describe", request, cancel); }
  spawn(request, cancel) { return this.connection.call("lane.spawn", request, cancel); }
  resume(sessionID, cancel) { return this.spawn({ resume_session_id: sessionID }, cancel); }
  run(request, cancel) { return this.connection.call("turn.run", request, cancel); }
  start(request, cancel) { return this.connection.call("turn.start", request, cancel); }
  status(request, cancel) { return this.connection.call("turn.status", request, cancel); }
  wait(request, cancel) { return this.connection.call("turn.wait", request, cancel); }
  interrupt(request, cancel) { return this.connection.call("turn.interrupt", request, cancel); }
  close(request, cancel) { return this.connection.call("session.close", request, cancel); }
  // Once submitted, acknowledgement settles consumption despite later abort.
  ack(request, cancel) {
    if (cancel?.aborted) return Promise.reject(cancel.reason || new Error("aborted"));
    return this.connection.call("turn.ack", request);
  }
  action(name, args = {}, cancel) {
    if (name === "ack") return this.ack(args, cancel);
    const method = ACTION_METHODS[name];
    if (method) return this.connection.call(method, name === "forget" ? { ...args, forget: true } : args, cancel);
    return Promise.reject(new Error("unknown action"));
  }
  disconnected() {}
}

module.exports = { ACTIONS, Caller };
