// SPDX-License-Identifier: MIT
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { Connection } = require("./connection.js");

class Stream extends EventEmitter {
  write(body, done) { this.writes ??= []; this.writes.push(JSON.parse(body)); done(); }
  destroy() {}
  reply(id, result) { this.emit("data", Buffer.from(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\n`)); }
}

test("cancelled correlations drain, suppress observers and retain the 256 bound", async () => {
 const stream = new Stream(), c = new Connection(stream, true);
 let observed = 0;
 for (let i = 0; i < 256; i++) {
  const control = new AbortController();
  const call = c.call("session.list", {}, control.signal, () => { observed++; });
  control.abort(); await assert.rejects(call, { name: "AbortError" });
 }
 await assert.rejects(c.call("session.list", {}), { code: -32003 });
 assert.equal(stream.writes.length, 256);
 for (const request of stream.writes) stream.reply(request.id, { sessions: [] });
 assert.equal(observed, 0); assert.equal(c.pending.size, 0); assert.equal(c.signal.aborted, false);
 const next = c.call("session.list", {}); stream.reply(257, { sessions: [] });
 assert.deepEqual(await next, { sessions: [] }); c.close();
});

test("malformed cancelled response still closes the connection", async () => {
 const stream = new Stream(), c = new Connection(stream, true), control = new AbortController();
 const call = c.call("session.list", {}, control.signal); control.abort();
 await assert.rejects(call); stream.reply(1, {}); assert.equal(c.signal.aborted, true);
});
