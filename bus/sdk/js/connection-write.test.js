// SPDX-License-Identifier: MIT
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter, once } = require("node:events");
const { setImmediate: nextTurn } = require("node:timers/promises");
const { mkdtemp, rm } = require("node:fs/promises");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const { Connection } = require("./connection.js");

function deferred() { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; }
class HeldStream extends EventEmitter {
  callbacks = [];
  destroy() { this.destroyed = true; }
  write(body, callback) { this.callbacks.push(callback); return false; }
}
const request = { id: 1, method: "session.close" };

for (const method of ["session.hello", "session.list"]) test(`actual socket close settles ${method} with withheld completed-write callback`, { timeout: 5000 }, async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "kit-write-"));
  const received = deferred(), written = deferred();
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket); socket.once("close", () => sockets.delete(socket));
    new Connection(socket, false, value => { assert.equal(value.method, method); received.resolve(); });
  });
  let client, connection, held, result;
  t.after(async () => {
    held?.(); connection?.close(); client?.destroy(); await result;
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  server.listen(path.join(directory, "bus")); await once(server, "listening");
  client = net.createConnection(path.join(directory, "bus")); await once(client, "connect");
  const write = client.write.bind(client);
  client.write = (body, callback) => write(body, error => { held = () => callback(error); written.resolve(); });
  connection = new Connection(client, true);
  let settled = false;
  result = connection.call(method, method === "session.hello" ? { protocol: 1, session_id: "ses_review", product: "example", groups: ["review"], info: {} } : {}).then(value => ({ value }), error => ({ error })).then(value => { settled = true; return value; });
  await Promise.all([received.promise, written.promise]);
  const closed = once(client, "close"); connection.close(new Error("owned shutdown")); await closed; await nextTurn();
  const atClose = settled;
  held(); // Failure cleanup cannot strand the old-source Call.
  assert.match((await result).error.message, /owned shutdown/);
  assert.equal(atClose, true, "actual close must settle Call before the missing callback is released");
  assert.equal(connection.writes.size, 0); assert.equal(connection.writeBytes, 0);
});

test("close settles response writes once, while destroy alone keeps ownership", async () => {
  const stream = new HeldStream(), connection = new Connection(stream, false);
  let settled = 0;
  const results = [connection.result(request, {}), connection.error(request, -32603, "failure")].map(p => p.then(() => { throw new Error("unexpected success"); }, error => { settled++; return error; }));
  const bytes = connection.writeBytes;
  assert.ok(bytes > 0); assert.equal(connection.writes.size, 2);
  const cause = new Error("owner closed"); connection.close(cause); await nextTurn();
  assert.equal(stream.destroyed, true); assert.equal(settled, 0); assert.equal(connection.writeBytes, bytes);
  stream.emit("close");
  assert.deepEqual(await Promise.all(results), [cause, cause]);
  for (const done of stream.callbacks) { done(new Error("late error")); done(); }
  stream.emit("close");
  assert.equal(settled, 2); assert.equal(connection.writes.size, 0); assert.equal(connection.writeBytes, 0);
  assert.equal(connection.signal.reason, cause);
});

test("write success is final and later callback errors cannot close a healthy connection", async () => {
  const stream = new HeldStream(), connection = new Connection(stream, false);
  const result = connection.result(request, {});
  stream.callbacks[0](); await result;
  stream.callbacks[0](new Error("late error"));
  assert.equal(connection.signal.aborted, false); assert.equal(connection.writes.size, 0); assert.equal(connection.writeBytes, 0);
  connection.close(); stream.emit("close");
});

test("synchronous write throw releases its reservation and keeps original failure", async () => {
  const stream = new HeldStream(), failure = new Error("write throw");
  stream.write = () => { throw failure; };
  const connection = new Connection(stream, false);
  await assert.rejects(connection.result(request, {}), error => error === failure);
  assert.equal(connection.signal.reason, failure); assert.equal(connection.writes.size, 0); assert.equal(connection.writeBytes, 0);
  stream.emit("close");
});

test("outstanding response writes have their own 256 bound and callback releases capacity", async () => {
  const stream = new HeldStream(), connection = new Connection(stream, false);
  const writes = Array.from({ length: 256 }, () => connection.result(request, {}));
  assert.equal(connection.pending.size, 0);
  await assert.rejects(connection.result(request, {}), { code: -32003 });
  assert.equal(stream.callbacks.length, 256); assert.equal(connection.signal.aborted, false);
  stream.callbacks[0](); await writes[0];
  writes.push(connection.result(request, {}));
  for (const done of stream.callbacks) done(); await Promise.all(writes);
  assert.equal(connection.writes.size, 0); assert.equal(connection.writeBytes, 0);
  connection.close(); stream.emit("close");
});

test("encoded bytes independently bound retained writes, then release for healthy work", async () => {
  const stream = new HeldStream(), connection = new Connection(stream, false);
  const data = "<".repeat(900000);
  const writes = [];
  // Each error is schema-valid and under the existing 1 MiB wire-frame limit.
  for (let i = 0; i < 37; i++) writes.push(connection.error(request, -32603, data));
  assert.ok(connection.writeBytes <= 32 * (1 << 20)); assert.ok(connection.writes.size < 256);
  await assert.rejects(connection.error(request, -32603, data), { code: -32003 });
  assert.equal(stream.callbacks.length, 37); assert.equal(connection.signal.aborted, false);
  for (const done of stream.callbacks) done(); await Promise.all(writes);
  const small = connection.result(request, {}); stream.callbacks.at(-1)(); await small;
  assert.equal(connection.writeBytes, 0); assert.equal(connection.writes.size, 0);
  connection.close(); stream.emit("close");
});
