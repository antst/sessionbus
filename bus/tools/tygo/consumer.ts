// SPDX-License-Identifier: MIT
import type { SessionListResult, SessionSelfInfo, WorkerHello, RPCError } from '@sessionbus/kit/protocol';
import { Caller } from '@sessionbus/kit/sdk/js/caller.js';

const self: SessionSelfInfo = {session_id: 'self@host', product: 'fixture', groups: []};
const legacy: SessionListResult = {sessions: []};
const current: SessionListResult = {sessions: [], self_info: self};
const worker: WorkerHello = {protocol: 1, launch_token: 'token', product: 'fixture', supported_open_fields: [], extra_arguments: []};
const error: RPCError = {code: -32603, message: 'internal', data: {arbitrary: [true]}};
void [legacy, current, worker, error];
// @ts-expect-error missing required identity field
const missing: SessionSelfInfo = {product: 'fixture', groups: []};
// @ts-expect-error self_info is absent or an identity; never null
const nullable: SessionListResult = {sessions: [], self_info: null};
// @ts-expect-error int64 wire values are JS numbers, not strings or bigint
const integer: import('@sessionbus/kit/protocol').WaitRequest = {session_id: 'x', timeout_ms: 1n};

async function consume(caller: Caller) {
 const result = await caller.list({host: 'other'});
 const identity: string | undefined = result.self_info?.session_id;
 // @ts-expect-error Caller.list returns the generated DTO, not any
 const impossible: number = result.self_info?.session_id;
 // @ts-expect-error no caller-supplied self identity
 await caller.list({self_info: self});
 // @ts-expect-error misspelled generated response field
 result.self_info?.sessionID;
 await caller.send({target: 'other', message: 'hello'});
 // @ts-expect-error request DTO requires message
 await caller.send({target: 'other'});
 const run = await caller.run({session_id: 'other', input: 'hello'});
 const state: string = run.state;
 const outcome: string | undefined = run.result?.outcome;
 // @ts-expect-error run returns a status record, not a direct TurnResult
 run.outcome;
 const resumed = await caller.resume('other');
 const resumedID: string = resumed.session_id;
 void [state, outcome, resumedID];
 return identity;
}
void consume;
