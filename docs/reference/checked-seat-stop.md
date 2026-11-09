# Checked seat stop (HTTP)

Read `GET /api/seat/stop-precondition/:seatRef` immediately before an operator-confirmed stop. A successful response has this shape:

```json
{"contract":"seat-stop.v1","expected":{"nodeId":"<node ID>","sessionId":"<latest session ID or null>"}}
```

Pass `expected` unchanged in the body of the existing `POST /api/seat/stop/:seatRef`, alongside `reason` and optional `operator`. The contract marker lets clients detect support. `expected` is optional; a request without it retains the existing stop behavior. A malformed explicit value returns HTTP 400. A null session ID can describe an empty seat, but stopping it returns `no_session`.

Checked stop requires the delivery guard. It refuses a changed node, latest session, binding or occupant before a terminal effect with HTTP 409 `selection_changed`. A kill failure or a failure to save stop state after a kill reports `stop_uncertain`; inspect the seat before retrying. Success retains the usual `seat`, `sessionName` and `sessionId` fields.

A `selection_changed` or `stop_uncertain` response from a checked stop says how far the stop got in `stopPhase`. A refusal before the stop starts carries neither `stopPhase` nor `partialEffect`, and nothing was sent. The stop detaches the seat's clients, checks the selection again, then kills the session:

| `stopPhase` | What was sent to the seat |
| --- | --- |
| `none` | Nothing. |
| `detach_attempted` | The client detach, which may not have finished. No kill. |
| `detached` | The client detach. No kill. |
| `kill_attempted` | The kill. |

`partialEffect` is `true` whenever `stopPhase` isn't `none`, so on its own it doesn't say whether the kill went out. `selection_changed` with `stopPhase: "kill_attempted"` is a change seen after the kill, when the stop state is saved: the session was killed, so re-read the seat rather than offering the stop again.

## Checked node removal

`DELETE /api/rigs/:rigId/nodes/:logicalId` accepts the same `expected` in an optional JSON body. A controller that stopped a seat with a checked stop passes the same `expected` to remove its node: the stop keeps the latest session ID. The removal checks it under the seat's lifecycle lease, before it reroutes, stops or deletes anything, and refuses a changed node or latest session with HTTP 409 `selection_changed`. A node that no longer exists returns `node_not_found`. A malformed explicit value returns HTTP 400 `invalid_stop_precondition`, and a body that isn't a JSON object returns HTTP 400 `invalid_json`; neither removes anything. Without `expected`, removal behaves as before.
