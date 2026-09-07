# fastrtc

Browser WebRTC mesh helper. Zero runtime dependencies.

You wire signaling. fastrtc connects peers and opens labeled data channels. Each channel tracks SCTP `maxMessageSize` and applies send backpressure.

## Mental model

```
You                          fastrtc                         Remote peer
 │                              │                                  │
 │  signal(to, payload) ───────►│ forwards SDP / ICE               │
 │◄──── receive(from, payload) ──│                                  │
 │                              │◄──────── WebRTC ────────────────►│
 │  channel("chat")             │ opens labeled data channels      │
 │  connect("bob")              │ negotiates connection            │
 │  peer.ready                  │ declared channels open           │
 │  send / sendTo / ch.send     │ string or BufferSource           │
```

1. **Signaling is yours** — forward `SignalPayload` objects. fastrtc never touches your server.
2. **Declare channels before `connect()`** — `channel("chat")` registers a label for every current and future peer.
3. **`peer.ready` means go** — wait for it (or `peer.open(label)`) before sending.

## Install

```bash
pnpm add fastrtc
```

## Quick start

```ts
import { FastRTC } from "fastrtc";
import type { SignalPayload } from "fastrtc";

const rtc = new FastRTC({
  id: "alice",
  signal: (to, payload) => {
    ws.send(JSON.stringify({ to, from: "alice", payload }));
  },
});

ws.onmessage = (e) => {
  const { from, payload } = JSON.parse(e.data);
  rtc.receive(from, payload as SignalPayload);
};

rtc.channel("chat", { ordered: true });

rtc.addEventListener("join", ({ peer }) => {
  void peer.ready.then(() => {
    void peer.channels.get("chat")!.send("hello");
  });
});

const bob = rtc.connect("bob");
await bob.ready;
const chat = bob.channels.get("chat")!;
chat.addEventListener("message", ({ data }) => console.log(data));
await chat.send("hi");
await rtc.send("chat", "hello all");
await rtc.sendTo("bob", "chat", "hello bob");
```

## Signaling

```ts
type SignalPayload =
  | { type: "description"; description: RTCSessionDescriptionInit }
  | { type: "candidate"; candidate: RTCIceCandidateInit };
```

- `signal(to, payload)` — fastrtc needs to send SDP or ICE to `to`
- `receive(from, payload)` — apply a remote signal (auto-connects, returns `Peer`)

**Offer glare:** peer with the lexicographically larger ID is *polite* and yields on collision. Only the *impolite* peer creates labeled data channels; the polite peer waits for `ondatachannel`.

## Channels

```ts
rtc.channel("chat", { ordered: true });
rtc.channel("files");

const bob = rtc.connect("bob");
await bob.ready;
const chat = bob.channels.get("chat")!;
await chat.send("hello");
```

`Channel.send` waits until `bufferedAmount` is under the watermark (`maxMessageSize * 2`), then hands the payload to SCTP. Parallel sends on the same channel queue. Payload larger than `channel.maxMessageSize` throws — caller splits. `maxMessageSize` comes from `pc.sctp.maxMessageSize` after the channel opens (fallback `65536`).

`string | BufferSource` only. No framing, no Blob/stream helpers.

Late labels after `ready`: `await bob.open("files")`.

## Broadcast

| Method | Target |
| ------ | ------ |
| `send(label, data)` | Every peer with that label open |
| `sendTo(peerId, label, data)` | One peer |
| `channel.send(data)` | Channel you already have |

No open targets on `send` → resolves, does not throw. Both honor `{ signal?: AbortSignal }`.

## Events

`FastRTC` is an `EventTarget`:

| Event | Payload |
| ----- | ------- |
| `join` | `peer` |
| `leave` | `id` |
| `error` | `id`, `error` |

`Channel` events: `message` (`data`), `close`, `error`.

ICE `disconnected` does not tear down (can recover). `failed` / `closed` emit `leave`.

## Media

No media helpers. Use the raw connection:

```ts
bob.connection.addTrack(track, stream);
bob.connection.ontrack = (e) => {
  video.srcObject = e.streams[0];
};
```

## API

### `FastRTC`

```ts
new FastRTC({
  id: string;
  signal: (to: string, payload: SignalPayload) => void;
  rtcConfig?: RTCConfiguration; // default: Google STUN
})
```

| Method | Description |
| ------ | ----------- |
| `channel(label, options?)` | Register a labeled channel for all peers |
| `connect(id, { signal? })` | Open connection; idempotent; returns `Peer` |
| `receive(id, payload)` | Apply remote SDP/ICE; auto-connects; returns `Peer` |
| `get(id)` | `Peer` or `undefined` |
| `send(label, data, options?)` | Broadcast to every open channel on `label` |
| `sendTo(peerId, label, data, options?)` | Send to one peer |
| `close(id)` | Tear down one peer |
| `dispose()` / `[Symbol.dispose]()` | Close everything |

| Property | Description |
| -------- | ----------- |
| `id` | Local peer ID |
| `rtcConfig` | Active RTC configuration |
| `peers` | `ReadonlyMap<string, Peer>` |

### `Peer`

| Method / property | Description |
| ----------------- | ----------- |
| `id` | Remote peer ID |
| `connection` | Underlying `RTCPeerConnection` |
| `channels` | `ReadonlyMap<string, Channel>` |
| `ready` | Resolves when every declared label is open (or ICE connected if none) |
| `open(label, { signal? })` | Promise for one open channel |
| `close()` | Tear down this peer |

### `Channel`

| Method / property | Description |
| ----------------- | ----------- |
| `label` / `readyState` / `maxMessageSize` | Channel state |
| `raw` | Underlying `RTCDataChannel` |
| `ready` | Resolves when open and SCTP size applied |
| `send(data, { signal? })` | Backpressured send; throws if oversized |
| `close()` / `[Symbol.dispose]()` | Close the channel |

## Build

```bash
pnpm build   # output in dist/
```

## Requirements

- Browser with `RTCPeerConnection` and `RTCDataChannel`
- A signaling transport you control

## Source Code
Since this plugin is MIT licensed, you can also contribute to it at it's repo on [GitHub](https://github.com/YSpoof/fastrtc)
