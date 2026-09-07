import { type SendOptions } from "./channel";
import { DEFAULT_RTC_CONFIG } from "./consts";
import { Peer, type SignalPayload } from "./peer";

export class JoinEvent extends Event {
  constructor(readonly peer: Peer) {
    super("join");
  }
}

export class LeaveEvent extends Event {
  constructor(readonly id: string) {
    super("leave");
  }
}

export class ErrorEvent extends Event {
  constructor(
    readonly id: string,
    readonly error: unknown,
  ) {
    super("error");
  }
}

interface FastRTCEventMap {
  join: JoinEvent;
  leave: LeaveEvent;
  error: ErrorEvent;
}

interface FastRTCOptions {
  id: string;
  signal: (to: string, payload: SignalPayload) => void;
  rtcConfig?: RTCConfiguration;
}

export class FastRTC extends EventTarget {
  readonly id: string;
  readonly rtcConfig: RTCConfiguration;

  #peers = new Map<string, Peer>();
  #signal: (to: string, payload: SignalPayload) => void;
  #channelConfigs = new Map<string, RTCDataChannelInit>();
  #aborts = new Map<string, () => void>();
  #destroyed = false;

  constructor({ id, signal, rtcConfig = DEFAULT_RTC_CONFIG }: FastRTCOptions) {
    super();
    this.id = id;
    this.#signal = signal;
    this.rtcConfig = rtcConfig;
  }

  get peers(): ReadonlyMap<string, Peer> {
    return this.#peers;
  }

  get(id: string): Peer | undefined {
    return this.#peers.get(id);
  }

  channel(label: string, options: RTCDataChannelInit = {}): void {
    this.#assertAlive();
    this.#channelConfigs.set(label, options);
    for (const peer of this.#peers.values()) {
      peer.createDataChannel(label, options);
    }
  }

  connect(remoteId: string, options: { signal?: AbortSignal } = {}): Peer {
    this.#assertAlive();
    if (remoteId === this.id) throw new Error("Cannot connect to self");
    const existing = this.#peers.get(remoteId);
    if (existing) return existing;
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Aborted");
    }

    const peer = new Peer({
      localId: this.id,
      remoteId,
      rtcConfig: this.rtcConfig,
      onSignal: (payload) => this.#signal(remoteId, payload),
      onError: (error) => this.dispatchEvent(new ErrorEvent(remoteId, error)),
      onDisconnect: () => this.close(remoteId),
    });

    this.#channelConfigs.forEach((channelOptions, label) => {
      peer.createDataChannel(label, channelOptions);
    });

    this.#peers.set(remoteId, peer);

    const signal = options.signal;
    if (signal) {
      const onAbort = () => this.close(remoteId);
      signal.addEventListener("abort", onAbort);
      this.#aborts.set(remoteId, () => signal.removeEventListener("abort", onAbort));
    }

    this.dispatchEvent(new JoinEvent(peer));
    return peer;
  }

  receive(remoteId: string, payload: SignalPayload): Peer {
    this.#assertAlive();
    const peer = this.connect(remoteId);
    peer.receiveSignal(payload);
    return peer;
  }

  async send(label: string, data: string | BufferSource, options: SendOptions = {}): Promise<void> {
    this.#assertAlive();
    const sends: Promise<void>[] = [];
    for (const peer of this.#peers.values()) {
      const channel = peer.channels.get(label);
      if (channel?.readyState === "open") sends.push(channel.send(data, options));
    }
    if (sends.length === 0) return;
    await Promise.all(sends);
  }

  async sendTo(
    peerId: string,
    label: string,
    data: string | BufferSource,
    options: SendOptions = {},
  ): Promise<void> {
    this.#assertAlive();
    const peer = this.#peers.get(peerId);
    if (!peer) throw new Error(`Unknown peer: ${peerId}`);
    const channel = peer.channels.get(label);
    if (!channel) throw new Error(`No channel "${label}" for peer ${peerId}`);
    if (channel.readyState !== "open") {
      throw new Error(`Channel "${label}" with ${peerId} is not open`);
    }
    await channel.send(data, options);
  }

  close(remoteId: string): void {
    const peer = this.#peers.get(remoteId);
    if (!peer) return;
    this.#peers.delete(remoteId);
    this.#aborts.get(remoteId)?.();
    this.#aborts.delete(remoteId);
    peer.close();
    this.dispatchEvent(new LeaveEvent(remoteId));
  }

  dispose(): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    for (const id of [...this.#peers.keys()]) {
      this.close(id);
    }
    this.#channelConfigs.clear();
  }

  [Symbol.dispose](): void {
    this.dispose();
  }

  addEventListener<K extends keyof FastRTCEventMap>(
    type: K,
    listener: (this: FastRTC, ev: FastRTCEventMap[K]) => void,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    super.addEventListener(type, listener, options);
  }

  removeEventListener<K extends keyof FastRTCEventMap>(
    type: K,
    listener: (this: FastRTC, ev: FastRTCEventMap[K]) => void,
    options?: boolean | EventListenerOptions,
  ): void;
  removeEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void;
  removeEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    super.removeEventListener(type, listener, options);
  }

  #assertAlive(): void {
    if (this.#destroyed) throw new Error("FastRTC disposed");
  }
}
