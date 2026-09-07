import { SCTP_DEFAULT_MESSAGE_SIZE } from "./consts";
import { createQueue } from "./queue";

export type SendOptions = { signal?: AbortSignal };

export class ChannelErrorEvent extends Event {
  constructor(readonly error: unknown) {
    super("error");
  }
}

interface ChannelEventMap {
  message: MessageEvent;
  close: Event;
  error: ChannelErrorEvent;
}

function payloadSize(data: string | BufferSource): number {
  if (typeof data === "string") return new TextEncoder().encode(data).byteLength;
  return data.byteLength;
}

function maxMessageSize(sctp: RTCSctpTransport | null): number {
  const max = sctp?.maxMessageSize;
  if (!max || !Number.isFinite(max)) return SCTP_DEFAULT_MESSAGE_SIZE;
  return max;
}

export class Channel extends EventTarget {
  readonly raw: RTCDataChannel;
  readonly ready: Promise<Channel>;

  #pc: RTCPeerConnection;
  #maxMessageSize = SCTP_DEFAULT_MESSAGE_SIZE;
  #closed = false;
  #readySettled = false;
  #opened = Promise.withResolvers<Channel>();
  #drain: PromiseWithResolvers<void> | null = null;
  #enqueue = createQueue();

  constructor(channel: RTCDataChannel, pc: RTCPeerConnection) {
    super();
    this.raw = channel;
    this.#pc = pc;
    this.ready = this.#opened.promise;
    this.ready.catch(() => {});

    this.raw.binaryType = "arraybuffer";
    this.#applySize();

    this.raw.onopen = () => this.#onOpen();
    this.raw.onclose = () => this.close();
    this.raw.onmessage = (e) => {
      this.dispatchEvent(new MessageEvent("message", { data: e.data }));
    };
    this.raw.onerror = (e) => {
      this.dispatchEvent(new ChannelErrorEvent(e));
    };
    this.raw.onbufferedamountlow = () => this.#notifyDrain();

    if (this.raw.readyState === "open") {
      queueMicrotask(() => this.#onOpen());
    }
  }

  get label(): string {
    return this.raw.label;
  }

  get readyState(): RTCDataChannelState {
    return this.raw.readyState;
  }

  get maxMessageSize(): number {
    return this.#maxMessageSize;
  }

  async send(data: string | BufferSource, options: SendOptions = {}): Promise<void> {
    if (this.#closed || this.raw.readyState !== "open") {
      throw new Error("Data channel closed");
    }
    if (payloadSize(data) > this.#maxMessageSize) {
      throw new Error(`Message exceeds max SCTP size (${this.#maxMessageSize})`);
    }
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Aborted");
    }

    await this.#enqueue(async () => {
      if (this.#closed || this.raw.readyState !== "open") {
        throw new Error("Data channel closed");
      }
      if (options.signal?.aborted) {
        throw options.signal.reason ?? new Error("Aborted");
      }
      await this.#waitForDrain(options.signal);
      if (this.#closed || this.raw.readyState !== "open") {
        throw new Error("Data channel closed");
      }
      if (options.signal?.aborted) {
        throw options.signal.reason ?? new Error("Aborted");
      }
      if (typeof data === "string") {
        this.raw.send(data);
        return;
      }
      if (data instanceof ArrayBuffer) {
        this.raw.send(data);
        return;
      }
      this.raw.send(data);
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#drain?.reject(new Error("Data channel closed"));
    this.#drain = null;
    if (!this.#readySettled) {
      this.#readySettled = true;
      this.#opened.reject(new Error("Data channel closed"));
    }
    this.dispatchEvent(new Event("close"));
    this.raw.onopen = null;
    this.raw.onclose = null;
    this.raw.onmessage = null;
    this.raw.onerror = null;
    this.raw.onbufferedamountlow = null;
    try {
      this.raw.close();
    } catch {}
  }

  [Symbol.dispose](): void {
    this.close();
  }

  addEventListener<K extends keyof ChannelEventMap>(
    type: K,
    listener: (this: Channel, ev: ChannelEventMap[K]) => void,
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

  removeEventListener<K extends keyof ChannelEventMap>(
    type: K,
    listener: (this: Channel, ev: ChannelEventMap[K]) => void,
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

  #onOpen(): void {
    if (this.#closed || this.#readySettled) return;
    this.#readySettled = true;
    this.#applySize();
    this.#notifyDrain();
    this.#opened.resolve(this);
  }

  #applySize(): void {
    this.#maxMessageSize = maxMessageSize(this.#pc.sctp ?? null);
    this.raw.bufferedAmountLowThreshold = this.#maxMessageSize * 2;
  }

  #readyToSend(): boolean {
    return (
      !this.#closed &&
      this.raw.readyState === "open" &&
      this.raw.bufferedAmount <= this.raw.bufferedAmountLowThreshold
    );
  }

  #waitForDrain(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("Data channel closed"));
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("Aborted"));
    if (this.#readyToSend()) return Promise.resolve();

    if (!this.#drain) this.#drain = Promise.withResolvers();
    const { promise } = this.#drain;

    const onAbort = () => {
      this.#drain?.reject(signal?.reason ?? new Error("Aborted"));
      this.#drain = null;
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    return promise.finally(() => {
      signal?.removeEventListener("abort", onAbort);
    });
  }

  #notifyDrain(): void {
    if (!this.#readyToSend()) return;
    this.#drain?.resolve();
    this.#drain = null;
  }
}
