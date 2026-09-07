import { Channel } from "./channel";
import { createQueue } from "./queue";

export type SignalPayload =
  | { type: "description"; description: RTCSessionDescriptionInit }
  | { type: "candidate"; candidate: RTCIceCandidateInit };

type PeerOptions = {
  localId: string;
  remoteId: string;
  rtcConfig: RTCConfiguration;
  onSignal: (payload: SignalPayload) => void;
  onError: (error: unknown) => void;
  onDisconnect: () => void;
};

function isGlareError(err: unknown): boolean {
  return err instanceof DOMException && err.name === "InvalidStateError";
}

export class Peer {
  readonly id: string;
  readonly ready: Promise<Peer>;
  readonly connection: RTCPeerConnection;

  #channels = new Map<string, Channel>();
  #expected = new Set<string>();
  #opts: PeerOptions;
  #isPolite: boolean;
  #candidateQueue: RTCIceCandidateInit[] = [];
  #makingOffer = false;
  #ignoreOffer = false;
  #closed = false;
  #readySettled = false;
  #opened = Promise.withResolvers<Peer>();
  #openChecks = new Set<(error?: unknown) => void>();
  #enqueue = createQueue();

  constructor(opts: PeerOptions) {
    this.id = opts.remoteId;
    this.#opts = opts;
    this.#isPolite = opts.localId > opts.remoteId;
    this.connection = new RTCPeerConnection(opts.rtcConfig);
    this.ready = this.#opened.promise;
    this.ready.catch(() => {});

    this.connection.onicecandidate = (e) => {
      if (this.#closed || !e.candidate) return;
      this.#opts.onSignal({ type: "candidate", candidate: e.candidate });
    };

    this.connection.onnegotiationneeded = () => {
      void this.#enqueue(async () => {
        if (this.#closed) return;
        try {
          this.#makingOffer = true;
          await this.connection.setLocalDescription();
          if (!this.#closed && this.connection.localDescription) {
            this.#opts.onSignal({
              type: "description",
              description: this.connection.localDescription,
            });
          }
        } catch (err) {
          if (!isGlareError(err)) this.#opts.onError(err);
        } finally {
          this.#makingOffer = false;
        }
      });
    };

    this.connection.onconnectionstatechange = () => {
      if (this.connection.connectionState === "connected") {
        this.#tryReady();
      }
      if (
        this.connection.connectionState === "failed" ||
        this.connection.connectionState === "closed"
      ) {
        if (this.#closed) return;
        this.#opts.onDisconnect();
      }
    };

    this.connection.ondatachannel = (e) => this.#wrapChannel(e.channel);
  }

  get channels(): ReadonlyMap<string, Channel> {
    return this.#channels;
  }

  createDataChannel(label: string, options?: RTCDataChannelInit): void {
    this.#expected.add(label);
    if (!this.#isPolite && !this.#channels.has(label)) {
      this.#wrapChannel(this.connection.createDataChannel(label, options));
    }
    this.#tryReady();
  }

  receiveSignal(signal: SignalPayload): void {
    void this.#enqueue(async () => {
      if (this.#closed) return;
      try {
        if (signal.type === "description") {
          const offerCollision =
            signal.description.type === "offer" &&
            (this.#makingOffer || this.connection.signalingState !== "stable");

          this.#ignoreOffer = !this.#isPolite && offerCollision;
          if (this.#ignoreOffer) return;

          await this.connection.setRemoteDescription(signal.description);

          for (const candidate of this.#candidateQueue) {
            await this.connection.addIceCandidate(candidate);
          }
          this.#candidateQueue = [];

          if (signal.description.type === "offer") {
            await this.connection.setLocalDescription();
            if (!this.#closed && this.connection.localDescription) {
              this.#opts.onSignal({
                type: "description",
                description: this.connection.localDescription,
              });
            }
          }
          return;
        }

        if (this.#ignoreOffer) return;

        if (!this.connection.remoteDescription) {
          this.#candidateQueue.push(signal.candidate);
        } else {
          await this.connection.addIceCandidate(signal.candidate);
        }
      } catch (err) {
        if (!this.#ignoreOffer && !isGlareError(err)) this.#opts.onError(err);
      }
    });
  }

  open(label: string, options: { signal?: AbortSignal } = {}): Promise<Channel> {
    if (this.#closed) return Promise.reject(new Error("Peer closed"));
    if (options.signal?.aborted) {
      return Promise.reject(options.signal.reason ?? new Error("Aborted"));
    }

    const existing = this.#channels.get(label);
    if (existing?.readyState === "open") return Promise.resolve(existing);

    const { promise, resolve, reject } = Promise.withResolvers<Channel>();
    let settled = false;

    const finish = (error?: unknown, channel?: Channel) => {
      if (settled) return;
      settled = true;
      this.#openChecks.delete(check);
      options.signal?.removeEventListener("abort", onAbort);
      if (channel) resolve(channel);
      else reject(error ?? new Error("Channel open failed"));
    };

    const check = (error?: unknown) => {
      if (error) {
        finish(error);
        return;
      }
      const channel = this.#channels.get(label);
      if (channel?.readyState === "open") finish(undefined, channel);
    };

    const onAbort = () => {
      finish(options.signal?.reason ?? new Error("Aborted"));
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    this.#openChecks.add(check);
    check();

    return promise;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#failWaiters(new Error("Peer closed"));
    this.connection.onicecandidate = null;
    this.connection.onnegotiationneeded = null;
    this.connection.onconnectionstatechange = null;
    this.connection.ondatachannel = null;
    for (const ch of this.#channels.values()) {
      ch.close();
    }
    this.#channels.clear();
    try {
      this.connection.close();
    } catch {}
  }

  #wrapChannel(raw: RTCDataChannel): void {
    const prev = this.#channels.get(raw.label);
    const channel = new Channel(raw, this.connection);
    this.#channels.set(raw.label, channel);
    prev?.close();
    void channel.ready.then(
      () => {
        this.#notifyOpenChecks();
        this.#tryReady();
      },
      () => this.#notifyOpenChecks(),
    );
    this.#notifyOpenChecks();
  }

  #allOpen(): boolean {
    if (this.#expected.size === 0) return this.connection.connectionState === "connected";
    for (const label of this.#expected) {
      const channel = this.#channels.get(label);
      if (!channel || channel.readyState !== "open") return false;
    }
    return true;
  }

  #tryReady(): void {
    if (this.#readySettled || this.#closed) return;
    if (!this.#allOpen()) return;
    this.#readySettled = true;
    this.#opened.resolve(this);
  }

  #notifyOpenChecks(): void {
    const error = this.#closed ? new Error("Peer closed") : undefined;
    for (const check of [...this.#openChecks]) check(error);
  }

  #failWaiters(error: unknown): void {
    if (!this.#readySettled) {
      this.#readySettled = true;
      this.#opened.reject(error);
    }
    for (const check of [...this.#openChecks]) check(error);
  }
}
