import { EventEmitter } from 'node:events';
import { TokenManager } from './auth';
import { TendClient } from './cxs';
import { IDR_NAL, KeyframeGate, PPS_NAL, SPS_NAL, nals } from './h264';
import { P2PMediaSession } from './p2p';
import type { Camera, ConnectionInfo, PluginLogger } from './types';

export interface CameraSessionOptions {
  camera?: string;
  audio?: boolean;
  quality?: number;
  fps?: number;
  size?: number;
  videoPunchTimeoutMs?: number;
  videoPunchAttempts?: number;
  videoPunchRetryDelayMs?: number;
  releaseCooldownMs?: number;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function seconds(ms: number): string {
  return `${(ms / 1000).toFixed(ms % 1000 === 0 ? 0 : 1)}s`;
}

export class TendConnectionManager {
  private client?: TendClient;
  private pending?: Promise<TendClient>;
  private keepalive?: NodeJS.Timeout;
  private lastReleaseAt = 0;

  constructor(
    private readonly tokens: TokenManager,
    private readonly log: PluginLogger,
  ) {}

  /** Record when a media session let go of the camera's single-viewer slot. */
  noteRelease(): void {
    this.lastReleaseAt = Date.now();
  }

  /**
   * A TC camera serves one live viewer and needs a moment to free that slot
   * after a session closes. HomeKit fetches a snapshot immediately before it
   * starts a stream, so without a gap the stream's hole punch races the
   * just-closed snapshot session. Wait out a minimum cooldown since the last
   * release before opening the next session.
   */
  async awaitReleaseCooldown(minGapMs = 3_000): Promise<void> {
    if (!this.lastReleaseAt) return;
    const remaining = minGapMs - (Date.now() - this.lastReleaseAt);
    if (remaining > 0) {
      this.log.info(`Waiting ${seconds(remaining)} for myQ camera to release prior media session`);
      await delay(remaining);
    }
  }

  async connect(): Promise<TendClient> {
    if (this.client) return this.client;
    if (this.pending) return this.pending;
    this.pending = this.create();
    try {
      return await this.pending;
    } finally {
      this.pending = undefined;
    }
  }

  private async create(): Promise<TendClient> {
    const client = new TendClient(await this.tokens.accessToken());
    try {
      await client.login(5);
    } catch (error) {
      client.close();
      throw error;
    }
    this.client = client;
    this.keepalive = setInterval(() => {
      try {
        client.cxs.keepalive();
      } catch (error) {
        this.log.warn(`myQ signaling keepalive failed: ${String(error)}`);
        this.invalidate(client);
      }
    }, 20_000);
    return client;
  }

  invalidate(client = this.client): void {
    if (!client || client !== this.client) return;
    if (this.keepalive) clearInterval(this.keepalive);
    this.keepalive = undefined;
    client.close();
    this.client = undefined;
  }

  close(): void {
    this.invalidate();
  }
}

/**
 * A single, shared, reference-counted camera session that both snapshots and
 * live streams multiplex over. The TC camera serves exactly one viewer, so
 * instead of each snapshot/stream opening a competing session, they all
 * `acquire()` this one — dissolving the snapshot→stream hole-punch collision
 * and letting multiple HomeKit viewers watch at once. When the last consumer
 * releases, the session is kept warm for `keepAliveMs` so a follow-up request
 * (another snapshot, or tapping into live view) reuses it instantly.
 */
export class SharedCameraSession extends EventEmitter {
  private session?: MyqCameraSession;
  private opening?: MyqCameraSession;
  private pending?: Promise<MyqCameraSession>;
  private noVideoTimer?: NodeJS.Timeout;
  private recovery?: Promise<void>;
  private recoveryAttempts = 0;
  private readonly maxRecoveryAttempts = 3;
  private consumers = 0;
  private idleTimer?: NodeJS.Timeout;
  private closed = false;
  private sps?: Buffer;
  private pps?: Buffer;
  private keyframe?: Buffer;

  /**
   * Optional hook run once, on the still-open session, right before the warm
   * keep-alive window closes it — used to grab a final fresh still so the tile
   * reflects the last frame after the stream is torn down. Bounded so it never
   * delays teardown for long.
   */
  onIdle?: () => Promise<void>;

  constructor(
    private readonly factory: () => MyqCameraSession,
    private readonly log: PluginLogger,
    private readonly keepAliveMs: number,
  ) {
    super();
    this.setMaxListeners(0); // multi-viewer: an unbounded number of consumers
  }

  /** True while the underlying camera session is open (streaming or warm). */
  get open(): boolean {
    return this.session !== undefined;
  }

  get consumerCount(): number {
    return this.consumers;
  }

  resetStaleWarmSession(): void {
    if (this.consumers === 0 && this.session) this.teardown();
  }

  get hasAudio(): boolean {
    return this.session?.hasAudio ?? false;
  }

  /** Latest SPS/PPS seen on the feed, to seed a late viewer's keyframe gate. */
  get videoHeaders(): { sps?: Buffer; pps?: Buffer } {
    return { sps: this.sps, pps: this.pps };
  }

  /** Latest decodable IDR access unit, prefixed with SPS/PPS if needed. */
  get videoKeyframe(): Buffer | undefined {
    return this.keyframe;
  }

  /** Open the session if needed and register a consumer. Balance with release(). */
  async acquire(): Promise<void> {
    if (this.closed) throw new Error('shared camera session is shut down');
    this.consumers += 1;
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    if (this.session) return;
    if (this.pending) {
      try {
        await this.pending;
        if (this.session && this.consumers === 0 && !this.idleTimer) this.scheduleIdleClose();
      } catch (error) {
        this.consumers = Math.max(0, this.consumers - 1);
        throw error;
      }
      return;
    }
    this.pending = this.openInternal();
    try {
      await this.pending;
      if (this.session && this.consumers === 0 && !this.idleTimer) this.scheduleIdleClose();
    } catch (error) {
      this.consumers = Math.max(0, this.consumers - 1);
      throw error;
    } finally {
      this.pending = undefined;
    }
  }

  private async openInternal(): Promise<MyqCameraSession> {
    const session = this.factory();
    this.opening = session;
    let openError: Error | undefined;
    // Capture errors emitted before the session is adopted with a local handler,
    // so they can't run this.onError() — which would teardown() and reset the
    // consumer count while this open is still in flight (corrupting the refcount
    // once the open resolves).
    const onOpenError = (error: Error): void => { openError ??= error; };
    session.on('video', this.onVideo);
    session.on('audio', this.onAudio);
    session.on('error', onOpenError);
    const abandon = (): void => {
      if (this.opening === session) this.opening = undefined;
      session.removeListener('video', this.onVideo);
      session.removeListener('audio', this.onAudio);
      session.removeListener('error', onOpenError);
      session.close();
    };
    try {
      await session.open();
    } catch (error) {
      abandon();
      throw error;
    }
    // Do not adopt a session that errored during open, or that shutdown() closed
    // out from under us — either would leak (and shutdown's session would keep
    // its keepalive timer alive past shutdown).
    if (this.closed || openError) {
      abandon();
      throw openError ?? new Error('shared camera session was shut down during open');
    }
    // Adopt: swap the local open-time handler for the real one (synchronous, so
    // no error can slip through in between).
    session.on('error', this.onError);
    session.removeListener('error', onOpenError);
    if (this.opening === session) this.opening = undefined;
    this.session = session;
    // Seedonk can report a completed handshake before any media arrives. The
    // first-frame watchdog is deliberately started only after adoption, so an
    // open-time race cannot reset a session that is not yet shared.
    this.noVideoTimer = setTimeout(() => {
      this.noVideoTimer = undefined;
      if (this.session === session) {
        this.log.warn('myQ shared camera session connected without video; recovering');
        void this.recoverSilentSession(session);
      }
    }, 10_000);
    return session;
  }

  /**
   * Replace a handshake-only session without dropping active HomeKit consumers.
   * Do not use teardown(): it clears the reference count and would make an
   * in-flight HomeKit request lose ownership while recovery is still pending.
   */
  private async recoverSilentSession(session: MyqCameraSession): Promise<void> {
    if (this.session !== session || this.closed || this.recovery) return;
    // Bound recovery so a camera that is genuinely offline cannot create an
    // endless reconnect loop or keep HomeKit requests alive forever.
    if (this.recoveryAttempts >= this.maxRecoveryAttempts) {
      this.log.error('myQ shared camera session remained silent after automatic recovery attempts');
      this.teardown();
      if (this.listenerCount('error') > 0) {
        this.emit('error', new Error('myQ camera connected but produced no video'));
      }
      return;
    }
    this.recoveryAttempts += 1;
    this.recovery = (async () => {
      session.close();
      this.session = undefined;
      this.log.warn(`myQ shared camera session recovering silent session (attempt ${this.recoveryAttempts}/${this.maxRecoveryAttempts})`);
      // After a power-cycle the camera may still be releasing its previous
      // relay state; a short gap avoids repeating the same failed handshake.
      await delay(2_000);
      if (this.closed || this.consumers === 0) return;
      this.pending = this.openInternal();
      try {
        await this.pending;
      } catch (error) {
        if (this.listenerCount('error') > 0) this.emit('error', error);
      } finally {
        this.pending = undefined;
      }
    })();
    try { await this.recovery; } finally { this.recovery = undefined; }
  }

  private readonly onVideo = (frame: Buffer): void => {
    if (this.noVideoTimer) {
      clearTimeout(this.noVideoTimer);
      this.noVideoTimer = undefined;
      this.log.info('myQ shared camera session received first video frame');
    }
    this.recoveryAttempts = 0;
    let idr = false;
    const units = nals(frame);
    for (const unit of units) {
      if (unit.type === SPS_NAL) this.sps = unit.data;
      else if (unit.type === PPS_NAL) this.pps = unit.data;
      else if (unit.type === IDR_NAL) idr = true;
    }
    if (idr && this.sps && this.pps) {
      const alreadyPrefixed = units[0] && [SPS_NAL, PPS_NAL].includes(units[0].type);
      this.keyframe = alreadyPrefixed ? frame : Buffer.concat([this.sps, this.pps, frame]);
    }
    this.emit('video', frame);
  };

  private readonly onAudio = (frame: Buffer): void => { this.emit('audio', frame); };

  private readonly onError = (error: Error): void => {
    this.log.error(`myQ shared camera session error: ${error.message}`);
    this.teardown();
    // Notify consumers so they clean up; guard against the EventEmitter throw
    // when nobody is currently listening for 'error'.
    if (this.listenerCount('error') > 0) this.emit('error', error);
  };

  /** Release a consumer; when the last one leaves, keep the session warm. */
  release(): void {
    this.consumers = Math.max(0, this.consumers - 1);
    if (this.consumers === 0) {
      if (this.session && !this.idleTimer) {
        this.scheduleIdleClose();
      } else if (!this.session && this.pending && this.opening) {
        this.log.info('No HomeKit consumers remain while myQ session is opening; canceling pending camera open');
        this.opening.close();
      }
    }
  }

  private scheduleIdleClose(): void {
    this.idleTimer = setTimeout(() => { void this.idleClose(); }, this.keepAliveMs);
  }

  private async idleClose(): Promise<void> {
    this.idleTimer = undefined;
    if (this.closed || this.consumers > 0 || !this.session) return;
    if (this.onIdle) {
      // Capture a final still off the still-open session before tearing it down,
      // so the tile reflects the last frame. Bounded so a stuck capture can't
      // hold the session open.
      try {
        await Promise.race([this.onIdle(), delay(3_000)]);
      } catch {
        // best-effort
      }
    }
    // A consumer may have acquired during the final capture — only tear down if
    // still idle.
    if (this.consumers === 0 && this.session) this.teardown();
  }

  writeTalkback(data: Buffer): void {
    this.session?.writeTalkback(data);
  }

  private teardown(): void {
    if (this.noVideoTimer) {
      clearTimeout(this.noVideoTimer);
      this.noVideoTimer = undefined;
    }
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    const session = this.session;
    const opening = this.opening;
    this.session = undefined;
    this.opening = undefined;
    this.consumers = 0;
    this.sps = undefined;
    this.pps = undefined;
    this.keyframe = undefined;
    if (session) {
      session.removeListener('video', this.onVideo);
      session.removeListener('audio', this.onAudio);
      session.removeListener('error', this.onError);
      session.close();
    }
    if (opening && opening !== session) {
      opening.removeListener('video', this.onVideo);
      opening.removeListener('audio', this.onAudio);
      opening.close();
    }
  }

  shutdown(): void {
    this.closed = true;
    this.teardown();
    this.removeAllListeners();
  }
}

export class MyqCameraSession extends EventEmitter {
  private client?: TendClient;
  private video?: P2PMediaSession;
  private videoInfo?: ConnectionInfo;
  private audio?: P2PMediaSession;
  private audioInfo?: ConnectionInfo;
  private closed = false;
  camera?: Camera;

  constructor(
    private readonly connection: TendConnectionManager,
    private readonly log: PluginLogger,
    private readonly options: CameraSessionOptions = {},
  ) {
    super();
  }

  private async retry<T>(operation: () => Promise<T>, attempts = 3): Promise<T> {
    let last: unknown;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        return await operation();
      } catch (error) {
        last = error;
        if (attempt + 1 < attempts) await delay(2_000);
      }
    }
    throw last;
  }

  /**
   * Establish the video channel, retrying the hole punch through the brief
   * window where the camera is still releasing a prior viewer session. Each
   * attempt requests fresh connection info because the relay assignment may
   * change between tries.
   */
  private async establishVideo(): Promise<void> {
    // Capture stable references: close() nulls this.client/this.camera, and the
    // retry loop below awaits, so HomeKit tearing the session down mid-setup
    // must not turn into an undefined dereference. We bail via the `closed`
    // checks instead.
    const client = this.client;
    const camera = this.camera;
    if (!client || !camera) throw new Error('camera session was torn down before video setup');
    const noop = (): void => {};
    const attempts = Math.max(1, Math.trunc(this.options.videoPunchAttempts ?? 2));
    const timeoutMs = Math.max(1_000, this.options.videoPunchTimeoutMs ?? 8_000);
    const retryDelayMs = Math.max(0, this.options.videoPunchRetryDelayMs ?? 4_000);
    let last: unknown;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (this.closed) throw new Error('camera session closed during video setup');
      const info = await this.retry(() => client.getVideoConnection(camera), 2);
      const session = new P2PMediaSession(camera, info, 'V', 'w1', 90_000);
      session.on('error', noop); // absorb transient socket errors during the punch
      try {
        this.log.info(
          `myQ video hole punch starting (attempt ${attempt + 1}/${attempts}, `
          + `timeout=${seconds(timeoutMs)})`,
        );
        await session.punch(timeoutMs);
      } catch (error) {
        last = error;
        session.close();
        if (this.closed) {
          this.log.info(
            `myQ video hole punch canceled while opening camera session `
            + `(attempt ${attempt + 1}/${attempts}): ${describeError(error)}`,
          );
          throw new Error('camera session closed during video setup');
        }
        if (attempt + 1 >= attempts) {
          this.log.warn(
            `myQ video hole punch failed (attempt ${attempt + 1}/${attempts}); `
            + `no retries left: ${describeError(error)}`,
          );
          throw last;
        }
        this.log.warn(
          `myQ video hole punch failed (attempt ${attempt + 1}/${attempts}): `
          + `${describeError(error)}; retrying after ${seconds(retryDelayMs)}`,
        );
        await delay(retryDelayMs);
        if (this.closed) {
          this.log.info('myQ video hole punch retry canceled while waiting for retry cooldown');
          throw new Error('camera session closed during video setup');
        }
        continue;
      }
      if (this.closed) {
        session.close();
        this.log.info('myQ video hole punch succeeded, but camera session was canceled before adoption');
        throw new Error('camera session closed during video setup');
      }
      session.removeListener('error', noop);
      const gate = new KeyframeGate();
      session.on('record', (record: Buffer) => {
        gate.feed(record).forEach((frame) => this.emit('video', frame));
      });
      session.on('error', (error) => this.emit('error', error));
      this.video = session;
      this.videoInfo = info;
      client.startVideo(camera, info);
      client.setVideoQuality(
        camera,
        this.options.quality ?? 3,
        this.options.fps ?? 20,
        this.options.size ?? 0,
      );
      this.log.info(`myQ video hole punch succeeded (attempt ${attempt + 1}/${attempts})`);
      return;
    }
    throw last;
  }

  async open(): Promise<void> {
    this.client = await this.connection.connect();
    const cameras = this.client.listCameras();
    this.camera = this.options.camera
      ? cameras.find((candidate) => (
        candidate.deviceId === this.options.camera || candidate.alias === this.options.camera
      ))
      : cameras[0];
    if (!this.camera) {
      throw new Error(this.options.camera
        ? `camera not found: ${this.options.camera}`
        : 'no Tend TC-series cameras found on this account');
    }

    await this.connection.awaitReleaseCooldown(this.options.releaseCooldownMs ?? 3_000);
    await this.establishVideo();

    if (this.options.audio !== false) {
      try {
        const audioInfo = await this.retry(() => this.client!.getAudioConnection(this.camera!));
        this.audio = new P2PMediaSession(this.camera, audioInfo, 'A', 'wa', 8_000);
        this.audio.on('record', (record: Buffer) => {
          if (record.length > 4) this.emit('audio', record.subarray(4));
        });
        this.audio.on('error', (error) => this.emit('error', error));
        await this.audio.punch();
        this.client.startAudio(this.camera, audioInfo);
        this.audioInfo = audioInfo;
      } catch (error) {
        this.log.warn(`myQ audio unavailable; continuing video-only: ${String(error)}`);
        this.audio?.close();
        this.audio = undefined;
        this.audioInfo = undefined;
      }
    }

    this.log.info(`Connected native myQ session to ${this.camera.alias}`);
  }

  get hasAudio(): boolean {
    return this.audio !== undefined;
  }

  writeTalkback(data: Buffer): void {
    if (!this.audio) return;
    this.audio.enqueueTalkback(data);
  }

  private stopSignaling(kind: 'video' | 'audio', info?: ConnectionInfo): void {
    if (!this.client || !this.camera || !info) return;
    try {
      if (kind === 'video') this.client.stopVideo(this.camera, info);
      else this.client.stopAudio(this.camera, info);
    } catch (error) {
      this.log.warn(`myQ ${kind} stop command failed; closing local sockets anyway: ${String(error)}`);
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.stopSignaling('video', this.videoInfo);
    this.stopSignaling('audio', this.audioInfo);
    this.video?.close();
    this.audio?.close();
    this.video = undefined;
    this.videoInfo = undefined;
    this.audio = undefined;
    this.audioInfo = undefined;
    this.client = undefined;
    this.connection.noteRelease();
  }
}
