import { spawn, type ChildProcess } from 'node:child_process';
import { createSocket, type Socket } from 'node:dgram';
import { readFileSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import type { Writable } from 'node:stream';
import type {
  API,
  CameraController,
  CameraStreamingOptions,
  CameraStreamingDelegate,
  HAP,
  PrepareStreamCallback,
  PrepareStreamRequest,
  SnapshotRequest,
  SnapshotRequestCallback,
  StartStreamRequest,
  StreamingRequest,
  StreamRequestCallback,
} from 'homebridge';
import { KeyframeGate } from './h264';
import { MyqCameraSession, SharedCameraSession, TendConnectionManager } from './session';
import type { CameraConfig, MyqPlatformConfig, PluginLogger } from './types';

// HAP needs a concrete stream count; treat "unlimited" (0/unset) as this cap.
const UNLIMITED_VIEWERS = 8;

interface SessionInfo {
  address: string;
  ipv6: boolean;
  videoPort: number;
  videoReturnPort: number;
  videoSrtp: Buffer;
  videoSsrc: number;
  audioPort?: number;
  audioReturnPort?: number;
  audioSrtp?: Buffer;
  audioSsrc?: number;
}

interface ActiveSession {
  cleanup: (reason?: string) => void;
}

type ErrorEmitter = {
  on(event: 'error', listener: (error: NodeJS.ErrnoException) => void): unknown;
};

function reservePort(ipv6: boolean): Promise<number> {
  return new Promise((resolve, reject) => {
    const socket = createSocket(ipv6 ? 'udp6' : 'udp4');
    socket.once('error', reject);
    socket.bind(0, ipv6 ? '::' : '0.0.0.0', () => {
      const port = socket.address().port;
      socket.close(() => resolve(port));
    });
  });
}

function isExpectedTeardownError(error: NodeJS.ErrnoException): boolean {
  return error.code === 'ECONNRESET'
    || error.code === 'EPIPE'
    || error.code === 'ERR_STREAM_DESTROYED'
    || error.code === 'ERR_STREAM_PREMATURE_CLOSE'
    || error.code === 'ERR_STREAM_WRITE_AFTER_END';
}

function pipeLabel(error: NodeJS.ErrnoException): string {
  return error.code ? `${error.code}: ${error.message}` : error.message;
}

function describeError(error: unknown): string {
  if (error instanceof Error) {
    const cause = (error as Error & { cause?: unknown }).cause;
    return cause ? `${error.message}; caused by ${describeError(cause)}` : error.message;
  }
  return String(error);
}

function secondsToMs(value: number | undefined, fallback: number, min: number): number {
  return Math.max(min, value ?? fallback) * 1000;
}

function integerOption(value: number | undefined, fallback: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.trunc(value ?? fallback)));
}

function secondsLabel(ms: number): string {
  return `${(ms / 1000).toFixed(ms % 1000 === 0 ? 0 : 1)}s`;
}

function observeErrors(
  emitter: ErrorEmitter | null | undefined,
  onError: (error: NodeJS.ErrnoException) => void,
): void {
  emitter?.on('error', onError);
}

function write(
  stream: Writable | null,
  data: Buffer,
  onError?: (error: NodeJS.ErrnoException) => void,
): void {
  if (!stream || stream.destroyed || !stream.writable) return;
  try {
    stream.write(data);
  } catch (error) {
    onError?.(error as NodeJS.ErrnoException);
  }
}

// MPEG-4 AudioSpecificConfig values for AAC-ELD, mono, 480-sample short frames.
// HomeKit commonly chooses 24 kHz/20 ms for return audio even when 16 kHz is
// also advertised. If this config's sampling-frequency index does not match the
// RTP rate, FFmpeg still decodes, but speech is pitch-shifted/slowed.
const AAC_ELD_CONFIGS = new Map<number, string>([
  [8000, 'F8F63000'],
  [16000, 'F8F03000'],
  [22050, 'F8EE3000'],
  [24000, 'F8EC3000'],
  [32000, 'F8EA3000'],
]);

function aacEldConfig(sampleRate: number): string {
  const config = AAC_ELD_CONFIGS.get(sampleRate);
  if (!config) throw new Error(`unsupported HomeKit AAC-ELD sample rate: ${sampleRate}`);
  return config;
}

export class StreamingDelegate implements CameraStreamingDelegate {
  readonly controller: CameraController;
  private readonly pending = new Map<string, SessionInfo>();
  private readonly active = new Map<string, ActiveSession>();
  private readonly shared: SharedCameraSession;
  private lastSnapshot?: Buffer;
  private lastSnapshotAt = 0;
  private readonly snapshotFile: string;
  private readonly snapshotRefreshMs: number;
  private readonly snapshotTimeoutMs: number;
  private readonly streamStartupTimeoutMs: number;
  private snapshotRefreshing = false;

  private readonly wantAudio: boolean;
  private readonly wantTalkback: boolean;
  private readonly copyVideo: boolean;

  constructor(
    private readonly hap: HAP,
    private readonly log: PluginLogger,
    private readonly config: MyqPlatformConfig,
    private readonly cameraConfig: CameraConfig,
    connection: TendConnectionManager,
    private readonly ffmpegPath: string,
    api: API,
  ) {
    this.wantAudio = config.audio !== false;
    this.wantTalkback = this.wantAudio && config.talkback !== false;
    this.copyVideo = config.copyVideo === true;

    // One shared, ref-counted camera session for snapshots + streams. Opened
    // with audio when wanted so a follow-up stream gets audio from a warm
    // session without reopening.
    const keepAliveMs = Math.max(10, config.keepAliveSeconds ?? 60) * 1000;
    const videoPunchTimeoutMs = secondsToMs(config.videoPunchTimeoutSeconds, 8, 1);
    const videoPunchAttempts = integerOption(config.videoPunchAttempts, 2, 1, 8);
    const videoPunchRetryDelayMs = secondsToMs(config.videoPunchRetryDelaySeconds, 4, 0);
    const releaseCooldownMs = secondsToMs(config.interSessionCooldownSeconds, 3, 0);
    this.snapshotTimeoutMs = secondsToMs(config.snapshotTimeoutSeconds, 15, 3);
    this.streamStartupTimeoutMs = secondsToMs(config.streamStartupTimeoutSeconds, 45, 10);
    this.shared = new SharedCameraSession(
      () => new MyqCameraSession(connection, this.log, {
        camera: this.cameraConfig.deviceId,
        audio: this.wantAudio,
        videoPunchTimeoutMs,
        videoPunchAttempts,
        videoPunchRetryDelayMs,
        releaseCooldownMs,
      }),
      this.log,
      keepAliveMs,
    );
    // Grab a final still right before the warm session idles out, so the tile
    // reflects the last frame after the stream is gone.
    this.shared.onIdle = () => this.captureFinalStill();

    // Persist snapshots to the Homebridge storage dir so a restart serves the
    // last still instantly (and the tile is never blank while a fresh one
    // renders).
    const slug = (cameraConfig.deviceId || cameraConfig.name || 'camera')
      .replace(/[^\w.-]+/g, '_');
    this.snapshotFile = join(api.user.storagePath(), 'myq-camera', `snapshot-${slug}.jpg`);
    // How stale a cached still may be before a snapshot request refreshes it
    // from the shared session (opening it if closed). 0 = always refresh on
    // snapshot request. This is safe with the singleton session: a follow-up
    // live view reuses the same warm camera session instead of colliding with
    // a separate snapshot session.
    const interval = config.snapshotRefreshInterval;
    this.snapshotRefreshMs = interval === 0 ? 0 : Math.max(5, interval ?? 15) * 1000;
    try {
      this.lastSnapshot = readFileSync(this.snapshotFile);
      this.lastSnapshotAt = Date.now();
    } catch {
      // no persisted snapshot yet — the first request captures a live one
    }

    const resolutions: Array<[number, number, number]> = this.copyVideo
      ? [[1280, 720, 20]]
      : [[1280, 720, 20], [1280, 720, 15], [640, 360, 20], [320, 240, 15]];
    const streamingOptions: CameraStreamingOptions = {
      supportedCryptoSuites: [hap.SRTPCryptoSuites.AES_CM_128_HMAC_SHA1_80],
      video: {
        resolutions,
        codec: {
          profiles: [hap.H264Profile.BASELINE],
          levels: [hap.H264Level.LEVEL3_1],
        },
      },
    };
    if (this.wantAudio) {
      streamingOptions.audio = {
        twoWayAudio: this.wantTalkback,
        codecs: [{
          type: this.wantTalkback
            ? hap.AudioStreamingCodecType.AAC_ELD
            : hap.AudioStreamingCodecType.OPUS,
          samplerate: [
            hap.AudioStreamingSamplerate.KHZ_16,
            hap.AudioStreamingSamplerate.KHZ_24,
          ],
        }],
      };
    }
    // Concurrent HomeKit viewers all multiplex the one shared session, so the
    // camera's single-viewer limit no longer caps us. HAP needs a concrete
    // count; 0/unset means "unlimited" -> a generous cap.
    const configuredViewers = config.maxViewers;
    const cameraStreamCount = !configuredViewers
      ? UNLIMITED_VIEWERS
      : Math.max(1, Math.min(16, configuredViewers));
    this.controller = new hap.CameraController({
      cameraStreamCount,
      delegate: this,
      streamingOptions,
    });
    api.on('shutdown', () => this.shutdown());
  }

  /** Capture one last still off the still-open session before it idles out. */
  private captureFinalStill(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.shared.open || this.snapshotRefreshing) {
        resolve();
        return;
      }
      this.refreshSnapshot(1280, 720, false, () => resolve());
    });
  }

  private async persistSnapshot(image: Buffer): Promise<void> {
    this.lastSnapshot = image;
    this.lastSnapshotAt = Date.now();
    try {
      await mkdir(dirname(this.snapshotFile), { recursive: true });
      await writeFile(this.snapshotFile, image);
    } catch (error) {
      this.log.debug(`[${this.cameraConfig.name}] could not persist snapshot: ${describeError(error)}`);
    }
  }

  private snapshotAge(): string {
    if (!this.lastSnapshot) return 'none';
    return secondsLabel(Date.now() - this.lastSnapshotAt);
  }

  handleSnapshotRequest(request: SnapshotRequest, callback: SnapshotRequestCallback): void {
    // Prefer returning a fresh still when the camera session is already open:
    // HomeKit tile polls after a live view otherwise show the previous poll's
    // image (roughly 10s old on iOS). This passive capture does not extend the
    // warm keep-alive window.
    if (this.shared.open) {
      this.log.info(
        `Snapshot request for ${this.cameraConfig.name} (${request.width}x${request.height}): `
        + `refreshing from warm session (cacheAge=${this.snapshotAge()})`,
      );
      this.refreshSnapshot(request.width, request.height, false, callback);
      return;
    }

    // Cold cache (first ever use): open a session, capture a still, answer.
    // This is intentionally allowed even when snapshotRefreshInterval=0 so the
    // accessory can bootstrap from blank to usable.
    if (!this.lastSnapshot) {
      this.log.info(
        `Snapshot request for ${this.cameraConfig.name} (${request.width}x${request.height}): `
        + 'no cache — opening shared session',
      );
      this.refreshSnapshot(request.width, request.height, true, callback);
      return;
    }

    // Closed session + refreshable cache: block for a fresh still (bounded by
    // the snapshot guard) instead of returning a "not responding" looking old
    // tile. Opening the shared session here also warms a likely follow-up live
    // view. snapshotRefreshInterval=0 means refresh on every snapshot request.
    if (this.snapshotRefreshMs === 0 || Date.now() - this.lastSnapshotAt >= this.snapshotRefreshMs) {
      const reason = this.snapshotRefreshMs === 0
        ? 'interval=0'
        : `cacheAge=${this.snapshotAge()} >= interval=${secondsLabel(this.snapshotRefreshMs)}`;
      this.log.info(
        `Snapshot request for ${this.cameraConfig.name} (${request.width}x${request.height}): `
        + `opening shared session (${reason})`,
      );
      this.refreshSnapshot(request.width, request.height, true, callback);
      return;
    }

    // Closed session + fresh-enough cache: serve the persisted still immediately
    // without opening the camera.
    this.log.info(
      `Snapshot request for ${this.cameraConfig.name} (${request.width}x${request.height}): `
      + `serving cached still (cacheAge=${this.snapshotAge()}, interval=${secondsLabel(this.snapshotRefreshMs)})`,
    );
    callback(undefined, this.lastSnapshot);
  }

  /**
   * Capture one still from the shared session into the cache (and disk),
   * optionally answering a HomeKit snapshot request. The FFmpeg handler is
   * attached before `acquire()` so a cold open's first keyframe is captured.
   */
  private refreshSnapshot(
    width: number,
    height: number,
    acquireSession: boolean,
    callback?: SnapshotRequestCallback,
  ): void {
    if (this.snapshotRefreshing) {
      this.log.info(
        `Snapshot refresh for ${this.cameraConfig.name} already in progress; `
        + `returning ${this.lastSnapshot ? 'cached still' : 'error'}`,
      );
      callback?.(this.lastSnapshot ? undefined : new Error('a snapshot capture is in progress'), this.lastSnapshot);
      return;
    }
    // Passive capture (no acquire) reads an already-open session — it neither
    // opens one nor resets the keep-alive timer, so tile polls during the warm
    // window stay fresh without holding the session open indefinitely.
    if (!acquireSession && !this.shared.open) {
      this.log.info(`Snapshot refresh for ${this.cameraConfig.name} skipped; shared session is no longer open`);
      callback?.(this.lastSnapshot ? undefined : new Error('camera session not open'), this.lastSnapshot);
      return;
    }
    this.snapshotRefreshing = true;
    const startedAt = Date.now();
    this.log.info(
      `Snapshot refresh starting for ${this.cameraConfig.name} `
      + `(${acquireSession ? 'open-or-reuse' : 'warm-read'}, timeout=${secondsLabel(this.snapshotTimeoutMs)})`,
    );
    // Gate so the still decodes from a clean SPS/PPS/IDR even mid-stream.
    const gate = new KeyframeGate(this.shared.videoHeaders.sps, this.shared.videoHeaders.pps);
    const ffmpeg = spawn(this.ffmpegPath, [
      '-hide_banner', '-loglevel', 'error',
      '-f', 'h264', '-i', 'pipe:0',
      '-frames:v', '1',
      '-vf',
      `scale=${width}:${height}:force_original_aspect_ratio=decrease,`
        + `pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2`,
      '-f', 'image2', 'pipe:1',
    ]);
    const chunks: Buffer[] = [];
    const onVideo = (frame: Buffer): void => {
      for (const out of gate.feed(frame)) write(ffmpeg.stdin, out, () => {});
    };
    const feedCachedKeyframe = (): void => {
      const keyframe = this.shared.videoKeyframe;
      if (!keyframe) return;
      for (const out of gate.feed(keyframe)) write(ffmpeg.stdin, out, () => {});
    };
    const onError = (error: Error): void => finish(error);
    let done = false;
    let acquired = false;
    const finish = (error?: Error): void => {
      if (done) return;
      done = true;
      clearTimeout(guard);
      this.shared.removeListener('video', onVideo);
      this.shared.removeListener('error', onError);
      ffmpeg.kill('SIGKILL');
      this.snapshotRefreshing = false;
      if (acquired) this.shared.release();
      const image = chunks.length ? Buffer.concat(chunks) : undefined;
      if (image) void this.persistSnapshot(image);
      const elapsed = secondsLabel(Date.now() - startedAt);
      if (error) {
        this.log.warn(`Snapshot refresh failed for ${this.cameraConfig.name} after ${elapsed}: ${describeError(error)}`);
        // Only reset an idle warm session: an active viewer owns the session
        // and must not be disrupted by a passive snapshot timeout.
        if (error.message === 'snapshot timeout' && !acquireSession
          && this.shared.consumerCount === 0 && this.shared.open) {
          this.log.warn(`Snapshot refresh for ${this.cameraConfig.name} reset stale warm camera session`);
          this.shared.resetStaleWarmSession();
        }
      } else if (image) {
        this.log.info(`Snapshot refresh completed for ${this.cameraConfig.name} after ${elapsed} (${image.length} bytes)`);
      } else {
        this.log.warn(`Snapshot refresh produced no image for ${this.cameraConfig.name} after ${elapsed}`);
      }
      callback?.(error ?? (image ? undefined : new Error('snapshot produced no image')), image ?? this.lastSnapshot);
    };
    const guard = setTimeout(() => finish(new Error('snapshot timeout')), this.snapshotTimeoutMs);
    ffmpeg.stdout.on('data', (data: Buffer) => chunks.push(data));
    ffmpeg.stderr.on('data', (data: Buffer) => this.debugFfmpeg('snapshot', data));
    observeErrors(ffmpeg.stdin, (error) => { if (!done && !isExpectedTeardownError(error)) finish(error); });
    observeErrors(ffmpeg.stdout, (error) => { if (!done && !isExpectedTeardownError(error)) finish(error); });
    observeErrors(ffmpeg.stderr, (error) => { if (!done && !isExpectedTeardownError(error)) finish(error); });
    ffmpeg.once('error', finish);
    ffmpeg.once('close', () => finish());
    // Listen for shared-session errors so a cold snapshot fails fast instead of
    // waiting out the 15s timeout (and so SharedCameraSession actually emits).
    this.shared.on('error', onError);
    this.shared.on('video', onVideo);
    feedCachedKeyframe();
    if (acquireSession) {
      this.log.info(`Snapshot refresh for ${this.cameraConfig.name} acquiring shared camera session`);
      this.shared.acquire().then(() => {
        if (done) {
          // Finished (e.g. timed out) before the open completed — release now.
          this.log.info(`Snapshot refresh for ${this.cameraConfig.name} finished before shared session opened; releasing late acquire`);
          this.shared.release();
          return;
        }
        acquired = true;
        this.log.info(`Snapshot refresh for ${this.cameraConfig.name} acquired shared camera session`);
        feedCachedKeyframe();
      }).catch((error) => finish(error as Error));
    }
    // Passive mode: the session is already open (checked above); just read it.
  }

  async prepareStream(request: PrepareStreamRequest, callback: PrepareStreamCallback): Promise<void> {
    try {
      const ipv6 = request.addressVersion === 'ipv6';
      const requestAudio = (request as { audio?: PrepareStreamRequest['audio'] }).audio;
      const videoReturnPort = await reservePort(ipv6);
      const audioReturnPort = this.wantAudio && requestAudio ? await reservePort(ipv6) : undefined;
      const info: SessionInfo = {
        address: request.targetAddress,
        ipv6,
        videoPort: request.video.port,
        videoReturnPort,
        videoSrtp: Buffer.concat([request.video.srtp_key, request.video.srtp_salt]),
        videoSsrc: this.hap.CameraController.generateSynchronisationSource(),
      };
      if (this.wantAudio && requestAudio && audioReturnPort) {
        info.audioPort = requestAudio.port;
        info.audioReturnPort = audioReturnPort;
        info.audioSrtp = Buffer.concat([requestAudio.srtp_key, requestAudio.srtp_salt]);
        info.audioSsrc = this.hap.CameraController.generateSynchronisationSource();
      }
      this.pending.set(request.sessionID, info);
      callback(undefined, {
        video: {
          port: info.videoReturnPort,
          ssrc: info.videoSsrc,
          srtp_key: request.video.srtp_key,
          srtp_salt: request.video.srtp_salt,
        },
        ...(info.audioReturnPort !== undefined && info.audioSsrc !== undefined && requestAudio
          ? {
            audio: {
              port: info.audioReturnPort,
              ssrc: info.audioSsrc,
              srtp_key: requestAudio.srtp_key,
              srtp_salt: requestAudio.srtp_salt,
            },
          }
          : {}),
      });
    } catch (error) {
      this.log.warn(`HomeKit stream preparation failed for ${this.cameraConfig.name}: ${describeError(error)}`);
      callback(error as Error);
    }
  }

  private outputArguments(request: StartStreamRequest, info: SessionInfo, hasAudio: boolean): string[] {
    const video = request.video;
    const bitrate = video.max_bit_rate || 800;
    const fps = video.fps || 20;
    const mtu = video.mtu || 1316;
    const args = [
      '-hide_banner', '-loglevel', this.config.debug ? 'verbose' : 'warning',
      '-fflags', '+genpts+discardcorrupt',
      '-f', 'h264', '-r', '20', '-i', 'pipe:3',
    ];
    if (hasAudio) args.push('-f', 'mulaw', '-ar', '8000', '-ac', '1', '-i', 'pipe:4');

    args.push('-map', '0:v:0', '-an', '-sn', '-dn');
    if (this.copyVideo) {
      args.push('-codec:v', 'copy');
    } else {
      args.push(
        '-codec:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency',
        '-profile:v', 'baseline', '-level:v', '3.1', '-pix_fmt', 'yuv420p',
        '-r', String(fps),
        '-vf', `scale=${video.width}:${video.height}:force_original_aspect_ratio=decrease`,
        '-b:v', `${bitrate}k`, '-maxrate', `${bitrate}k`, '-bufsize', `${bitrate * 2}k`,
        '-g', String(fps * 2), '-keyint_min', String(fps * 2), '-sc_threshold', '0',
      );
    }
    args.push(
      '-payload_type', String(video.pt),
      '-ssrc', String(info.videoSsrc),
      '-f', 'rtp',
      '-srtp_out_suite', 'AES_CM_128_HMAC_SHA1_80',
      '-srtp_out_params', info.videoSrtp.toString('base64'),
      `srtp://${info.address}:${info.videoPort}?rtcpport=${info.videoPort}&pkt_size=${mtu}`,
    );

    if (hasAudio) {
      if (info.audioPort === undefined || !info.audioSrtp) {
        throw new Error('HomeKit audio endpoint was not prepared');
      }
      const audio = request.audio;
      args.push('-map', '1:a:0', '-vn', '-sn', '-dn');
      if (audio.codec === this.hap.AudioStreamingCodecType.AAC_ELD) {
        args.push('-codec:a', 'libfdk_aac', '-profile:a', 'aac_eld');
      } else {
        args.push('-codec:a', 'libopus', '-application', 'lowdelay');
      }
      args.push(
        '-flags', '+global_header',
        '-ar', `${audio.sample_rate}k`, '-ac', String(audio.channel),
        '-b:a', `${audio.max_bit_rate || 24}k`,
        '-payload_type', String(audio.pt),
        '-ssrc', String(info.audioSsrc),
        '-f', 'rtp',
        '-srtp_out_suite', 'AES_CM_128_HMAC_SHA1_80',
        '-srtp_out_params', info.audioSrtp.toString('base64'),
        `srtp://${info.address}:${info.audioPort}?rtcpport=${info.audioPort}&pkt_size=188`,
      );
    }
    return args;
  }

  private returnAudioSdp(request: StartStreamRequest, info: SessionInfo): string {
    if (info.audioReturnPort === undefined || !info.audioSrtp) {
      throw new Error('HomeKit talkback endpoint was not prepared');
    }
    const ipVersion = info.ipv6 ? 'IP6' : 'IP4';
    const payloadType = request.audio.pt;
    const sampleRate = request.audio.sample_rate * 1000;
    const config = aacEldConfig(sampleRate);
    return [
      'v=0',
      `o=- 0 0 IN ${ipVersion} ${info.address}`,
      's=Talk',
      `c=IN ${ipVersion} ${info.address}`,
      't=0 0',
      `m=audio ${info.audioReturnPort} RTP/AVP ${payloadType}`,
      'b=AS:24',
      `a=rtpmap:${payloadType} MPEG4-GENERIC/${sampleRate}/1`,
      'a=rtcp-mux',
      `a=fmtp:${payloadType} profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;`
        + `indexdeltalength=3;config=${config}`,
      `a=crypto:1 AES_CM_128_HMAC_SHA1_80 inline:${info.audioSrtp.toString('base64')}`,
      '',
    ].join('\r\n');
  }

  private startReturnAudio(request: StartStreamRequest, info: SessionInfo): ChildProcess {
    let loggedDecodedAudio = false;
    const child = spawn(this.ffmpegPath, [
      '-hide_banner', '-loglevel', this.config.debug ? 'verbose' : 'warning',
      '-protocol_whitelist', 'pipe,udp,rtp,file,crypto',
      '-c:a', 'libfdk_aac',
      '-f', 'sdp', '-i', 'pipe:0',
      '-map', '0:a:0', '-ac', '1', '-ar', '8000',
      '-af', 'highpass=f=120,lowpass=f=3400,alimiter=limit=0.95',
      '-f', 'mulaw', 'pipe:1',
    ]);
    child.once('error', (error: NodeJS.ErrnoException) => {
      if (isExpectedTeardownError(error)) {
        this.log.debug?.(`[${this.cameraConfig.name}] HomeKit talkback receiver closed: ${pipeLabel(error)}`);
      } else {
        this.log.warn(`HomeKit talkback receiver failed: ${pipeLabel(error)}`);
      }
    });
    observeErrors(child.stdin, (error) => {
      if (!isExpectedTeardownError(error)) {
        this.log.warn(`HomeKit talkback SDP pipe failed: ${pipeLabel(error)}`);
      }
    });
    observeErrors(child.stdout, (error) => {
      if (!isExpectedTeardownError(error)) {
        this.log.warn(`HomeKit talkback audio pipe failed: ${pipeLabel(error)}`);
      }
    });
    observeErrors(child.stderr, (error) => {
      if (!isExpectedTeardownError(error)) {
        this.log.warn(`HomeKit talkback log pipe failed: ${pipeLabel(error)}`);
      }
    });
    child.stdout.on('data', (data: Buffer) => {
      if (!loggedDecodedAudio) {
        loggedDecodedAudio = true;
        this.log.info(`Received HomeKit push-to-talk audio for ${this.cameraConfig.name}`);
      }
      this.shared.writeTalkback(data);
    });
    child.stderr.on('data', (data: Buffer) => this.debugFfmpeg('talkback', data));
    child.once('close', (code, signal) => {
      if (this.config.debug) {
        this.log.debug(
          `[${this.cameraConfig.name}] HomeKit talkback receiver exited `
          + `(code=${code ?? 'null'}, signal=${signal ?? 'null'})`,
        );
      }
    });
    try {
      child.stdin.end(this.returnAudioSdp(request, info));
    } catch (error) {
      const pipeError = error as NodeJS.ErrnoException;
      if (!isExpectedTeardownError(pipeError)) {
        this.log.warn(`HomeKit talkback SDP write failed: ${pipeLabel(pipeError)}`);
      }
    }
    return child;
  }

  private startStream(request: StartStreamRequest, callback: StreamRequestCallback): void {
    const info = this.pending.get(request.sessionID);
    if (!info) {
      callback(new Error('unknown HomeKit stream session'));
      return;
    }
    void this.runStream(request, info, callback);
  }

  private async runStream(
    request: StartStreamRequest,
    info: SessionInfo,
    callback: StreamRequestCallback,
  ): Promise<void> {
    let started = false;
    let cleaned = false;
    let callbackCompleted = false;
    let acquired = false;
    let acquiring = false;
    let pendingAcquireReleased = false;
    let ffmpeg: ChildProcess | undefined;
    let returnFfmpeg: ChildProcess | undefined;
    let returnSocket: Socket | undefined;
    let startupTimeout: NodeJS.Timeout | undefined;
    let inactivityTimer: NodeJS.Timeout | undefined;
    let videoInput: Writable | undefined;
    let audioInput: Writable | undefined;
    let gate: KeyframeGate | undefined;

    const finishCallback = (error?: Error): void => {
      if (callbackCompleted) return;
      callbackCompleted = true;
      callback(error);
    };
    const handlePipeError = (scope: string, error: NodeJS.ErrnoException): void => {
      if (isExpectedTeardownError(error)) {
        if (this.config.debug) {
          this.log.debug(`[${this.cameraConfig.name}] ${scope} closed during stream teardown: ${pipeLabel(error)}`);
        }
        return;
      }
      this.log.warn(`[${this.cameraConfig.name}] ${scope} pipe failed: ${pipeLabel(error)}`);
      cleanup('ffmpeg-pipe-error');
    };
    // Per-viewer keyframe gate: a viewer joining a warm/in-progress feed must
    // start on an IDR with SPS/PPS, or FFmpeg/HomeKit sees black until headers
    // recur. Seeded (below) with the session's known SPS/PPS.
    const onVideo = (frame: Buffer): void => {
      if (!gate) return;
      for (const out of gate.feed(frame)) {
        write(videoInput ?? null, out, (error) => handlePipeError('ffmpeg video input', error));
      }
    };
    const onAudio = (frame: Buffer): void => write(audioInput ?? null, frame, (error) => handlePipeError('ffmpeg audio input', error));
    const onSessionError = (error: Error): void => {
      this.log.error(`myQ media error: ${error.message}`);
      cleanup('camera-error');
    };

    const cleanup = (reason = 'unknown'): void => {
      if (cleaned) return;
      const wasStarted = started;
      cleaned = true;
      this.log.info(`Cleaning up HomeKit stream for ${this.cameraConfig.name} (${reason}, started=${wasStarted})`);
      if (startupTimeout) clearTimeout(startupTimeout);
      if (inactivityTimer) clearTimeout(inactivityTimer);
      this.shared.removeListener('video', onVideo);
      this.shared.removeListener('audio', onAudio);
      this.shared.removeListener('error', onSessionError);
      try { returnSocket?.close(); } catch {}
      ffmpeg?.kill('SIGKILL');
      returnFfmpeg?.kill('SIGKILL');
      this.active.delete(request.sessionID);
      this.pending.delete(request.sessionID);
      if (acquiring && !acquired && !pendingAcquireReleased) {
        pendingAcquireReleased = true;
        this.log.info(
          `HomeKit stream for ${this.cameraConfig.name} stopped while waiting for shared session; `
          + `canceling pending acquire (${reason})`,
        );
        this.shared.release();
      }
      if (wasStarted && reason !== 'shutdown' && this.shared.open) {
        // HomeKit often shows the just-ended live frame for a moment, then asks
        // us for a tile snapshot. Capture a fresh still immediately while the
        // shared feed is still warm, rather than waiting until keepAlive idle
        // close to grab the final still.
        this.refreshSnapshot(1280, 720, false);
      }
      if (acquired) {
        acquired = false;
        this.shared.release();
      }
      if (wasStarted) {
        this.log.info(`Stopped HomeKit stream for ${this.cameraConfig.name} (${reason})`);
      }
    };

    this.active.set(request.sessionID, { cleanup });
    try {
      startupTimeout = setTimeout(() => {
        if (started || cleaned) return;
        const error = new Error('HomeKit stream startup timed out');
        this.log.warn(`HomeKit stream failed for ${this.cameraConfig.name}: ${error.message}`);
        finishCallback(error);
        cleanup('startup-timeout');
      }, this.streamStartupTimeoutMs);
      this.log.info(
        `Starting HomeKit stream for ${this.cameraConfig.name} `
        + `(${request.video.width}x${request.video.height}@${request.video.fps}fps, `
        + `startupTimeout=${secondsLabel(this.streamStartupTimeoutMs)})`,
      );

      acquiring = true;
      this.log.info(`HomeKit stream for ${this.cameraConfig.name} waiting for shared camera session`);
      await this.shared.acquire();
      acquiring = false;
      if (cleaned) {
        // cleanup() ran while acquire() was still pending (HomeKit STOP or the
        // startup timeout). It normally released/canceled the pending consumer
        // already, but older paths may not have; be defensive.
        if (!pendingAcquireReleased) this.shared.release();
        this.log.info(`HomeKit stream for ${this.cameraConfig.name} acquired after cleanup; not starting FFmpeg`);
        return;
      }
      // The no-video watchdog may have replaced the session while acquire()
      // was resolving. Reacquire once rather than starting FFmpeg on a dead
      // session; the shared recovery remains single-flight.
      if (!this.shared.open && !cleaned) {
        this.log.warn(`HomeKit stream for ${this.cameraConfig.name} acquired a reset session; retrying once`);
        await this.shared.acquire();
      }
      acquired = true;
      this.log.info(`HomeKit stream for ${this.cameraConfig.name} acquired shared camera session`);

      const hasHomeKitAudio = Boolean((request as { audio?: StartStreamRequest['audio'] }).audio);
      const hasAudio = this.shared.hasAudio && hasHomeKitAudio
        && info.audioPort !== undefined && info.audioReturnPort !== undefined
        && info.audioSsrc !== undefined && !!info.audioSrtp;

      ffmpeg = spawn(
        this.ffmpegPath,
        this.outputArguments(request, info, hasAudio),
        { stdio: ['ignore', 'ignore', 'pipe', 'pipe', 'pipe'] },
      );
      videoInput = ffmpeg.stdio[3] as Writable;
      audioInput = ffmpeg.stdio[4] as Writable;
      observeErrors(videoInput, (error) => handlePipeError('ffmpeg video input', error));
      observeErrors(audioInput, (error) => handlePipeError('ffmpeg audio input', error));
      observeErrors(ffmpeg.stderr, (error) => handlePipeError('ffmpeg log output', error));

      gate = new KeyframeGate(this.shared.videoHeaders.sps, this.shared.videoHeaders.pps);
      this.shared.on('video', onVideo);
      if (hasAudio) {
        this.shared.on('audio', onAudio);
      } else if (this.wantAudio && this.shared.hasAudio) {
        this.log.warn(`HomeKit did not prepare audio for ${this.cameraConfig.name}; streaming video-only`);
      }
      this.shared.on('error', onSessionError);

      ffmpeg.stderr?.on('data', (data: Buffer) => this.debugFfmpeg('stream', data));
      ffmpeg.once('error', (error: NodeJS.ErrnoException) => {
        if (!started) {
          this.log.warn(`[${this.cameraConfig.name}] ffmpeg stream process failed before startup: ${pipeLabel(error)}`);
          finishCallback(error);
        } else if (!isExpectedTeardownError(error)) {
          this.log.warn(`[${this.cameraConfig.name}] ffmpeg stream process failed: ${pipeLabel(error)}`);
        }
        cleanup('ffmpeg-error');
      });
      ffmpeg.once('close', (code, signal) => {
        if (!started && !callbackCompleted) {
          const error = new Error(
            `ffmpeg stream exited before startup (code=${code ?? 'null'}, signal=${signal ?? 'null'})`,
          );
          this.log.warn(`HomeKit stream failed for ${this.cameraConfig.name}: ${error.message}`);
          finishCallback(error);
        }
        cleanup('ffmpeg-exit');
      });

      returnSocket = createSocket(info.ipv6 ? 'udp6' : 'udp4');
      returnSocket.on('error', (error) => {
        this.log.error(`HomeKit RTCP socket failed: ${error.message}`);
        cleanup('rtcp-error');
      });
      returnSocket.on('message', () => {
        if (inactivityTimer) clearTimeout(inactivityTimer);
        inactivityTimer = setTimeout(() => {
          this.log.info(`HomeKit client inactive; stopping ${this.cameraConfig.name}`);
          this.controller.forceStopStreamingSession(request.sessionID);
          cleanup('homekit-inactive');
        }, Math.max(5, request.video.rtcp_interval * 5) * 1000);
      });
      returnSocket.bind(info.videoReturnPort);

      if (this.wantTalkback && hasAudio) {
        this.log.info(
          `HomeKit push-to-talk ready for ${this.cameraConfig.name} `
          + `(${request.audio.codec}/${request.audio.sample_rate}kHz, `
          + `ptime=${request.audio.packet_time}ms, pt=${request.audio.pt})`,
        );
        returnFfmpeg = this.startReturnAudio(request, info);
      }
      started = true;
      if (startupTimeout) clearTimeout(startupTimeout);
      finishCallback();
      this.log.info(`Started HomeKit stream for ${this.cameraConfig.name}`);
      // Refresh the cached still off the now-open session so the tile is current
      // after a live view — the "refresh while streaming" path, independent of
      // whether HomeKit polls a snapshot during the stream. Passive: the stream
      // already holds the session.
      this.refreshSnapshot(1280, 720, false);
    } catch (error) {
      this.log.warn(`HomeKit stream failed for ${this.cameraConfig.name}: ${describeError(error)}`);
      if (this.config.debug && error instanceof Error && error.stack) {
        this.log.debug(`[${this.cameraConfig.name}] stream startup stack: ${error.stack}`);
      }
      if (!started) finishCallback(error as Error);
      cleanup(started ? 'stream-error' : 'startup-failure');
    }
  }

  handleStreamRequest(request: StreamingRequest, callback: StreamRequestCallback): void {
    switch (request.type) {
      case this.hap.StreamRequestTypes.START:
        this.startStream(request, callback);
        break;
      case this.hap.StreamRequestTypes.RECONFIGURE:
        this.log.info(`HomeKit stream reconfigure received for ${this.cameraConfig.name}`);
        callback();
        break;
      case this.hap.StreamRequestTypes.STOP:
        this.log.info(`HomeKit stream stop received for ${this.cameraConfig.name}`);
        this.active.get(request.sessionID)?.cleanup('homekit-stop');
        callback();
        break;
      default:
        callback();
    }
  }

  private debugFfmpeg(scope: string, data: Buffer): void {
    const message = data.toString().trim();
    if (message) this.log.debug(`[${this.cameraConfig.name}] [ffmpeg:${scope}] ${message}`);
  }

  shutdown(): void {
    this.active.forEach((session) => session.cleanup('shutdown'));
    this.active.clear();
    this.pending.clear();
    this.shared.shutdown();
  }
}

export { reservePort };
