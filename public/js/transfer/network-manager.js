// ZapShare Network Performance & Dynamic Bottleneck Controller (Phases 9, 10, 19, 20, 28)
import { NetworkClass, BottleneckType, RouteType } from './constants.js';

export class NetworkManager {
  constructor() {
    this.networkClass = NetworkClass.MODERATE;
    this.bottleneck = BottleneckType.NONE;
    this.routeType = RouteType.UNKNOWN;

    this.rttMs = null;
    this.confirmedSpeedMB = 0;
    this.rollingSpeedMB = 0;
    this.storageLatencyMs = 0;
    this.backpressureWaitTimeMs = 0;
    this.bufferedAmount = 0;

    this.speedSamples = [];
  }

  updateMetrics({ confirmedSpeedMB, rttMs, backpressureWaitTimeMs, bufferedAmount, storageLatencyMs, routeType }) {
    if (confirmedSpeedMB !== undefined) {
      this.confirmedSpeedMB = confirmedSpeedMB;
      this.speedSamples.push(confirmedSpeedMB);
      if (this.speedSamples.length > 5) this.speedSamples.shift();
      this.rollingSpeedMB = this.speedSamples.reduce((a, b) => a + b, 0) / this.speedSamples.length;
    }
    if (rttMs !== undefined) this.rttMs = rttMs;
    if (backpressureWaitTimeMs !== undefined) this.backpressureWaitTimeMs = backpressureWaitTimeMs;
    if (bufferedAmount !== undefined) this.bufferedAmount = bufferedAmount;
    if (storageLatencyMs !== undefined) this.storageLatencyMs = storageLatencyMs;
    if (routeType !== undefined) this.routeType = routeType;

    this.classifyNetwork();
    this.detectBottleneck();
  }

  classifyNetwork() {
    const speed = this.rollingSpeedMB;
    const rtt = this.rttMs || 50;

    if (speed < 0.5 || rtt > 350) {
      this.networkClass = NetworkClass.VERY_SLOW;
    } else if (speed < 2.0 || rtt > 180) {
      this.networkClass = NetworkClass.SLOW;
    } else if (speed < 10.0) {
      this.networkClass = NetworkClass.MODERATE;
    } else if (speed < 30.0) {
      this.networkClass = NetworkClass.FAST;
    } else {
      this.networkClass = NetworkClass.VERY_FAST;
    }
  }

  detectBottleneck() {
    if (this.storageLatencyMs > 100) {
      this.bottleneck = BottleneckType.RECEIVER_STORAGE;
    } else if (this.bufferedAmount > 1.5 * 1024 * 1024) {
      this.bottleneck = BottleneckType.BUFFER_PRESSURE;
    } else if (this.rttMs && this.rttMs > 250 && this.rollingSpeedMB < 1.0) {
      this.bottleneck = BottleneckType.NETWORK;
    } else {
      this.bottleneck = BottleneckType.NONE;
    }
  }

  // Adaptive checkpoint interval based on network speed (Phase 10)
  getAdaptiveCheckpointInterval() {
    switch (this.networkClass) {
      case NetworkClass.VERY_SLOW:
        return 512 * 1024;        // 512 KB: small recovery window for weak links
      case NetworkClass.SLOW:
        return 1024 * 1024;       // 1 MB: modest recovery checkpoint
      case NetworkClass.MODERATE:
        return 2 * 1024 * 1024;   // 2 MB: standard checkpoint
      case NetworkClass.FAST:
        return 4 * 1024 * 1024;   // 4 MB: lower ACK overhead
      case NetworkClass.VERY_FAST:
        return 6 * 1024 * 1024;   // 6 MB: high throughput efficiency
      default:
        return 2 * 1024 * 1024;
    }
  }

  // Generates honest user-facing status messages (Phase 28)
  getStatusMessage(currentState, isSender = true) {
    if (this.bottleneck === BottleneckType.RECEIVER_STORAGE) {
      return 'Receiver writing to storage...';
    }
    if (this.bottleneck === BottleneckType.BUFFER_PRESSURE) {
      return 'Data pipeline paused — backpressure';
    }
    if (this.networkClass === NetworkClass.VERY_SLOW || this.networkClass === NetworkClass.SLOW) {
      return 'Optimizing for weak network...';
    }
    if (this.routeType.includes('TURN')) {
      return `Relayed via TURN (${this.confirmedSpeedMB.toFixed(1)} MB/s)`;
    }
    if (this.routeType.includes('LAN')) {
      return `Direct LAN (${this.confirmedSpeedMB.toFixed(1)} MB/s)`;
    }
    return `Direct P2P (${this.confirmedSpeedMB.toFixed(1)} MB/s)`;
  }
}
