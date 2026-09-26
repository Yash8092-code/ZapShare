// ZapShare Performance & Mobile Thermal Resource Manager (Phases 15, 16, 21, 22)
// Optimizes sustainable throughput per unit of CPU/battery load without false temperature claims

export class PerformanceManager {
  constructor(options = {}) {
    this.deviceProfile = options.deviceProfile || null;
    this.backpressure = options.backpressure || null;
    this.callbacks = options.callbacks || {};

    this.uiThrottleMs = options.uiThrottleMs || 500;
    this.lastUiUpdateTime = 0;
    this.lastMetricCalcTime = 0;

    // Resource & thermal degradation heuristics
    this.recentProcessingTimes = [];
    this.isDeviceThrottlingDetected = false;
    this.consecutiveSlowDiskWrites = 0;
  }

  setBackpressureController(bp) {
    this.backpressure = bp;
  }

  // Throttled UI updater (Phase 15)
  shouldUpdateUi(now = performance.now()) {
    if (now - this.lastUiUpdateTime >= this.uiThrottleMs) {
      this.lastUiUpdateTime = now;
      return true;
    }
    return false;
  }

  // Records chunk processing latency and evaluates indirect device stress (Phase 21)
  recordProcessingEvent(durationMs, storageLatencyMs = 0) {
    this.recentProcessingTimes.push(durationMs);
    if (this.recentProcessingTimes.length > 20) {
      this.recentProcessingTimes.shift();
    }

    if (storageLatencyMs > 80) {
      this.consecutiveSlowDiskWrites++;
    } else {
      this.consecutiveSlowDiskWrites = Math.max(0, this.consecutiveSlowDiskWrites - 1);
    }

    // Evaluate indirect thermal / resource strain
    const avgDuration = this.recentProcessingTimes.reduce((a, b) => a + b, 0) / this.recentProcessingTimes.length;
    if ((avgDuration > 45 || this.consecutiveSlowDiskWrites >= 3) && !this.isDeviceThrottlingDetected) {
      this.isDeviceThrottlingDetected = true;
      console.warn('[PerformanceManager] ⚡ High device resource latency detected. Optimizing transfer for device performance...');

      if (this.backpressure) {
        // Step down chunk size to reduce CPU & memory bandwidth
        this.backpressure.recordBackpressurePause(150);
      }

      if (this.callbacks.onDeviceStress) {
        this.callbacks.onDeviceStress('Optimizing transfer for device performance...');
      }
    } else if (avgDuration < 15 && this.consecutiveSlowDiskWrites === 0 && this.isDeviceThrottlingDetected) {
      this.isDeviceThrottlingDetected = false;
    }
  }

  isHeavyVisualsAllowed() {
    if (!this.deviceProfile) return true;
    const tuning = this.deviceProfile.getTuning();
    if (!tuning.enableHeavyVisuals) return false;
    if (this.isDeviceThrottlingDetected) return false; // Temporarily pause heavy canvas stream
    return true;
  }

  reset() {
    this.recentProcessingTimes = [];
    this.isDeviceThrottlingDetected = false;
    this.consecutiveSlowDiskWrites = 0;
    this.lastUiUpdateTime = 0;
  }
}
