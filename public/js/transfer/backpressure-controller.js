// ZapShare Backpressure Controller (Phases 5, 6, 7)
// Deterministic RTCDataChannel flow control with adaptive chunk sizing & zero fake timeouts
import { TransferDefaults } from './constants.js';

export class BackpressureController {
  constructor(dataChannel, options = {}) {
    this.dataChannel = dataChannel;
    this.highWaterMark = options.highWaterMark || TransferDefaults.HIGH_WATER_MARK;
    this.lowWaterMark = options.lowWaterMark || TransferDefaults.LOW_WATER_MARK;
    this.sctpMaxMessageSize = options.sctpMaxMessageSize || 65536;

    this.minChunkSize = options.minChunkSize || TransferDefaults.MIN_CHUNK_SIZE;
    const initialUpperLimit = Math.min(options.maxChunkSize || TransferDefaults.MAX_CHUNK_SIZE, this.sctpMaxMessageSize);
    this.maxChunkSize = initialUpperLimit;

    this.currentChunkSize = Math.min(
      Math.max(this.minChunkSize, options.initialChunkSize || TransferDefaults.INITIAL_CHUNK_SIZE),
      this.maxChunkSize
    );

    // Metrics & Diagnostics
    this.totalWaitTimeMs = 0;
    this.waitEventCount = 0;
    this.isWaiting = false;
    this.consecutiveFastDrains = 0;
    this.consecutiveCongestions = 0;

    if (this.dataChannel) {
      try {
        this.dataChannel.bufferedAmountLowThreshold = this.lowWaterMark;
      } catch (err) {
        console.warn('[Backpressure] Failed to set bufferedAmountLowThreshold:', err);
      }
    }
  }

  applyTuning(tuning) {
    if (!tuning) return;
    if (tuning.highWater) this.highWaterMark = tuning.highWater;
    if (tuning.lowWater) this.lowWaterMark = tuning.lowWater;
    if (tuning.minChunk) this.minChunkSize = tuning.minChunk;
    if (tuning.maxChunk) {
      this.maxChunkSize = Math.min(tuning.maxChunk, this.sctpMaxMessageSize || 262144);
    }
    if (tuning.initialChunk && this.waitEventCount === 0) {
      this.currentChunkSize = Math.min(tuning.initialChunk, this.maxChunkSize);
    }

    if (this.dataChannel) {
      try {
        this.dataChannel.bufferedAmountLowThreshold = this.lowWaterMark;
      } catch (e) {}
    }
  }

  updateDataChannel(dataChannel, sctpMax = null) {
    this.dataChannel = dataChannel;
    if (sctpMax && sctpMax > 0) {
      this.sctpMaxMessageSize = sctpMax;
      this.maxChunkSize = Math.min(this.maxChunkSize, sctpMax);
      if (this.currentChunkSize > this.maxChunkSize) {
        this.currentChunkSize = this.maxChunkSize;
      }
    }
    if (this.dataChannel) {
      try {
        this.dataChannel.bufferedAmountLowThreshold = this.lowWaterMark;
      } catch (e) {}
    }
  }

  getBufferedAmount() {
    return this.dataChannel ? this.dataChannel.bufferedAmount : 0;
  }

  getChunkSize() {
    return this.currentChunkSize;
  }

  isCongested() {
    if (!this.dataChannel) return true;
    return this.dataChannel.bufferedAmount >= this.highWaterMark;
  }

  /**
   * Deterministically awaits until dataChannel.bufferedAmount drains below lowWaterMark.
   * NEVER uses an arbitrary timeout to resume pushing chunks into a full buffer.
   * If aborted via cancelSignal or channel closes/errors, rejects immediately.
   */
  async waitUntilDrained(abortSignal = null) {
    if (!this.dataChannel || this.dataChannel.readyState !== 'open') {
      throw new Error('DataChannel is not open');
    }

    // Fast path: buffer already at or below low water mark
    if (this.dataChannel.bufferedAmount <= this.lowWaterMark) {
      this.recordFastDrain();
      return 0;
    }

    const waitStart = performance.now();
    this.isWaiting = true;
    this.waitEventCount++;

    return new Promise((resolve, reject) => {
      let resolved = false;

      const cleanup = () => {
        if (this.dataChannel) {
          this.dataChannel.removeEventListener('bufferedamountlow', onLow);
          this.dataChannel.removeEventListener('close', onClose);
          this.dataChannel.removeEventListener('error', onError);
        }
        if (abortSignal) {
          abortSignal.removeEventListener('abort', onAbort);
        }
        this.isWaiting = false;
      };

      const finishSuccess = () => {
        if (resolved) return;
        resolved = true;
        const duration = performance.now() - waitStart;
        this.totalWaitTimeMs += duration;
        this.recordBackpressurePause(duration);
        cleanup();
        resolve(duration);
      };

      const onLow = () => {
        if (this.dataChannel && this.dataChannel.bufferedAmount <= this.lowWaterMark) {
          finishSuccess();
        }
      };

      const onClose = () => {
        if (resolved) return;
        resolved = true;
        cleanup();
        reject(new Error('DataChannel closed during backpressure wait'));
      };

      const onError = (e) => {
        if (resolved) return;
        resolved = true;
        cleanup();
        reject(new Error(`DataChannel error during backpressure wait: ${e?.message || 'unknown'}`));
      };

      const onAbort = () => {
        if (resolved) return;
        resolved = true;
        cleanup();
        reject(new Error('Transfer aborted while waiting for backpressure drain'));
      };

      this.dataChannel.addEventListener('bufferedamountlow', onLow);
      this.dataChannel.addEventListener('close', onClose, { once: true });
      this.dataChannel.addEventListener('error', onError, { once: true });

      if (abortSignal) {
        if (abortSignal.aborted) {
          onAbort();
          return;
        }
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }

      // Safeguard check in case the event fired immediately between check and listener attachment
      if (this.dataChannel.bufferedAmount <= this.lowWaterMark) {
        finishSuccess();
      }
    });
  }

  recordFastDrain() {
    this.consecutiveCongestions = 0;
    this.consecutiveFastDrains++;

    // After 35 consecutive fast drain chunks, safely step up chunk size
    if (this.consecutiveFastDrains >= 35) {
      this.consecutiveFastDrains = 0;
      if (this.currentChunkSize < this.maxChunkSize) {
        const nextSize = Math.min(this.currentChunkSize + 32 * 1024, this.maxChunkSize);
        if (nextSize !== this.currentChunkSize) {
          console.log(`[Backpressure Tuning] Stepping up chunk size: ${this.currentChunkSize / 1024} KB ➔ ${nextSize / 1024} KB`);
          this.currentChunkSize = nextSize;
        }
      }
    }
  }

  recordBackpressurePause(waitDurationMs) {
    this.consecutiveFastDrains = 0;
    this.consecutiveCongestions++;

    // If wait was prolonged (> 60 ms) or repeated backpressure pauses occurred:
    if (waitDurationMs > 60 || this.consecutiveCongestions >= 2) {
      this.consecutiveCongestions = 0;
      if (this.currentChunkSize > this.minChunkSize) {
        const prevSize = this.currentChunkSize;
        this.currentChunkSize = Math.max(this.currentChunkSize - 32 * 1024, this.minChunkSize);
        console.log(`[Backpressure Tuning] Congestion detected (waited ${waitDurationMs.toFixed(0)} ms). Stepping down chunk size: ${prevSize / 1024} KB ➔ ${this.currentChunkSize / 1024} KB`);
      }
    }
  }

  forceScaleDown() {
    if (this.currentChunkSize > this.minChunkSize) {
      this.currentChunkSize = Math.max(this.currentChunkSize - 32 * 1024, this.minChunkSize);
      console.log(`[Backpressure Tuning] Thermal/device strain scale-down: ${this.currentChunkSize / 1024} KB`);
    }
  }

  getMetrics() {
    return {
      currentChunkSize: this.currentChunkSize,
      bufferedAmount: this.getBufferedAmount(),
      isWaiting: this.isWaiting,
      totalWaitTimeMs: Math.round(this.totalWaitTimeMs),
      waitEventCount: this.waitEventCount,
      highWaterMark: this.highWaterMark,
      lowWaterMark: this.lowWaterMark
    };
  }

  reset() {
    this.totalWaitTimeMs = 0;
    this.waitEventCount = 0;
    this.isWaiting = false;
    this.consecutiveFastDrains = 0;
    this.consecutiveCongestions = 0;
  }
}
