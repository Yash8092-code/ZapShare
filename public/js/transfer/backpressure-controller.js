// ZapShare Backpressure Controller
// Deterministic RTCDataChannel flow control with adaptive chunk sizing & zero fake timeouts
import { TransferDefaults } from './constants.js';

export class BackpressureController {
  constructor(dataChannel, options = {}) {
    this.dataChannel = dataChannel;
    this.highWaterMark = options.highWaterMark || TransferDefaults.HIGH_WATER_MARK;
    this.lowWaterMark = options.lowWaterMark || TransferDefaults.LOW_WATER_MARK;
    this.sctpMaxMessageSize = options.sctpMaxMessageSize || 65536;

    // Chunk size range: 32 KB to 128 KB (capped by sctpMaxMessageSize)
    const upperLimit = Math.min(TransferDefaults.MAX_CHUNK_SIZE, this.sctpMaxMessageSize);
    this.currentChunkSize = Math.min(
      Math.max(TransferDefaults.MIN_CHUNK_SIZE, TransferDefaults.INITIAL_CHUNK_SIZE),
      upperLimit
    );
    this.minChunkSize = TransferDefaults.MIN_CHUNK_SIZE;
    this.maxChunkSize = upperLimit;

    // Metrics & Diagnostics
    this.totalWaitTimeMs = 0;
    this.waitEventCount = 0;
    this.isWaiting = false;
    this.consecutiveFastDrains = 0;
    this.consecutiveCongestions = 0;

    // Configure the underlying DataChannel threshold
    if (this.dataChannel) {
      try {
        this.dataChannel.bufferedAmountLowThreshold = this.lowWaterMark;
      } catch (err) {
        console.warn('[Backpressure] Failed to set bufferedAmountLowThreshold:', err);
      }
    }
  }

  updateDataChannel(dataChannel, sctpMax = null) {
    this.dataChannel = dataChannel;
    if (sctpMax) {
      this.sctpMaxMessageSize = sctpMax;
      this.maxChunkSize = Math.min(TransferDefaults.MAX_CHUNK_SIZE, sctpMax);
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

  // Returns true if the channel buffer is above high-water mark
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
      return;
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
        // Double-check buffer level (some browsers fire bufferedamountlow slightly before full drain)
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

  // Adaptive tuning: increases chunk size after consecutive smooth drain periods
  recordFastDrain() {
    this.consecutiveCongestions = 0;
    this.consecutiveFastDrains++;

    // If we have had 40 consecutive chunks with low buffer and high throughput:
    if (this.consecutiveFastDrains >= 40) {
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

  // Adaptive tuning: decreases chunk size if channel is hitting backpressure stalls
  recordBackpressurePause(waitDurationMs) {
    this.consecutiveFastDrains = 0;
    this.consecutiveCongestions++;

    // If wait was prolonged or backpressure repeats frequently:
    if (waitDurationMs > 80 || this.consecutiveCongestions >= 2) {
      this.consecutiveCongestions = 0;
      if (this.currentChunkSize > this.minChunkSize) {
        const prevSize = this.currentChunkSize;
        this.currentChunkSize = Math.max(this.currentChunkSize - 32 * 1024, this.minChunkSize);
        console.log(`[Backpressure Tuning] Congestion detected (waited ${waitDurationMs.toFixed(0)} ms). Stepping down chunk size: ${prevSize / 1024} KB ➔ ${this.currentChunkSize / 1024} KB`);
      }
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
    this.currentChunkSize = Math.min(
      Math.max(TransferDefaults.MIN_CHUNK_SIZE, TransferDefaults.INITIAL_CHUNK_SIZE),
      this.maxChunkSize
    );
  }
}
