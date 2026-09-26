// ZapShare Sender Pipeline
// High-throughput, backpressure-governed file transmission engine
import { ControlMessageType, TransferDefaults } from './constants.js';
import { BackpressureController } from './backpressure-controller.js';
import { IntegrityManager } from './integrity-manager.js';

export class SenderPipeline {
  constructor(options = {}) {
    this.controlChannel = options.controlChannel || null;
    this.dataChannel = options.dataChannel || null;
    this.stateMachine = options.stateMachine || null;
    this.diagnostics = options.diagnostics || null;
    this.callbacks = options.callbacks || {};

    this.file = null;
    this.fileId = null;
    this.totalSize = 0;
    this.bytesQueued = 0;
    this.confirmedBytes = 0;
    this.resumeOffset = 0;

    this.isStreamingActive = false;
    this.isRemotePaused = false;
    this.abortController = null;
    this.backpressure = null;

    // Heartbeat timers
    this.heartbeatInterval = null;
    this.lastHeartbeatAckTime = performance.now();

    // Pending completion promise
    this.completionResolve = null;
    this.completionReject = null;

    if (this.dataChannel) {
      this.setChannels(this.controlChannel, this.dataChannel);
    }
  }

  setChannels(controlChannel, dataChannel, sctpMax = null) {
    this.controlChannel = controlChannel;
    this.dataChannel = dataChannel;

    if (this.dataChannel) {
      this.dataChannel.binaryType = 'arraybuffer';
      if (!this.backpressure) {
        this.backpressure = new BackpressureController(this.dataChannel, { sctpMaxMessageSize: sctpMax });
      } else {
        this.backpressure.updateDataChannel(this.dataChannel, sctpMax);
      }
    }
  }

  sendControlMessage(msg) {
    if (this.controlChannel && this.controlChannel.readyState === 'open') {
      try {
        this.controlChannel.send(JSON.stringify(msg));
        return true;
      } catch (err) {
        console.warn('[SenderPipeline] Error sending control message:', err);
      }
    }
    return false;
  }

  handleControlMessage(msg) {
    switch (msg.type) {
      case ControlMessageType.ACK: {
        const prevConfirmed = this.confirmedBytes;
        this.confirmedBytes = Math.max(this.confirmedBytes, msg.receivedBytes || 0);

        if (this.diagnostics) {
          this.diagnostics.updateProgress({
            queuedBytes: this.bytesQueued,
            confirmedBytes: this.confirmedBytes,
            currentChunkSize: this.backpressure ? this.backpressure.getChunkSize() : 64 * 1024,
            backpressureWaitTimeMs: this.backpressure ? this.backpressure.totalWaitTimeMs : 0
          });
        }

        if (this.callbacks.onProgress) {
          const snapshot = this.diagnostics ? this.diagnostics.getSnapshot() : {};
          this.callbacks.onProgress({
            percent: this.totalSize > 0 ? Math.min(100, Math.floor((this.confirmedBytes / this.totalSize) * 100)) : 0,
            speedMB: snapshot.effectiveSpeedMB || 0,
            etaSeconds: snapshot.effectiveSpeedMB > 0 ? Math.ceil(((this.totalSize - this.confirmedBytes) / (1024 * 1024)) / snapshot.effectiveSpeedMB) : 0,
            transferredBytes: this.confirmedBytes,
            totalBytes: this.totalSize
          });
        }
        break;
      }

      case ControlMessageType.PAUSE: {
        console.warn('[SenderPipeline] Receiver requested pause (storage backpressure). Halting transmission...');
        this.isRemotePaused = true;
        break;
      }

      case ControlMessageType.RESUME: {
        console.log('[SenderPipeline] Receiver requested resume. Resuming transmission...');
        this.isRemotePaused = false;
        if (msg.resumeFrom !== undefined && msg.resumeFrom !== this.confirmedBytes) {
          console.log(`[SenderPipeline] Seeking to resume offset: ${msg.resumeFrom} B`);
          this.resumeOffset = msg.resumeFrom;
          this.confirmedBytes = msg.resumeFrom;
        }
        break;
      }

      case ControlMessageType.HEARTBEAT_ACK: {
        this.lastHeartbeatAckTime = performance.now();
        break;
      }

      case ControlMessageType.VERIFY_OK:
      case ControlMessageType.ACK_COMPLETE: {
        console.log('[SenderPipeline] ✅ Receiver confirmed file verification and complete receipt!');
        this.confirmedBytes = this.totalSize;
        this.stopHeartbeat();
        if (this.completionResolve) {
          this.completionResolve();
          this.completionResolve = null;
        }
        break;
      }

      case ControlMessageType.VERIFY_FAILED: {
        console.error('[SenderPipeline] ❌ Receiver reported verification failure:', msg.reason);
        this.stopHeartbeat();
        if (this.completionReject) {
          this.completionReject(new Error(`Receiver verification failed: ${msg.reason}`));
          this.completionReject = null;
        }
        break;
      }
    }
  }

  startHeartbeat() {
    this.stopHeartbeat();
    this.lastHeartbeatAckTime = performance.now();
    this.heartbeatInterval = setInterval(() => {
      if (!this.isStreamingActive) return;
      this.sendControlMessage({ type: ControlMessageType.HEARTBEAT, timestamp: Date.now() });

      // Check for heartbeat stall
      const timeSinceAck = performance.now() - this.lastHeartbeatAckTime;
      if (timeSinceAck > TransferDefaults.HEARTBEAT_TIMEOUT_MS) {
        console.warn(`[SenderPipeline] Heartbeat timeout: no ack for ${(timeSinceAck / 1000).toFixed(1)}s`);
        if (this.callbacks.onStall) {
          this.callbacks.onStall('Receiver responsiveness degraded (heartbeat timeout).');
        }
      }
    }, TransferDefaults.HEARTBEAT_INTERVAL_MS);
  }

  stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  /**
   * Main streaming transfer loop.
   * Strictly prevents concurrent duplicate runs.
   */
  async streamFile(file, resumeFrom = 0) {
    if (this.isStreamingActive) {
      console.warn('[SenderPipeline] streamFile already active. Ignoring duplicate invocation.');
      return;
    }

    if (!file) throw new Error('No file provided to stream');
    if (!this.dataChannel || this.dataChannel.readyState !== 'open') {
      throw new Error('DataChannel not open for streaming');
    }

    this.isStreamingActive = true;
    this.file = file;
    this.totalSize = file.size;
    this.fileId = `zap_${file.name}_${file.size}_${file.lastModified || 0}`;
    this.bytesQueued = resumeFrom;
    this.confirmedBytes = resumeFrom;
    this.resumeOffset = resumeFrom;
    this.abortController = new AbortController();

    if (this.backpressure) {
      this.backpressure.reset();
    }
    if (this.diagnostics) {
      this.diagnostics.start(this.totalSize);
    }

    this.startHeartbeat();

    // 1. Send file HEADER over control channel
    const initialChunkSize = this.backpressure ? this.backpressure.getChunkSize() : TransferDefaults.INITIAL_CHUNK_SIZE;
    this.sendControlMessage({
      type: ControlMessageType.HEADER,
      fileId: this.fileId,
      name: file.name,
      size: this.totalSize,
      mime: file.type || 'application/octet-stream',
      chunkSize: initialChunkSize,
      resumeOffset: this.resumeOffset
    });

    console.log(`[SenderPipeline] 🚀 Beginning streaming: "${file.name}" (${(this.totalSize / (1024 * 1024)).toFixed(2)} MB), resumeOffset: ${resumeFrom} B`);

    let fileOffset = resumeFrom;
    const blockSize = TransferDefaults.READ_BLOCK_SIZE; // 4 MB disk read slices

    const completionPromise = new Promise((resolve, reject) => {
      this.completionResolve = resolve;
      this.completionReject = reject;
    });

    try {
      while (fileOffset < this.totalSize) {
        if (this.abortController.signal.aborted) {
          throw new Error('Transfer cancelled');
        }

        // Handle receiver pause (disk write congestion)
        while (this.isRemotePaused) {
          if (this.abortController.signal.aborted) throw new Error('Transfer cancelled');
          await new Promise(r => setTimeout(r, 50));
        }

        // Read 4MB memory block slice
        const blockSlice = this.file.slice(fileOffset, Math.min(this.totalSize, fileOffset + blockSize));
        const blockBuffer = await blockSlice.arrayBuffer();
        fileOffset += blockBuffer.byteLength;

        let blockOffset = 0;
        const blockLen = blockBuffer.byteLength;

        while (blockOffset < blockLen) {
          if (this.abortController.signal.aborted) {
            throw new Error('Transfer cancelled');
          }

          // Backpressure check: wait if buffer exceeds high-water mark
          if (this.backpressure && this.backpressure.isCongested()) {
            await this.backpressure.waitUntilDrained(this.abortController.signal);
          }

          const currentChunkSize = this.backpressure ? this.backpressure.getChunkSize() : TransferDefaults.INITIAL_CHUNK_SIZE;
          const sliceSize = Math.min(currentChunkSize, blockLen - blockOffset);
          const chunk = blockBuffer.slice(blockOffset, blockOffset + sliceSize);

          // Verify channel state before sending
          if (this.dataChannel.readyState !== 'open') {
            throw new Error(`DataChannel closed (state: ${this.dataChannel.readyState})`);
          }

          // Send raw binary ArrayBuffer over dataChannel
          this.dataChannel.send(chunk);

          blockOffset += sliceSize;
          this.bytesQueued += sliceSize;

          if (this.diagnostics) {
            this.diagnostics.updateProgress({
              queuedBytes: this.bytesQueued,
              transportBytes: Math.max(0, this.bytesQueued - this.dataChannel.bufferedAmount),
              confirmedBytes: this.confirmedBytes,
              currentChunkSize,
              backpressureWaitTimeMs: this.backpressure ? this.backpressure.totalWaitTimeMs : 0
            });
          }
        }
      }

      // Flush remaining data in buffer
      console.log(`[SenderPipeline] All bytes sliced (${(this.bytesQueued / (1024 * 1024)).toFixed(2)} MB). Flushing DataChannel buffer...`);
      while (this.dataChannel && this.dataChannel.bufferedAmount > 0) {
        if (this.abortController.signal.aborted) throw new Error('Transfer cancelled');
        await new Promise(r => setTimeout(r, 20));
      }

      console.log('[SenderPipeline] DataChannel flushed! Sending EOF...');
      this.sendControlMessage({
        type: ControlMessageType.EOF,
        fileId: this.fileId,
        totalBytes: this.totalSize
      });

      // Wait for receiver verification & completion
      await Promise.race([
        completionPromise,
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Timeout waiting for receiver verification')), 30000);
        })
      ]);

      console.log('[SenderPipeline] 🏁 Transfer complete and verified!');
      return true;

    } catch (err) {
      console.error('[SenderPipeline] Streaming loop failed:', err);
      this.stopHeartbeat();
      throw err;
    } finally {
      this.isStreamingActive = false;
      this.stopHeartbeat();
    }
  }

  cancel() {
    if (this.abortController) {
      this.abortController.abort();
    }
    this.sendControlMessage({ type: ControlMessageType.CANCEL });
    this.isStreamingActive = false;
    this.stopHeartbeat();
    if (this.completionReject) {
      this.completionReject(new Error('Transfer cancelled'));
      this.completionReject = null;
    }
  }
}
