// ZapShare Sender Pipeline
// High-throughput, backpressure-governed file transmission engine
// Features 5-stage byte tracking, adaptive chunking, mobile profiles, and graceful stop/cancel handshake
import { ControlMessageType, TransferDefaults, TransferState } from './constants.js';
import { BackpressureController } from './backpressure-controller.js';
import { IntegrityManager } from './integrity-manager.js';
import { DeviceProfileManager } from './device-profile.js';
import { CancellationManager } from './cancellation-manager.js';
import { NetworkManager } from './network-manager.js';
import { PerformanceManager } from './performance-manager.js';

export class SenderPipeline {
  constructor(options = {}) {
    this.controlChannel = options.controlChannel || null;
    this.dataChannel = options.dataChannel || null;
    this.stateMachine = options.stateMachine || null;
    this.diagnostics = options.diagnostics || null;
    this.callbacks = options.callbacks || {};

    // Device, Network, and Performance Managers
    this.deviceProfile = options.deviceProfile || new DeviceProfileManager();
    this.networkManager = options.networkManager || new NetworkManager();
    this.performanceManager = options.performanceManager || new PerformanceManager({
      deviceProfile: this.deviceProfile,
      uiThrottleMs: this.deviceProfile.getTuning().uiThrottleMs,
      callbacks: {
        onDeviceStress: (msg) => {
          if (this.callbacks.onDeviceStress) this.callbacks.onDeviceStress(msg);
        }
      }
    });

    // Cancellation Manager (Phase 3 & 4)
    this.cancellationManager = options.cancellationManager || new CancellationManager({
      role: 'sender',
      stateMachine: this.stateMachine,
      sendControlFn: (msg) => this.sendControlMessage(msg),
      diagnostics: this.diagnostics
    });

    this.file = null;
    this.fileId = null;
    this.totalSize = 0;

    // The Five Independent Byte Counters (Phase 2)
    this.bytesRead = 0;        // Sliced from source File
    this.bytesQueued = 0;      // Handed to RTCDataChannel.send()
    this.bytesTransported = 0; // Exited WebRTC buffer (bytesQueued - bufferedAmount)
    this.bytesReceived = 0;    // Acknowledged as received by peer
    this.confirmedBytes = 0;   // Acknowledged as written/verified by peer

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
    this.cancellationManager.setSendControl((msg) => this.sendControlMessage(msg));

    if (this.dataChannel) {
      this.dataChannel.binaryType = 'arraybuffer';
      const tuning = this.deviceProfile.getTuning(sctpMax);

      if (!this.backpressure) {
        this.backpressure = new BackpressureController(this.dataChannel, {
          initialChunkSize: tuning.initialChunk,
          minChunkSize: tuning.minChunk,
          maxChunkSize: tuning.maxChunk,
          highWaterMark: tuning.highWater,
          lowWaterMark: tuning.lowWater,
          sctpMaxMessageSize: sctpMax
        });
      } else {
        this.backpressure.updateDataChannel(this.dataChannel, sctpMax);
        this.backpressure.applyTuning(tuning);
      }
      this.performanceManager.setBackpressureController(this.backpressure);
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

  async handleControlMessage(msg) {
    // 1. Give cancellation manager priority on stop/cancel handshake messages
    if (
      msg.type === ControlMessageType.STOP_ACK ||
      msg.type === ControlMessageType.CANCEL_ACK ||
      msg.type === ControlMessageType.CANCEL
    ) {
      await this.cancellationManager.handleControlMessage(msg);
      return;
    }

    switch (msg.type) {
      case ControlMessageType.ACK: {
        const prevConfirmed = this.confirmedBytes;
        this.bytesReceived = Math.max(this.bytesReceived, msg.receivedBytes || 0);
        this.confirmedBytes = Math.max(this.confirmedBytes, msg.receivedBytes || 0);
        this.bytesTransported = Math.max(0, this.bytesQueued - (this.dataChannel ? this.dataChannel.bufferedAmount : 0));

        // Update NetworkManager metrics for bottleneck & network classification
        const snapshot = this.diagnostics ? this.diagnostics.getSnapshot() : {};
        this.networkManager.updateMetrics({
          confirmedSpeedMB: snapshot.effectiveSpeedMB || 0,
          rttMs: snapshot.rttMs || null,
          backpressureWaitTimeMs: this.backpressure ? this.backpressure.totalWaitTimeMs : 0,
          bufferedAmount: this.dataChannel ? this.dataChannel.bufferedAmount : 0,
          storageLatencyMs: snapshot.storageLatencyMs || 0
        });

        if (this.diagnostics) {
          this.diagnostics.updateProgress({
            bytesRead: this.bytesRead,
            bytesQueued: this.bytesQueued,
            bytesTransported: this.bytesTransported,
            bytesReceived: this.bytesReceived,
            bytesConfirmed: this.confirmedBytes,
            currentChunkSize: this.backpressure ? this.backpressure.getChunkSize() : 64 * 1024,
            backpressureWaitTimeMs: this.backpressure ? this.backpressure.totalWaitTimeMs : 0,
            networkClass: this.networkManager.networkClass,
            bottleneck: this.networkManager.bottleneck,
            deviceClass: this.deviceProfile.deviceClass,
            transferMode: this.deviceProfile.transferMode
          });
        }

        // Throttled UI Progress updates (Phase 15, 29)
        // Progress is strictly based on bytesConfirmed / totalSize
        if (this.callbacks.onProgress && this.performanceManager.shouldUpdateUi()) {
          const effectiveSpeed = snapshot.effectiveSpeedMB || 0;
          this.callbacks.onProgress({
            percent: this.totalSize > 0 ? Math.min(100, Math.floor((this.confirmedBytes / this.totalSize) * 100)) : 0,
            speedMB: effectiveSpeed,
            etaSeconds: effectiveSpeed > 0 ? Math.ceil(((this.totalSize - this.confirmedBytes) / (1024 * 1024)) / effectiveSpeed) : 0,
            transferredBytes: this.confirmedBytes,
            totalBytes: this.totalSize,
            statusText: this.networkManager.getStatusMessage(this.stateMachine?.getState(), true),
            // The 5 granular byte counters exposed for UI/Diagnostics
            bytesRead: this.bytesRead,
            bytesQueued: this.bytesQueued,
            bytesTransported: this.bytesTransported,
            bytesReceived: this.bytesReceived,
            bytesConfirmed: this.confirmedBytes
          });
        }
        break;
      }

      case ControlMessageType.PAUSE: {
        console.warn('[SenderPipeline] Receiver requested pause (storage backpressure). Halting transmission...');
        this.isRemotePaused = true;
        if (this.stateMachine) {
          this.stateMachine.transition(TransferState.PAUSED, 'receiver-storage-backpressure');
        }
        break;
      }

      case ControlMessageType.RESUME: {
        console.log('[SenderPipeline] Receiver requested resume. Resuming transmission...');
        this.isRemotePaused = false;
        if (this.stateMachine && this.stateMachine.is(TransferState.PAUSED)) {
          this.stateMachine.transition(TransferState.TRANSFERRING, 'receiver-resumed');
        }
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
        this.bytesReceived = this.totalSize;
        this.bytesTransported = this.totalSize;
        this.stopHeartbeat();

        if (this.callbacks.onProgress) {
          const snapshot = this.diagnostics ? this.diagnostics.getSnapshot() : {};
          this.callbacks.onProgress({
            percent: 100,
            speedMB: snapshot.effectiveSpeedMB || 0,
            etaSeconds: 0,
            transferredBytes: this.totalSize,
            totalBytes: this.totalSize,
            statusText: 'Transfer verified ✓',
            bytesRead: this.totalSize,
            bytesQueued: this.totalSize,
            bytesTransported: this.totalSize,
            bytesReceived: this.totalSize,
            bytesConfirmed: this.totalSize
          });
        }

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
   * Backpressure-governed, non-duplicating binary streaming with 5 byte counter tracking.
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

    // Initialize the 5 byte counters
    this.bytesRead = resumeFrom;
    this.bytesQueued = resumeFrom;
    this.bytesTransported = resumeFrom;
    this.bytesReceived = resumeFrom;
    this.confirmedBytes = resumeFrom;
    this.resumeOffset = resumeFrom;

    this.abortController = new AbortController();
    this.performanceManager.reset();

    if (this.backpressure) {
      this.backpressure.reset();
      const tuning = this.deviceProfile.getTuning();
      this.backpressure.applyTuning(tuning);
    }
    if (this.diagnostics) {
      this.diagnostics.start(this.totalSize);
      this.diagnostics.updateProgress({
        bytesRead: this.bytesRead,
        bytesQueued: this.bytesQueued,
        bytesTransported: this.bytesTransported,
        bytesReceived: this.bytesReceived,
        bytesConfirmed: this.confirmedBytes,
        deviceClass: this.deviceProfile.deviceClass,
        transferMode: this.deviceProfile.transferMode
      });
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

    if (this.stateMachine && this.stateMachine.canTransitionTo(TransferState.TRANSFERRING)) {
      this.stateMachine.transition(TransferState.TRANSFERRING, 'streaming-active');
    }

    const completionPromise = new Promise((resolve, reject) => {
      this.completionResolve = resolve;
      this.completionReject = reject;
    });
    completionPromise.catch(() => {});

    try {
      while (fileOffset < this.totalSize) {
        if (this.abortController.signal.aborted || this.cancellationManager.isStoppingOrCancelled()) {
          throw new Error('Transfer cancelled');
        }

        // Handle receiver pause (disk write congestion)
        while (this.isRemotePaused) {
          if (this.abortController.signal.aborted || this.cancellationManager.isStoppingOrCancelled()) {
            throw new Error('Transfer cancelled');
          }
          await new Promise(r => setTimeout(r, 50));
        }

        // Read 4MB memory block slice
        const sliceStart = performance.now();
        const blockSlice = this.file.slice(fileOffset, Math.min(this.totalSize, fileOffset + blockSize));
        const blockBuffer = await blockSlice.arrayBuffer();
        const sliceDuration = performance.now() - sliceStart;

        fileOffset += blockBuffer.byteLength;
        this.bytesRead = fileOffset;

        let blockOffset = 0;
        const blockLen = blockBuffer.byteLength;

        while (blockOffset < blockLen) {
          if (this.abortController.signal.aborted || this.cancellationManager.isStoppingOrCancelled()) {
            throw new Error('Transfer cancelled');
          }

          // Backpressure check: wait deterministically until buffer drains below low-water mark
          if (this.backpressure && this.backpressure.isCongested()) {
            await this.backpressure.waitUntilDrained(this.abortController.signal);
          }

          const currentChunkSize = this.backpressure ? this.backpressure.getChunkSize() : TransferDefaults.INITIAL_CHUNK_SIZE;
          const sliceSize = Math.min(currentChunkSize, blockLen - blockOffset);

          // Zero unnecessary duplicate copy: slice from blockBuffer directly
          const chunk = blockBuffer.slice(blockOffset, blockOffset + sliceSize);

          if (this.dataChannel.readyState !== 'open') {
            throw new Error(`DataChannel closed (state: ${this.dataChannel.readyState})`);
          }

          // Send raw binary ArrayBuffer over dataChannel
          this.dataChannel.send(chunk);

          blockOffset += sliceSize;
          this.bytesQueued += sliceSize;
          this.bytesTransported = Math.max(0, this.bytesQueued - this.dataChannel.bufferedAmount);

          // Record processing event for mobile thermal/strain heuristic
          this.performanceManager.recordProcessingEvent(sliceDuration / (blockLen / sliceSize));

          if (this.diagnostics) {
            this.diagnostics.updateProgress({
              bytesRead: this.bytesRead,
              bytesQueued: this.bytesQueued,
              bytesTransported: this.bytesTransported,
              bytesReceived: this.bytesReceived,
              bytesConfirmed: this.confirmedBytes,
              currentChunkSize,
              backpressureWaitTimeMs: this.backpressure ? this.backpressure.totalWaitTimeMs : 0
            });
          }
        }
      }

      // Flush remaining data in buffer
      console.log(`[SenderPipeline] All bytes sliced (${(this.bytesQueued / (1024 * 1024)).toFixed(2)} MB). Flushing DataChannel buffer...`);
      while (this.dataChannel && this.dataChannel.bufferedAmount > 0) {
        if (this.abortController.signal.aborted || this.cancellationManager.isStoppingOrCancelled()) {
          throw new Error('Transfer cancelled');
        }
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
      if (err.message === 'Transfer cancelled' || this.cancellationManager.isStoppingOrCancelled()) {
        console.log('[SenderPipeline] Transfer stopped cleanly.');
      } else {
        console.error('[SenderPipeline] Streaming loop failed:', err);
      }
      this.stopHeartbeat();
      throw err;
    } finally {
      this.isStreamingActive = false;
      this.stopHeartbeat();
    }
  }

  /**
   * Graceful Stop & Cancellation Protocol (Phases 1, 3, 4, 25)
   * Prevents Problem 1: Receiver continues receiving data after sender stops
   */
  async requestStop() {
    console.log('[SenderPipeline] requestStop invoked. Halting local file reading & initiating handshake...');
    if (this.abortController) {
      this.abortController.abort();
    }
    this.isStreamingActive = false;
    this.stopHeartbeat();

    const stopStats = {
      bytesRead: this.bytesRead,
      bytesQueued: this.bytesQueued,
      bytesConfirmed: this.confirmedBytes,
      bufferedAmount: this.dataChannel ? this.dataChannel.bufferedAmount : 0
    };

    const handshakeResult = await this.cancellationManager.requestStop(stopStats);

    if (this.completionReject) {
      const rejectFn = this.completionReject;
      this.completionReject = null;
      try { rejectFn(new Error('Transfer cancelled')); } catch (e) {}
    }

    return handshakeResult;
  }

  cancel() {
    this.requestStop().catch(() => {});
  }
}
