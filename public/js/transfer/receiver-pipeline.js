// ZapShare Receiver Pipeline
// Streaming progressive disk sink, ACK checkpoint flow control, integrity verification, and resume
// Features strict post-cancel data dropping, adaptive checkpoints, and storage backpressure
import { ControlMessageType, TransferDefaults, StorageMode, TransferState } from './constants.js';
import { StorageAdapter } from './storage-adapter.js';
import { IntegrityManager } from './integrity-manager.js';
import { CancellationManager } from './cancellation-manager.js';
import { NetworkManager } from './network-manager.js';
import { PerformanceManager } from './performance-manager.js';
import { DeviceProfileManager } from './device-profile.js';

export class ReceiverPipeline {
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

    // Cancellation Manager (Phase 3 & Phase 26)
    this.cancellationManager = options.cancellationManager || new CancellationManager({
      role: 'receiver',
      stateMachine: this.stateMachine,
      sendControlFn: (msg) => this.sendControlMessage(msg),
      diagnostics: this.diagnostics,
      onCleanupFn: async () => {
        this.isReceiving = false;
        // Finish committing pending writes in queue before reporting confirmed checkpoint
        if (this.storageAdapter) {
          try {
            await this.storageAdapter.writeQueue;
          } catch (e) {}
        }
        const confirmed = this.storageAdapter ? this.storageAdapter.bytesWritten : this.receivedBytes;
        return {
          confirmedBytes: confirmed,
          checkpoint: confirmed
        };
      }
    });

    this.fileMeta = null;
    this.fileId = null;
    this.totalBytes = 0;
    this.receivedBytes = 0;
    this.lastAckedBytes = 0;
    this.ackIntervalBytes = options.ackIntervalBytes || TransferDefaults.ACK_INTERVAL_BYTES;

    this.storageAdapter = null;
    this.integrityManager = null;
    this.isReceiving = false;
    this.isStoragePaused = false;
    this.customFileHandle = options.fileHandle || null;

    this.sessionCheckpointKey = null;
  }

  setChannels(controlChannel, dataChannel) {
    this.controlChannel = controlChannel;
    this.dataChannel = dataChannel;
    this.cancellationManager.setSendControl((msg) => this.sendControlMessage(msg));

    if (this.dataChannel) {
      this.dataChannel.binaryType = 'arraybuffer';
    }
  }

  setFileHandle(handle) {
    this.customFileHandle = handle;
  }

  sendControlMessage(msg) {
    if (this.controlChannel && this.controlChannel.readyState === 'open') {
      try {
        this.controlChannel.send(JSON.stringify(msg));
        return true;
      } catch (e) {
        console.warn('[ReceiverPipeline] Error sending control message:', e);
      }
    }
    return false;
  }

  async handleControlMessage(msg) {
    // 1. Give cancellation manager priority on stop/cancel handshake messages (Phase 3, 26)
    if (
      msg.type === ControlMessageType.STOP_REQUEST ||
      msg.type === ControlMessageType.CANCEL ||
      msg.type === ControlMessageType.CANCEL_ACK
    ) {
      await this.cancellationManager.handleControlMessage(msg);
      if (msg.type === ControlMessageType.CANCEL) {
        await this.cleanup();
        if (this.callbacks.onCancelled) {
          this.callbacks.onCancelled();
        }
      }
      return;
    }

    switch (msg.type) {
      case ControlMessageType.HEADER: {
        console.log(`[ReceiverPipeline] Received file header: "${msg.name}" (${(msg.size / (1024 * 1024)).toFixed(2)} MB)`);
        this.fileMeta = {
          name: msg.name,
          size: msg.size,
          type: msg.mime || 'application/octet-stream'
        };
        this.fileId = msg.fileId || `zap_${msg.name}_${msg.size}`;
        this.totalBytes = msg.size;
        this.receivedBytes = msg.resumeOffset || 0;
        this.lastAckedBytes = this.receivedBytes;
        this.sessionCheckpointKey = `zap_resume_${this.fileId}`;

        // Initialize streaming storage adapter with bounded queue
        const tuning = this.deviceProfile.getTuning();
        this.storageAdapter = new StorageAdapter(this.fileMeta, {
          fileHandle: this.customFileHandle,
          maxQueueBytes: tuning.maxInMemoryQueue
        });
        const storageMode = await this.storageAdapter.initialize();

        // Initialize integrity manager
        this.integrityManager = new IntegrityManager();

        this.isReceiving = true;
        this.cancellationManager.reset();

        if (this.diagnostics) {
          this.diagnostics.start(this.totalBytes);
          this.diagnostics.updateProgress({
            storageMode,
            bytesReceived: this.receivedBytes,
            bytesConfirmed: this.receivedBytes,
            deviceClass: this.deviceProfile.deviceClass,
            transferMode: this.deviceProfile.transferMode
          });
        }

        // Send initial ACK
        this.sendControlMessage({
          type: ControlMessageType.ACK,
          receivedBytes: this.receivedBytes,
          nextOffset: this.receivedBytes
        });
        break;
      }

      case ControlMessageType.HEARTBEAT: {
        this.sendControlMessage({
          type: ControlMessageType.HEARTBEAT_ACK,
          timestamp: Date.now()
        });
        break;
      }

      case ControlMessageType.EOF: {
        console.log(`[ReceiverPipeline] Received EOF. Expected: ${msg.totalBytes} B, Received: ${this.receivedBytes} B. Finalizing storage & verification...`);
        await this.handleEof();
        break;
      }
    }
  }

  /**
   * Process raw binary chunk from DataChannel
   * Zero-copy streaming directly to storage & integrity manager
   */
  async handleBinaryChunk(data) {
    let buffer = data;
    if (data instanceof Blob) {
      buffer = await data.arrayBuffer();
    }

    if (!(buffer instanceof ArrayBuffer)) {
      return;
    }

    const chunkLen = buffer.byteLength;

    // Phase 26 Gatekeeper: Drop any phantom data arriving after STOP_REQUEST or CANCEL
    if (!this.cancellationManager.shouldAcceptChunk()) {
      this.cancellationManager.recordDroppedPostCancelData(chunkLen);
      return;
    }

    if (!this.isReceiving || !this.storageAdapter) return;

    this.receivedBytes += chunkLen;

    // 1. Stream write to storage adapter (OPFS / FileSystem / bounded memory)
    this.storageAdapter.write(buffer);

    // 2. Stream chunk to integrity hasher
    if (this.integrityManager) {
      this.integrityManager.update(buffer);
    }

    // 3. Measure storage write latency & device strain (Phases 11, 20, 21)
    const storageLatency = this.storageAdapter.lastWriteLatencyMs || 0;
    this.performanceManager.recordProcessingEvent(1, storageLatency);

    // 4. Storage backpressure flow control: pause sender if disk falls behind (Phase 11)
    if (this.storageAdapter.isCongested() && !this.isStoragePaused) {
      this.isStoragePaused = true;
      console.warn(`[ReceiverPipeline] Disk writing buffer congested (${(this.storageAdapter.unwrittenBytes / 1024).toFixed(0)} KB queued). Pausing sender transmission...`);
      this.sendControlMessage({ type: ControlMessageType.PAUSE });
      if (this.stateMachine) {
        this.stateMachine.transition(TransferState.PAUSED, 'storage-congestion');
      }
    } else if (!this.storageAdapter.isCongested() && this.isStoragePaused) {
      this.isStoragePaused = false;
      console.log('[ReceiverPipeline] Disk writing caught up. Resuming sender transmission...');
      this.sendControlMessage({ type: ControlMessageType.RESUME, resumeFrom: this.receivedBytes });
      if (this.stateMachine && this.stateMachine.is(TransferState.PAUSED)) {
        this.stateMachine.transition(TransferState.TRANSFERRING, 'storage-cleared');
      }
    }

    // 5. Receiver ACK checkpoint with dynamic interval based on network speed (Phase 10)
    const dynamicAckInterval = this.networkManager.getAdaptiveCheckpointInterval();
    if (this.receivedBytes - this.lastAckedBytes >= dynamicAckInterval || this.receivedBytes >= this.totalBytes) {
      this.lastAckedBytes = this.receivedBytes;
      this.sendControlMessage({
        type: ControlMessageType.ACK,
        receivedBytes: this.receivedBytes,
        nextOffset: this.receivedBytes
      });

      // Update ephemeral resume checkpoint in sessionStorage
      try {
        if (this.sessionCheckpointKey && typeof sessionStorage !== 'undefined') {
          sessionStorage.setItem(this.sessionCheckpointKey, JSON.stringify({
            fileId: this.fileId,
            name: this.fileMeta.name,
            totalBytes: this.totalBytes,
            receivedBytes: this.receivedBytes,
            timestamp: Date.now()
          }));
        }
      } catch (e) {}
    }

    // 6. Update metrics & diagnostics
    const confirmedBytes = this.storageAdapter ? this.storageAdapter.bytesWritten : this.receivedBytes;
    if (this.diagnostics) {
      this.diagnostics.updateProgress({
        bytesReceived: this.receivedBytes,
        bytesConfirmed: confirmedBytes,
        queueSize: this.storageAdapter.unwrittenBytes,
        storageLatencyMs: storageLatency,
        networkClass: this.networkManager.networkClass,
        bottleneck: this.networkManager.bottleneck,
        deviceClass: this.deviceProfile.deviceClass,
        transferMode: this.deviceProfile.transferMode
      });
    }

    // 7. Throttled UI Progress updates (Phase 15, 29)
    if (this.callbacks.onProgress && this.performanceManager.shouldUpdateUi()) {
      const snapshot = this.diagnostics ? this.diagnostics.getSnapshot() : {};
      const effectiveSpeed = snapshot.effectiveSpeedMB || 0;
      this.callbacks.onProgress({
        percent: this.totalBytes > 0 ? Math.min(100, Math.floor((confirmedBytes / this.totalBytes) * 100)) : 0,
        speedMB: effectiveSpeed,
        etaSeconds: effectiveSpeed > 0 ? Math.ceil(((this.totalBytes - confirmedBytes) / (1024 * 1024)) / effectiveSpeed) : 0,
        transferredBytes: confirmedBytes,
        totalBytes: this.totalBytes,
        statusText: this.networkManager.getStatusMessage(this.stateMachine?.getState(), false),
        bytesReceived: this.receivedBytes,
        bytesConfirmed: confirmedBytes
      });
    }
  }

  async handleEof() {
    this.isReceiving = false;

    if (this.callbacks.onVerifying) {
      this.callbacks.onVerifying('Verifying transfer integrity & finalizing storage...');
    }

    try {
      // 1. Finalize storage adapter
      const storageResult = await this.storageAdapter.finalize();

      // 2. Finalize cryptographic hash
      const actualHash = this.integrityManager ? await this.integrityManager.finalize() : null;

      // 3. Verify file size
      const verification = IntegrityManager.verifyTransfer({
        expectedName: this.fileMeta?.name,
        receivedName: this.fileMeta?.name,
        expectedSize: this.totalBytes,
        receivedSize: this.receivedBytes,
        expectedHash: null,
        actualHash
      });

      if (!verification.sizeOk) {
        console.error('[ReceiverPipeline] ❌ Verification failed:', verification.reason);
        this.sendControlMessage({
          type: ControlMessageType.VERIFY_FAILED,
          reason: verification.reason
        });
        if (this.callbacks.onError) {
          this.callbacks.onError(`Transfer verification failed: ${verification.reason}`);
        }
        return;
      }

      console.log('[ReceiverPipeline] ✅ Verification passed! Notifying sender of completion...');
      this.sendControlMessage({
        type: ControlMessageType.VERIFY_OK,
        checksum: actualHash
      });
      this.sendControlMessage({
        type: ControlMessageType.ACK_COMPLETE
      });

      // Clear ephemeral resume checkpoint
      try {
        if (this.sessionCheckpointKey && typeof sessionStorage !== 'undefined') {
          sessionStorage.removeItem(this.sessionCheckpointKey);
        }
      } catch (e) {}

      // Trigger completion callback
      if (this.callbacks.onComplete) {
        this.callbacks.onComplete({
          role: 'receiver',
          meta: this.fileMeta,
          blob: storageResult.blob,
          file: storageResult.file,
          downloadUrl: storageResult.downloadUrl,
          isDirectlySaved: storageResult.isDirectlySaved,
          storageMode: storageResult.mode,
          bytesWritten: storageResult.bytesWritten,
          verified: true
        });
      }
    } catch (err) {
      console.error('[ReceiverPipeline] Error finalizing transfer:', err);
      this.sendControlMessage({
        type: ControlMessageType.VERIFY_FAILED,
        reason: err.message || 'Storage finalization error'
      });
      if (this.callbacks.onError) {
        this.callbacks.onError('Error saving completed file: ' + err.message);
      }
    }
  }

  async cleanup() {
    this.isReceiving = false;
    this.isStoragePaused = false;
    if (this.storageAdapter) {
      await this.storageAdapter.cleanup();
      this.storageAdapter = null;
    }
    if (this.integrityManager) {
      this.integrityManager.destroy();
      this.integrityManager = null;
    }
    this.cancellationManager.finishCancellation();
  }
}
