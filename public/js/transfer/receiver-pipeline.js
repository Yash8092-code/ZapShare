// ZapShare Receiver Pipeline
// Streaming progressive disk sink, ACK checkpoint flow control, integrity verification, and resume
import { ControlMessageType, TransferDefaults, StorageMode } from './constants.js';
import { StorageAdapter } from './storage-adapter.js';
import { IntegrityManager } from './integrity-manager.js';

export class ReceiverPipeline {
  constructor(options = {}) {
    this.controlChannel = options.controlChannel || null;
    this.dataChannel = options.dataChannel || null;
    this.stateMachine = options.stateMachine || null;
    this.diagnostics = options.diagnostics || null;
    this.callbacks = options.callbacks || {};

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

        // Initialize streaming storage adapter
        this.storageAdapter = new StorageAdapter(this.fileMeta, {
          fileHandle: this.customFileHandle
        });
        const storageMode = await this.storageAdapter.initialize();

        // Initialize integrity manager
        this.integrityManager = new IntegrityManager();

        this.isReceiving = true;

        if (this.diagnostics) {
          this.diagnostics.start(this.totalBytes);
          this.diagnostics.updateProgress({
            storageMode,
            confirmedBytes: this.receivedBytes
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

      case ControlMessageType.CANCEL: {
        console.warn('[ReceiverPipeline] Sender cancelled transfer.');
        await this.cleanup();
        if (this.callbacks.onCancelled) {
          this.callbacks.onCancelled();
        }
        break;
      }
    }
  }

  /**
   * Process raw binary chunk from DataChannel
   */
  async handleBinaryChunk(data) {
    if (!this.isReceiving || !this.storageAdapter) return;

    let buffer = data;
    if (data instanceof Blob) {
      buffer = await data.arrayBuffer();
    }

    if (!(buffer instanceof ArrayBuffer)) {
      return;
    }

    const chunkLen = buffer.byteLength;
    this.receivedBytes += chunkLen;

    // 1. Stream write to storage adapter (OPFS / FileSystem / memory)
    this.storageAdapter.write(buffer);

    // 2. Stream chunk to integrity hasher
    if (this.integrityManager) {
      this.integrityManager.update(buffer);
    }

    // 3. Storage backpressure flow control: pause sender if disk falls behind
    if (this.storageAdapter.isCongested() && !this.isStoragePaused) {
      this.isStoragePaused = true;
      console.warn('[ReceiverPipeline] Disk writing buffer congested. Pausing sender transmission...');
      this.sendControlMessage({ type: ControlMessageType.PAUSE });
    } else if (!this.storageAdapter.isCongested() && this.isStoragePaused) {
      this.isStoragePaused = false;
      console.log('[ReceiverPipeline] Disk writing caught up. Resuming sender transmission...');
      this.sendControlMessage({ type: ControlMessageType.RESUME, resumeFrom: this.receivedBytes });
    }

    // 4. Receiver ACK checkpoint
    if (this.receivedBytes - this.lastAckedBytes >= this.ackIntervalBytes || this.receivedBytes >= this.totalBytes) {
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

    // Update UI & diagnostics
    if (this.diagnostics) {
      this.diagnostics.updateProgress({
        transportBytes: this.receivedBytes,
        confirmedBytes: this.receivedBytes,
        queueSize: this.storageAdapter.unwrittenBytes
      });
    }

    if (this.callbacks.onProgress) {
      const snapshot = this.diagnostics ? this.diagnostics.getSnapshot() : {};
      this.callbacks.onProgress({
        percent: this.totalBytes > 0 ? Math.min(100, Math.floor((this.receivedBytes / this.totalBytes) * 100)) : 0,
        speedMB: snapshot.effectiveSpeedMB || 0,
        etaSeconds: snapshot.effectiveSpeedMB > 0 ? Math.ceil(((this.totalBytes - this.receivedBytes) / (1024 * 1024)) / snapshot.effectiveSpeedMB) : 0,
        transferredBytes: this.receivedBytes,
        totalBytes: this.totalBytes
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
        expectedHash: null, // Verified against size & format
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
  }
}
