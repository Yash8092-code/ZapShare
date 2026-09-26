// ZapShare Cancellation & Stop Protocol Manager (Phases 3, 4, 25, 26, 33)
// Solves Problem 1: Sender stops but receiver continues receiving data
import { ControlMessageType, TransferState } from './constants.js';

export class CancellationManager {
  constructor(options = {}) {
    this.role = options.role || 'sender';
    this.stateMachine = options.stateMachine || null;
    this.sendControlFn = options.sendControlFn || (() => {});
    this.onCleanupFn = options.onCleanupFn || (() => {});
    this.diagnostics = options.diagnostics || null;

    this.isStopRequested = false;
    this.isCancelled = false;
    this.postCancelDataDroppedBytes = 0;
    this.stopHandshakeTimeout = null;

    this.pendingStopResolve = null;
    this.pendingStopReject = null;
  }

  setSendControl(fn) {
    this.sendControlFn = fn;
  }

  isStoppingOrCancelled() {
    return this.isStopRequested || this.isCancelled;
  }

  // --- SENDER STOP INITIATION ---
  async requestStop(currentStats = {}) {
    if (this.isStopRequested || this.isCancelled) return;
    this.isStopRequested = true;

    console.log('[CancellationManager] 🛑 Initiating graceful transfer stop protocol...');
    if (this.stateMachine) {
      this.stateMachine.transition(TransferState.STOP_REQUESTED, 'user-stop-requested');
    }

    // Step 1: Send STOP_REQUEST to peer with current queued byte counts
    this.sendControlFn({
      type: ControlMessageType.STOP_REQUEST,
      bytesQueued: currentStats.bytesQueued || 0,
      timestamp: Date.now()
    });

    // Step 2: Await STOP_ACK or timeout fallback
    return new Promise((resolve) => {
      this.pendingStopResolve = resolve;

      this.stopHandshakeTimeout = setTimeout(() => {
        console.warn('[CancellationManager] Timeout waiting for STOP_ACK from peer. Forcing termination.');
        this.finishCancellation(currentStats.bytesConfirmed || 0);
        resolve({ confirmedBytes: currentStats.bytesConfirmed || 0, timedOut: true });
      }, 3000);
    });
  }

  // Handle incoming control protocol messages for Stop/Cancel
  async handleControlMessage(msg) {
    switch (msg.type) {
      case ControlMessageType.STOP_REQUEST: {
        console.log('[CancellationManager] Peer sent STOP_REQUEST. Stopping chunk ingestion and committing current writes...');
        this.isStopRequested = true;
        if (this.stateMachine) {
          this.stateMachine.transition(TransferState.STOP_REQUESTED, 'peer-stop-requested');
        }

        // Return STOP_ACK with confirmed checkpoint
        if (typeof this.onCleanupFn === 'function') {
          const result = await this.onCleanupFn('stop-requested');
          this.sendControlFn({
            type: ControlMessageType.STOP_ACK,
            confirmedBytes: result?.confirmedBytes || 0,
            checkpoint: result?.checkpoint || 0,
            state: 'STOPPED'
          });
        }
        break;
      }

      case ControlMessageType.STOP_ACK: {
        console.log(`[CancellationManager] Received STOP_ACK from receiver. Confirmed bytes: ${msg.confirmedBytes}`);
        if (this.stopHandshakeTimeout) {
          clearTimeout(this.stopHandshakeTimeout);
          this.stopHandshakeTimeout = null;
        }

        if (this.stateMachine) {
          this.stateMachine.transition(TransferState.CANCELLING, 'stop-ack-received');
        }

        // Send final CANCEL confirmation
        this.sendControlFn({
          type: ControlMessageType.CANCEL,
          finalConfirmedBytes: msg.confirmedBytes
        });

        this.finishCancellation(msg.confirmedBytes);

        if (this.pendingStopResolve) {
          this.pendingStopResolve({ confirmedBytes: msg.confirmedBytes, timedOut: false });
          this.pendingStopResolve = null;
        }
        break;
      }

      case ControlMessageType.CANCEL: {
        console.log('[CancellationManager] Received CANCEL. Acknowledging with CANCEL_ACK...');
        this.sendControlFn({ type: ControlMessageType.CANCEL_ACK });
        this.finishCancellation(msg.finalConfirmedBytes || 0);
        break;
      }

      case ControlMessageType.CANCEL_ACK: {
        console.log('[CancellationManager] Received CANCEL_ACK. Protocol handshake complete.');
        this.finishCancellation();
        break;
      }
    }
  }

  // Receiver Chunk Gatekeeper: prevents phantom data writes after stop/cancel (Phase 26)
  shouldAcceptChunk() {
    if (this.isStopRequested || this.isCancelled) {
      return false;
    }
    return true;
  }

  recordDroppedPostCancelData(byteLen) {
    this.postCancelDataDroppedBytes += byteLen;
    console.warn(`[CancellationManager] ⚠️ Received data after cancellation request: dropped ${byteLen} B (${(this.postCancelDataDroppedBytes / 1024).toFixed(1)} KB total dropped).`);
    if (this.diagnostics) {
      this.diagnostics.recordPostCancelDrop(this.postCancelDataDroppedBytes);
    }
  }

  finishCancellation(finalConfirmedBytes = 0) {
    if (this.isCancelled) return;
    this.isCancelled = true;

    if (this.stopHandshakeTimeout) {
      clearTimeout(this.stopHandshakeTimeout);
      this.stopHandshakeTimeout = null;
    }

    if (this.stateMachine) {
      this.stateMachine.transition(TransferState.CANCELLED, 'cancellation-complete');
    }

    console.log('[CancellationManager] Transfer cancelled cleanly. State settled.');
  }

  reset() {
    if (this.stopHandshakeTimeout) {
      clearTimeout(this.stopHandshakeTimeout);
      this.stopHandshakeTimeout = null;
    }
    this.isStopRequested = false;
    this.isCancelled = false;
    this.postCancelDataDroppedBytes = 0;
    this.pendingStopResolve = null;
    this.pendingStopReject = null;
  }
}
