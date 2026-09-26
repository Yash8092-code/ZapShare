// ZapShare Transfer State Machine (V2.5 Explicit Lifecycle with Stop/Drain/Cancel States)
import { TransferState } from './constants.js';

export class TransferStateMachine {
  constructor(initialState = TransferState.IDLE, onStateChange = null) {
    this.currentState = initialState;
    this.onStateChange = onStateChange;
    this.stateHistory = [{ state: initialState, timestamp: performance.now(), reason: 'init' }];

    // Valid transitions map
    this.validTransitions = {
      [TransferState.IDLE]: [
        TransferState.PREPARING,
        TransferState.CONNECTING,
        TransferState.NEGOTIATING,
        TransferState.READY
      ],
      [TransferState.PREPARING]: [
        TransferState.CONNECTING,
        TransferState.NEGOTIATING,
        TransferState.READY,
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.CONNECTING]: [
        TransferState.NEGOTIATING,
        TransferState.READY,
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.TRANSFERRING,
        TransferState.RECONNECTING,
        TransferState.DISCONNECTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.NEGOTIATING]: [
        TransferState.READY,
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.DISCONNECTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.READY]: [
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.TRANSFERRING,
        TransferState.STOP_REQUESTED,
        TransferState.DISCONNECTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED,
        TransferState.RECONNECTING
      ],
      [TransferState.WAITING_FOR_ACCEPT]: [
        TransferState.TRANSFERRING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED,
        TransferState.IDLE
      ],
      [TransferState.TRANSFERRING]: [
        TransferState.BACKPRESSURED,
        TransferState.PAUSED_BACKPRESSURE,
        TransferState.PAUSING,
        TransferState.PAUSED,
        TransferState.STOP_REQUESTED,
        TransferState.DRAINING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.EOF_SENT,
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.DISCONNECTED,
        TransferState.RESUMING,
        TransferState.RECONNECTING,
        TransferState.FAILED
      ],
      [TransferState.BACKPRESSURED]: [
        TransferState.TRANSFERRING,
        TransferState.PAUSING,
        TransferState.PAUSED,
        TransferState.STOP_REQUESTED,
        TransferState.DRAINING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.EOF_SENT,
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.DISCONNECTED,
        TransferState.FAILED
      ],
      [TransferState.PAUSED_BACKPRESSURE]: [
        TransferState.TRANSFERRING,
        TransferState.BACKPRESSURED,
        TransferState.PAUSING,
        TransferState.PAUSED,
        TransferState.STOP_REQUESTED,
        TransferState.DRAINING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.EOF_SENT,
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.DISCONNECTED,
        TransferState.FAILED
      ],
      [TransferState.PAUSING]: [
        TransferState.PAUSED,
        TransferState.TRANSFERRING,
        TransferState.STOP_REQUESTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.PAUSED]: [
        TransferState.TRANSFERRING,
        TransferState.BACKPRESSURED,
        TransferState.STOP_REQUESTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.STOP_REQUESTED]: [
        TransferState.DRAINING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.DRAINING]: [
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.COMPLETED,
        TransferState.FAILED
      ],
      [TransferState.CANCELLING]: [
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.EOF_SENT]: [
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.VERIFYING]: [
        TransferState.COMPLETED,
        TransferState.FAILED,
        TransferState.CANCELLING,
        TransferState.CANCELLED
      ],
      [TransferState.DISCONNECTED]: [
        TransferState.RESUMING,
        TransferState.CONNECTING,
        TransferState.RECONNECTING,
        TransferState.READY,
        TransferState.TRANSFERRING,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.RESUMING]: [
        TransferState.TRANSFERRING,
        TransferState.BACKPRESSURED,
        TransferState.DISCONNECTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.RECONNECTING]: [
        TransferState.READY,
        TransferState.TRANSFERRING,
        TransferState.BACKPRESSURED,
        TransferState.DISCONNECTED,
        TransferState.CANCELLING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.COMPLETED]: [
        TransferState.IDLE
      ],
      [TransferState.CANCELLED]: [
        TransferState.IDLE
      ],
      [TransferState.FAILED]: [
        TransferState.IDLE,
        TransferState.RESUMING,
        TransferState.RECONNECTING
      ]
    };
  }

  getState() {
    return this.currentState;
  }

  is(state) {
    return this.currentState === state;
  }

  isActive() {
    return (
      this.currentState === TransferState.TRANSFERRING ||
      this.currentState === TransferState.BACKPRESSURED ||
      this.currentState === TransferState.PAUSED_BACKPRESSURE ||
      this.currentState === TransferState.PAUSING ||
      this.currentState === TransferState.PAUSED ||
      this.currentState === TransferState.DRAINING ||
      this.currentState === TransferState.EOF_SENT ||
      this.currentState === TransferState.VERIFYING ||
      this.currentState === TransferState.RESUMING
    );
  }

  isStopping() {
    return (
      this.currentState === TransferState.STOP_REQUESTED ||
      this.currentState === TransferState.DRAINING ||
      this.currentState === TransferState.CANCELLING
    );
  }

  isTerminal() {
    return (
      this.currentState === TransferState.COMPLETED ||
      this.currentState === TransferState.CANCELLED ||
      this.currentState === TransferState.FAILED
    );
  }

  canTransitionTo(targetState) {
    if (this.currentState === targetState) return false;
    const allowed = this.validTransitions[this.currentState] || [];
    return allowed.includes(targetState);
  }

  transition(targetState, reason = '') {
    if (this.currentState === targetState) {
      return true; // No-op idempotent
    }

    if (!this.canTransitionTo(targetState)) {
      console.warn(`[StateMachine] Illegal transition attempted: ${this.currentState} -> ${targetState} (${reason})`);
      return false;
    }

    const prevState = this.currentState;
    this.currentState = targetState;
    const entry = { state: targetState, prevState, timestamp: performance.now(), reason };
    this.stateHistory.push(entry);

    if (this.stateHistory.length > 50) {
      this.stateHistory.shift();
    }

    console.log(`[StateMachine] Transition: %c${prevState}%c ➔ %c${targetState}%c (${reason})`,
      'color: #94A3B8; font-weight: bold;',
      'color: inherit;',
      'color: #06B6D4; font-weight: bold;',
      'color: #64748B; font-style: italic;'
    );

    if (this.onStateChange) {
      try {
        this.onStateChange(targetState, prevState, reason);
      } catch (err) {
        console.error('[StateMachine] Error in state change listener:', err);
      }
    }

    return true;
  }

  forceState(state, reason = 'force') {
    const prevState = this.currentState;
    this.currentState = state;
    this.stateHistory.push({ state, prevState, timestamp: performance.now(), reason });
    if (this.onStateChange) {
      this.onStateChange(state, prevState, reason);
    }
  }

  reset() {
    this.forceState(TransferState.IDLE, 'reset');
  }
}
