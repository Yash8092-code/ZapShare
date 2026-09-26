// ZapShare Transfer State Machine
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
        TransferState.READY
      ],
      [TransferState.PREPARING]: [
        TransferState.CONNECTING,
        TransferState.READY,
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.CONNECTING]: [
        TransferState.READY,
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.TRANSFERRING,
        TransferState.RECONNECTING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.READY]: [
        TransferState.WAITING_FOR_ACCEPT,
        TransferState.TRANSFERRING,
        TransferState.CANCELLED,
        TransferState.FAILED,
        TransferState.RECONNECTING
      ],
      [TransferState.WAITING_FOR_ACCEPT]: [
        TransferState.TRANSFERRING,
        TransferState.CANCELLED,
        TransferState.FAILED,
        TransferState.IDLE
      ],
      [TransferState.TRANSFERRING]: [
        TransferState.PAUSED_BACKPRESSURE,
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.RECONNECTING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.PAUSED_BACKPRESSURE]: [
        TransferState.TRANSFERRING,
        TransferState.VERIFYING,
        TransferState.COMPLETED,
        TransferState.RECONNECTING,
        TransferState.CANCELLED,
        TransferState.FAILED
      ],
      [TransferState.VERIFYING]: [
        TransferState.COMPLETED,
        TransferState.FAILED,
        TransferState.CANCELLED
      ],
      [TransferState.RECONNECTING]: [
        TransferState.READY,
        TransferState.TRANSFERRING,
        TransferState.PAUSED_BACKPRESSURE,
        TransferState.FAILED,
        TransferState.CANCELLED
      ],
      [TransferState.COMPLETED]: [
        TransferState.IDLE
      ],
      [TransferState.CANCELLED]: [
        TransferState.IDLE
      ],
      [TransferState.FAILED]: [
        TransferState.IDLE,
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
      this.currentState === TransferState.PAUSED_BACKPRESSURE ||
      this.currentState === TransferState.VERIFYING
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
