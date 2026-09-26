// ZapShare Transfer Constants & Protocol Specification

export const TransferState = Object.freeze({
  IDLE: 'IDLE',
  PREPARING: 'PREPARING',
  CONNECTING: 'CONNECTING',
  READY: 'READY',
  WAITING_FOR_ACCEPT: 'WAITING_FOR_ACCEPT',
  TRANSFERRING: 'TRANSFERRING',
  PAUSED_BACKPRESSURE: 'PAUSED_BACKPRESSURE',
  VERIFYING: 'VERIFYING',
  COMPLETED: 'COMPLETED',
  CANCELLED: 'CANCELLED',
  FAILED: 'FAILED',
  RECONNECTING: 'RECONNECTING'
});

export const ControlMessageType = Object.freeze({
  HEADER: 'HEADER',
  ACK: 'ACK',
  CHECKPOINT: 'CHECKPOINT',
  HEARTBEAT: 'HEARTBEAT',
  HEARTBEAT_ACK: 'HEARTBEAT_ACK',
  PAUSE: 'PAUSE',
  RESUME: 'RESUME',
  CANCEL: 'CANCEL',
  EOF: 'EOF',
  VERIFY: 'VERIFY',
  VERIFY_OK: 'VERIFY_OK',
  VERIFY_FAILED: 'VERIFY_FAILED',
  ACK_COMPLETE: 'ACK_COMPLETE'
});

export const StorageMode = Object.freeze({
  FILE_SYSTEM_ACCESS: 'FILE_SYSTEM_ACCESS',
  OPFS: 'OPFS',
  MEMORY: 'MEMORY'
});

export const RouteType = Object.freeze({
  LAN: 'Direct LAN (Local P2P)',
  STUN: 'Direct P2P (STUN)',
  TURN: 'Relayed via TURN',
  UNKNOWN: 'Negotiating route...'
});

export const TransferDefaults = Object.freeze({
  INITIAL_CHUNK_SIZE: 64 * 1024,      // 64 KB initial chunk size
  MIN_CHUNK_SIZE: 32 * 1024,          // 32 KB minimum chunk size
  MAX_CHUNK_SIZE: 128 * 1024,         // 128 KB maximum chunk size
  HIGH_WATER_MARK: 2 * 1024 * 1024,   // 2 MB backpressure pause ceiling
  LOW_WATER_MARK: 512 * 1024,         // 512 KB backpressure resume threshold
  ACK_INTERVAL_BYTES: 2 * 1024 * 1024,// 2 MB receiver ACK checkpoint interval
  MAX_IN_MEMORY_QUEUE_BYTES: 16 * 1024 * 1024, // 16 MB max unwritten storage buffer
  HEARTBEAT_INTERVAL_MS: 5000,        // 5 seconds control channel heartbeat
  HEARTBEAT_TIMEOUT_MS: 15000,        // 15 seconds without heartbeat response is stalled
  STALL_DETECTION_WINDOW_MS: 15000,   // 15 seconds without byte progress triggers pipeline stall
  READ_BLOCK_SIZE: 4 * 1024 * 1024    // 4 MB file disk-slice read buffer
});
