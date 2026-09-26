// ZapShare Transfer Constants & Protocol Specification (V2.5 Mobile & Cancellation Architecture)

export const TransferState = Object.freeze({
  IDLE: 'IDLE',
  CONNECTING: 'CONNECTING',
  NEGOTIATING: 'NEGOTIATING',
  READY: 'READY',
  WAITING_FOR_ACCEPT: 'WAITING_FOR_ACCEPT', // UI pairing alias
  PREPARING: 'PREPARING',                 // Legacy alias
  TRANSFERRING: 'TRANSFERRING',
  BACKPRESSURED: 'BACKPRESSURED',
  PAUSED_BACKPRESSURE: 'PAUSED_BACKPRESSURE', // Backward-compatible alias
  PAUSING: 'PAUSING',
  PAUSED: 'PAUSED',
  STOP_REQUESTED: 'STOP_REQUESTED',
  DRAINING: 'DRAINING',
  CANCELLING: 'CANCELLING',
  CANCELLED: 'CANCELLED',
  EOF_SENT: 'EOF_SENT',
  VERIFYING: 'VERIFYING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
  DISCONNECTED: 'DISCONNECTED',
  RESUMING: 'RESUMING',
  RECONNECTING: 'RECONNECTING' // Backward-compatible alias
});

export const ControlMessageType = Object.freeze({
  HEADER: 'HEADER',
  ACK: 'ACK',
  CHECKPOINT: 'CHECKPOINT',
  HEARTBEAT: 'HEARTBEAT',
  HEARTBEAT_ACK: 'HEARTBEAT_ACK',
  PAUSE: 'PAUSE',
  RESUME: 'RESUME',
  STOP_REQUEST: 'STOP_REQUEST',
  STOP_ACK: 'STOP_ACK',
  DRAIN_COMPLETE: 'DRAIN_COMPLETE',
  CANCEL: 'CANCEL',
  CANCEL_ACK: 'CANCEL_ACK',
  EOF: 'EOF',
  VERIFY: 'VERIFY',
  VERIFY_OK: 'VERIFY_OK',
  VERIFY_FAILED: 'VERIFY_FAILED',
  ACK_COMPLETE: 'ACK_COMPLETE',
  ERROR: 'ERROR'
});

export const TransferMode = Object.freeze({
  MAXIMUM_SPEED: 'MAXIMUM_SPEED',
  BALANCED: 'BALANCED',
  DEVICE_FRIENDLY: 'DEVICE_FRIENDLY'
});

export const NetworkClass = Object.freeze({
  VERY_SLOW: 'VERY_SLOW',
  SLOW: 'SLOW',
  MODERATE: 'MODERATE',
  FAST: 'FAST',
  VERY_FAST: 'VERY_FAST'
});

export const BottleneckType = Object.freeze({
  NONE: 'NONE',
  NETWORK: 'NETWORK',
  RECEIVER_STORAGE: 'RECEIVER_STORAGE',
  CPU: 'CPU',
  BUFFER_PRESSURE: 'BUFFER_PRESSURE',
  SIGNALING_ICE: 'SIGNALING_ICE',
  UNKNOWN: 'UNKNOWN'
});

export const FailureCategory = Object.freeze({
  ICE_FAILURE: 'ICE_FAILURE',
  DATACHANNEL_FAILURE: 'DATACHANNEL_FAILURE',
  NETWORK_TIMEOUT: 'NETWORK_TIMEOUT',
  BACKPRESSURE_TIMEOUT: 'BACKPRESSURE_TIMEOUT',
  STORAGE_FAILURE: 'STORAGE_FAILURE',
  HASH_FAILURE: 'HASH_FAILURE',
  CANCELLED: 'CANCELLED',
  REMOTE_CANCELLED: 'REMOTE_CANCELLED',
  PROTOCOL_ERROR: 'PROTOCOL_ERROR',
  BROWSER_LIFECYCLE_INTERRUPTION: 'BROWSER_LIFECYCLE_INTERRUPTION',
  UNKNOWN: 'UNKNOWN'
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

export const DeviceClass = Object.freeze({
  DESKTOP: 'DESKTOP',
  MOBILE: 'MOBILE'
});

export const TransferDefaults = Object.freeze({
  INITIAL_CHUNK_SIZE: 64 * 1024,      // 64 KB default starting chunk
  MIN_CHUNK_SIZE: 32 * 1024,          // 32 KB minimum chunk size
  MAX_CHUNK_SIZE: 128 * 1024,         // 128 KB maximum chunk size
  HIGH_WATER_MARK: 2 * 1024 * 1024,   // 2 MB backpressure pause ceiling (Desktop)
  LOW_WATER_MARK: 512 * 1024,         // 512 KB backpressure resume threshold (Desktop)
  ACK_INTERVAL_BYTES: 2 * 1024 * 1024,// 2 MB receiver ACK checkpoint interval
  MAX_IN_MEMORY_QUEUE_BYTES: 16 * 1024 * 1024, // 16 MB max unwritten storage buffer
  HEARTBEAT_INTERVAL_MS: 5000,        // 5 seconds control channel heartbeat
  HEARTBEAT_TIMEOUT_MS: 15000,        // 15 seconds without heartbeat response is stalled
  STALL_DETECTION_WINDOW_MS: 15000,   // 15 seconds without byte progress triggers pipeline stall
  READ_BLOCK_SIZE: 4 * 1024 * 1024,   // 4 MB file disk-slice read buffer
  UI_UPDATE_THROTTLE_MS: 500          // Throttled UI aggregation window (500ms)
});
