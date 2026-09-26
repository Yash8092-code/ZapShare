// Comprehensive Unit & Integration Test Suite for ZapShare Transfer Engine
import assert from 'assert';
import http from 'http';
import { WebSocket } from 'ws';
import {
  TransferState,
  ControlMessageType,
  TransferDefaults,
  TransferMode,
  NetworkClass,
  BottleneckType,
  DeviceClass
} from '../public/js/transfer/constants.js';
import { TransferStateMachine } from '../public/js/transfer/state-machine.js';
import { BackpressureController } from '../public/js/transfer/backpressure-controller.js';
import { IntegrityManager } from '../public/js/transfer/integrity-manager.js';
import { CancellationManager } from '../public/js/transfer/cancellation-manager.js';
import { DeviceProfileManager } from '../public/js/transfer/device-profile.js';
import { NetworkManager } from '../public/js/transfer/network-manager.js';
import { PerformanceManager } from '../public/js/transfer/performance-manager.js';

console.log('🧪 Starting ZapShare Transfer Engine Test Suite...\n');

// ========================================================
// TEST 1: State Machine Transitions
// ========================================================
console.log('▶ Test 1: TransferStateMachine lifecycle and constraints');
const sm = new TransferStateMachine();
assert.strictEqual(sm.getState(), TransferState.IDLE);

// Valid progression
assert.strictEqual(sm.transition(TransferState.PREPARING, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.PREPARING);

assert.strictEqual(sm.transition(TransferState.READY, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.READY);

assert.strictEqual(sm.transition(TransferState.TRANSFERRING, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.TRANSFERRING);
assert.strictEqual(sm.isActive(), true);

assert.strictEqual(sm.transition(TransferState.PAUSED_BACKPRESSURE, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.PAUSED_BACKPRESSURE);

assert.strictEqual(sm.transition(TransferState.TRANSFERRING, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.TRANSFERRING);

assert.strictEqual(sm.transition(TransferState.VERIFYING, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.VERIFYING);

assert.strictEqual(sm.transition(TransferState.COMPLETED, 'test'), true);
assert.strictEqual(sm.getState(), TransferState.COMPLETED);
assert.strictEqual(sm.isTerminal(), true);

// Invalid transition from COMPLETED to TRANSFERRING must be rejected
assert.strictEqual(sm.canTransitionTo(TransferState.TRANSFERRING), false);
assert.strictEqual(sm.transition(TransferState.TRANSFERRING, 'illegal'), false);
assert.strictEqual(sm.getState(), TransferState.COMPLETED);

console.log('  ✔ State Machine passed all transition constraints.\n');

// ========================================================
// TEST 2: Backpressure Controller & Deterministic Drain
// ========================================================
console.log('▶ Test 2: BackpressureController & Deterministic Drain');

// Mock RTCDataChannel
class MockDataChannel {
  constructor() {
    this.readyState = 'open';
    this.bufferedAmount = 0;
    this.bufferedAmountLowThreshold = 0;
    this.listeners = {};
  }

  addEventListener(event, fn) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(fn);
  }

  removeEventListener(event, fn) {
    if (this.listeners[event]) {
      this.listeners[event] = this.listeners[event].filter(l => l !== fn);
    }
  }

  dispatchEvent(event) {
    if (this.listeners[event]) {
      this.listeners[event].forEach(fn => fn());
    }
  }
}

const mockDc = new MockDataChannel();
const bp = new BackpressureController(mockDc, {
  highWaterMark: 2 * 1024 * 1024,
  lowWaterMark: 512 * 1024,
  sctpMaxMessageSize: 262144
});

assert.strictEqual(mockDc.bufferedAmountLowThreshold, 512 * 1024);
assert.strictEqual(bp.getChunkSize(), 64 * 1024);

// Not congested when 0
assert.strictEqual(bp.isCongested(), false);

// Congested when above high water mark
mockDc.bufferedAmount = 3 * 1024 * 1024;
assert.strictEqual(bp.isCongested(), true);

// waitUntilDrained must NOT resolve until bufferedAmount <= lowWaterMark
let drained = false;
const drainPromise = bp.waitUntilDrained().then(() => {
  drained = true;
});

// While buffer is still high, event should not resolve
mockDc.bufferedAmount = 1.5 * 1024 * 1024;
mockDc.dispatchEvent('bufferedamountlow');
assert.strictEqual(drained, false, 'Should not resolve while buffer > lowWaterMark');

// Now simulate drain to 400 KB (<= 512 KB)
mockDc.bufferedAmount = 400 * 1024;
mockDc.dispatchEvent('bufferedamountlow');

await drainPromise;
assert.strictEqual(drained, true, 'Successfully resolved when buffer <= lowWaterMark');
console.log('  ✔ Deterministic backpressure drain verified.\n');

// ========================================================
// TEST 3: Adaptive Chunk Size Step-Up & Step-Down
// ========================================================
console.log('▶ Test 3: Adaptive Chunk Sizing');
assert.strictEqual(bp.getChunkSize(), 64 * 1024);

// Simulate 40 consecutive fast drains -> steps up to 96 KB
for (let i = 0; i < 40; i++) {
  bp.recordFastDrain();
}
assert.strictEqual(bp.getChunkSize(), 96 * 1024, 'Chunk size stepped up to 96 KB');

// Simulate 40 more -> steps up to 128 KB
for (let i = 0; i < 40; i++) {
  bp.recordFastDrain();
}
assert.strictEqual(bp.getChunkSize(), 128 * 1024, 'Chunk size stepped up to 128 KB');

// Simulate prolonged wait -> steps down to 96 KB
bp.recordBackpressurePause(120);
assert.strictEqual(bp.getChunkSize(), 96 * 1024, 'Chunk size stepped down to 96 KB on congestion');

console.log('  ✔ Adaptive chunk sizing verified.\n');

// ========================================================
// TEST 4: Integrity Verification
// ========================================================
console.log('▶ Test 4: Integrity Verification Logic');

const verifiedGood = IntegrityManager.verifyTransfer({
  expectedName: 'test.zip',
  receivedName: 'test.zip',
  expectedSize: 1200000000,
  receivedSize: 1200000000,
  expectedHash: 'abc1234567890',
  actualHash: 'abc1234567890'
});
assert.strictEqual(verifiedGood.isValid, true);
assert.strictEqual(verifiedGood.sizeOk, true);

const verifiedBadSize = IntegrityManager.verifyTransfer({
  expectedName: 'test.zip',
  receivedName: 'test.zip',
  expectedSize: 1200000000,
  receivedSize: 1199990000,
  expectedHash: 'abc1234567890',
  actualHash: 'abc1234567890'
});
assert.strictEqual(verifiedBadSize.isValid, false);
assert.strictEqual(verifiedBadSize.sizeOk, false);

console.log('  ✔ Transfer verification logic verified.\n');

// ========================================================
// TEST 5: HTTP Endpoints (/api/info & /api/ice-servers)
// ========================================================
console.log('▶ Test 5: Server API Endpoints');

function fetchJson(path) {
  return new Promise((resolve, reject) => {
    http.get(`http://localhost:3000${path}`, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

const info = await fetchJson('/api/info');
assert.ok(info.port, 'Should return port');
assert.ok(info.fullUrl, 'Should return fullUrl');
console.log('  ✔ /api/info:', info);

const ice = await fetchJson('/api/ice-servers');
assert.ok(Array.isArray(ice.iceServers), 'Should return iceServers array');
assert.ok(ice.iceServers.length >= 1, 'Should contain STUN servers');
// Verify NO openrelayproject credentials are leaked
const hasOpenRelay = JSON.stringify(ice).includes('openrelayproject');
assert.strictEqual(hasOpenRelay, false, 'Must NOT contain hardcoded openrelayproject credentials');
console.log('  ✔ /api/ice-servers returned clean STUN/TURN configs (zero hardcoded openrelay credentials).\n');

// ========================================================
// TEST 6: WebSocket Signaling & Room Pairing
// ========================================================
console.log('▶ Test 6: WebSocket 6-Digit PIN Pairing Flow');

const wsSender = new WebSocket('ws://localhost:3000');
const wsReceiver = new WebSocket('ws://localhost:3000');

await Promise.all([
  new Promise(res => wsSender.on('open', res)),
  new Promise(res => wsReceiver.on('open', res))
]);

const testPin = '839201';
const testMeta = { name: 'big_file.bin', size: 1200000000, device: 'Test Sender PC' };

// Sender creates room
wsSender.send(JSON.stringify({
  type: 'create-room',
  pin: testPin,
  fileMeta: testMeta
}));

const senderRoomCreated = await new Promise(res => {
  wsSender.on('message', raw => {
    const msg = JSON.parse(raw.toString());
    if (msg.type === 'room-created') res(msg);
  });
});
assert.strictEqual(senderRoomCreated.pin, testPin);

// Receiver joins room
wsReceiver.send(JSON.stringify({
  type: 'join-room',
  pin: testPin,
  clientInfo: { device: 'Test Receiver Android' }
}));

const receiverRoomJoined = await new Promise(res => {
  wsReceiver.on('message', raw => {
    const msg = JSON.parse(raw.toString());
    if (msg.type === 'room-joined') res(msg);
  });
});

assert.strictEqual(receiverRoomJoined.pin, testPin);
assert.strictEqual(receiverRoomJoined.fileMeta.name, 'big_file.bin');
assert.strictEqual(receiverRoomJoined.fileMeta.size, 1200000000);

wsSender.close();
wsReceiver.close();

console.log('  ✔ WebSocket 6-digit PIN pairing & metadata exchange succeeded.\n');

// ========================================================
// TEST 7: Stop & Cancellation Protocol Handshake (Phases 3, 4, 25)
// ========================================================
console.log('▶ Test 7: Stop & Cancellation Protocol Handshake (STOP_REQUEST -> STOP_ACK -> CANCEL -> CANCEL_ACK)');

const senderSm = new TransferStateMachine(TransferState.TRANSFERRING);
const receiverSm = new TransferStateMachine(TransferState.TRANSFERRING);

let senderSentControl = [];
let receiverSentControl = [];

const senderCancelMgr = new CancellationManager({
  role: 'sender',
  stateMachine: senderSm,
  sendControlFn: (msg) => {
    senderSentControl.push(msg);
    // Route to receiver asynchronously
    setTimeout(() => receiverCancelMgr.handleControlMessage(msg), 5);
  }
});

const receiverCancelMgr = new CancellationManager({
  role: 'receiver',
  stateMachine: receiverSm,
  sendControlFn: (msg) => {
    receiverSentControl.push(msg);
    // Route to sender asynchronously
    setTimeout(() => senderCancelMgr.handleControlMessage(msg), 5);
  },
  onCleanupFn: async (reason) => {
    return { confirmedBytes: 524288, checkpoint: 524288 };
  }
});

// Sender initiates stop
const stopPromise = senderCancelMgr.requestStop({
  bytesRead: 1048576,
  bytesQueued: 1048576,
  bytesConfirmed: 524288
});

const stopResult = await stopPromise;
assert.strictEqual(stopResult.confirmedBytes, 524288, 'Should confirm exact last checkpoint');
assert.strictEqual(senderSm.getState(), TransferState.CANCELLED, 'Sender must settle in CANCELLED state');

// Wait small tick for CANCEL / CANCEL_ACK round trip to reach receiver
await new Promise(r => setTimeout(r, 20));
assert.strictEqual(receiverSm.getState(), TransferState.CANCELLED, 'Receiver must settle in CANCELLED state');

// Verify handshake messages were exchanged in exact order
assert.strictEqual(senderSentControl[0].type, ControlMessageType.STOP_REQUEST);
assert.strictEqual(receiverSentControl[0].type, ControlMessageType.STOP_ACK);
assert.strictEqual(senderSentControl[1].type, ControlMessageType.CANCEL);
assert.strictEqual(receiverSentControl[1].type, ControlMessageType.CANCEL_ACK);

console.log('  ✔ Full 4-step stop & cancellation protocol handshake verified.\n');

// ========================================================
// TEST 8: Receiver Post-Cancel Data Rejection (Phase 26)
// ========================================================
console.log('▶ Test 8: Receiver Post-Cancel Data Rejection & Dropped Bytes Telemetry');
assert.strictEqual(receiverCancelMgr.shouldAcceptChunk(), false, 'Receiver must reject chunks after cancellation');

// Simulate in-flight chunks arriving at receiver after cancel
receiverCancelMgr.recordDroppedPostCancelData(65536);
receiverCancelMgr.recordDroppedPostCancelData(65536);
assert.strictEqual(receiverCancelMgr.postCancelDataDroppedBytes, 131072, 'Must record 128 KB dropped post-cancel data');

console.log('  ✔ Post-cancel chunk rejection and telemetry verified.\n');

// ========================================================
// TEST 9: DeviceProfileManager & Transfer Modes (Phases 7, 8)
// ========================================================
console.log('▶ Test 9: DeviceProfileManager & Transfer Modes (Balanced, Device Friendly, Maximum Speed)');
const dpm = new DeviceProfileManager();

// Test BALANCED (Default)
dpm.setTransferMode(TransferMode.BALANCED);
let tuning = dpm.getTuning();
assert.strictEqual(tuning.transferMode, TransferMode.BALANCED);
assert.ok(tuning.initialChunk >= 32 * 1024);
assert.ok(tuning.maxChunk <= 128 * 1024);

// Test DEVICE_FRIENDLY (Mobile / thermal constrained)
dpm.setTransferMode(TransferMode.DEVICE_FRIENDLY);
tuning = dpm.getTuning();
assert.strictEqual(tuning.initialChunk, 32 * 1024, 'Device Friendly initial chunk 32 KB');
assert.strictEqual(tuning.maxChunk, 64 * 1024, 'Device Friendly max chunk 64 KB');
assert.strictEqual(tuning.highWater, 768 * 1024, 'Device Friendly conservative high-water mark');
assert.strictEqual(tuning.enableHeavyVisuals, false, 'Device Friendly disables heavy canvas animations');

// Test MAXIMUM_SPEED (Desktop / High Performance)
dpm.setTransferMode(TransferMode.MAXIMUM_SPEED);
tuning = dpm.getTuning();
assert.strictEqual(tuning.maxChunk, 128 * 1024, 'Maximum Speed max chunk 128 KB');
assert.strictEqual(tuning.highWater, 2.5 * 1024 * 1024, 'Maximum Speed aggressive high-water mark');
assert.strictEqual(tuning.enableHeavyVisuals, true);

console.log('  ✔ Device profiles and transfer mode parameters verified.\n');

// ========================================================
// TEST 10: NetworkManager Classification & Adaptive Checkpoints (Phases 9, 10, 20)
// ========================================================
console.log('▶ Test 10: NetworkManager Dynamic Classification & Adaptive Checkpoints');
const netMgr = new NetworkManager();

// Weak network test
netMgr.updateMetrics({ confirmedSpeedMB: 0.3, rttMs: 420 });
assert.strictEqual(netMgr.networkClass, NetworkClass.VERY_SLOW);
assert.strictEqual(netMgr.getAdaptiveCheckpointInterval(), 512 * 1024, '512 KB checkpoint for weak networks');

// Fast network test
netMgr.updateMetrics({ confirmedSpeedMB: 35.0, rttMs: 15 });
// Let samples smooth out
for (let i = 0; i < 5; i++) netMgr.updateMetrics({ confirmedSpeedMB: 35.0 });
assert.strictEqual(netMgr.networkClass, NetworkClass.VERY_FAST);
assert.strictEqual(netMgr.getAdaptiveCheckpointInterval(), 6 * 1024 * 1024, '6 MB checkpoint for very fast networks');

// Bottleneck detection: Receiver Storage Bottleneck
netMgr.updateMetrics({ storageLatencyMs: 150 });
assert.strictEqual(netMgr.bottleneck, BottleneckType.RECEIVER_STORAGE);
assert.strictEqual(netMgr.getStatusMessage(), 'Receiver writing to storage...');

console.log('  ✔ Dynamic network classification & bottleneck detection verified.\n');

// ========================================================
// TEST 11: PerformanceManager UI Throttling & Indirect Resource Heuristics (Phases 15, 21)
// ========================================================
console.log('▶ Test 11: PerformanceManager UI Throttling & Thermal Heuristics');
let stressReported = null;
const perfMgr = new PerformanceManager({
  uiThrottleMs: 500,
  callbacks: {
    onDeviceStress: (msg) => { stressReported = msg; }
  }
});

// Throttling verification
const t0 = 1000;
assert.strictEqual(perfMgr.shouldUpdateUi(t0), true, 'First UI update allowed');
assert.strictEqual(perfMgr.shouldUpdateUi(t0 + 200), false, 'Update at +200ms blocked by 500ms throttle');
assert.strictEqual(perfMgr.shouldUpdateUi(t0 + 490), false, 'Update at +490ms blocked by 500ms throttle');
assert.strictEqual(perfMgr.shouldUpdateUi(t0 + 510), true, 'Update at +510ms allowed');

// Indirect resource stress detection (prolonged processing latency without fake temp APIs)
for (let i = 0; i < 5; i++) {
  perfMgr.recordProcessingEvent(55, 95); // High processing & storage write delay
}
assert.strictEqual(perfMgr.isDeviceThrottlingDetected, true);
assert.ok(stressReported.includes('Optimizing transfer for device performance'));

console.log('  ✔ UI update throttling & indirect performance stress detection verified.\n');

console.log('🎉 ALL 11 COMPONENT & INTEGRATION TESTS PASSED PERFECTLY!\n');
process.exit(0);
