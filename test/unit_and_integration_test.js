// Comprehensive Unit & Integration Test Suite for ZapShare Transfer Engine
import assert from 'assert';
import http from 'http';
import { WebSocket } from 'ws';
import { TransferState, ControlMessageType, TransferDefaults } from '../public/js/transfer/constants.js';
import { TransferStateMachine } from '../public/js/transfer/state-machine.js';
import { BackpressureController } from '../public/js/transfer/backpressure-controller.js';
import { IntegrityManager } from '../public/js/transfer/integrity-manager.js';

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

console.log('🎉 ALL 6 COMPONENT & INTEGRATION TESTS PASSED PERFECTLY!\n');
process.exit(0);
