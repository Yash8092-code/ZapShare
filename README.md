# ZapShare ⚡

> **High-Throughput, Weightless, Zero-Friction Peer-to-Peer File Transfer** (PC/Mac ↔ iOS/Android, and vice versa).  
> Zero logins. Zero cloud storage. Zero limits. End-to-end encrypted direct WebRTC SCTP streaming with deterministic backpressure, progressive OPFS disk streaming, and cryptographic verification.

---

## 🚀 Transfer Engine Architecture (V2 Redesign)

ZapShare features an upgraded high-throughput transfer pipeline designed specifically to solve the common WebRTC buffer flooding and receiver memory exhaustion issues on multi-gigabyte transfers (e.g. 1.2 GB+ files):

```
Sender Device                                               Receiver Device
┌──────────────────────┐                               ┌──────────────────────┐
│     Source File      │                               │     Local Storage    │
└──────────┬───────────┘                               └──────────▲───────────┘
           │ (Slices)                                             │ (Stream writes)
┌──────────▼───────────┐                               ┌──────────┴───────────┐
│    SenderPipeline    │                               │   StorageAdapter     │
│ - Strict Backpressure│                               │ - OPFS sandbox sink  │
│ - Adaptive Chunking  │                               │ - FileSystemAccess   │
└──────────┬───────────┘                               └──────────▲───────────┘
           │                                                      │
┌──────────▼───────────┐    Raw File Chunks (32-128 KB)┌──────────┴───────────┐
│     zapshare_data    ├──────────────────────────────►│    zapshare_data     │
│   (RTCDataChannel)   │                               │   (RTCDataChannel)   │
└──────────────────────┘                               └──────────────────────┘
┌──────────────────────┐     ACK Checkpoints / Heartbeat┌──────────────────────┐
│   zapshare_control   │◄─────────────────────────────►│   zapshare_control   │
│   (RTCDataChannel)   │  (HEADER, ACK, PAUSE, RESUME) │   (RTCDataChannel)   │
└──────────────────────┘                               └──────────────────────┘
```

### 1. Deterministic Backpressure Flow Control
- **Dual Water Marks**: High-water mark at 2 MB; low-water mark at 512 KB.
- **BufferedAmountLow**: Sender waits strictly for the SCTP buffer to drain using `dataChannel.onbufferedamountlow`. Zero fake timeouts (no 250ms fallback flooding the network socket).
- **Adaptive Chunk Tuning**: Chunks automatically scale between 32 KB and 128 KB based on network buffer drain rates and negotiated `sctp.maxMessageSize`.

### 2. Dual-Channel Separation
- **Data Channel (`zapshare_data`)**: Dedicated raw binary file chunk transmission with zero per-chunk JSON framing overhead.
- **Control Channel (`zapshare_control`)**: Lightweight JSON control protocol handling:
  - `HEADER`: File metadata, expected size, fileId, chunk size.
  - `ACK`: Checkpoint acknowledgments sent every 2 MB containing confirmed bytes.
  - `PAUSE` / `RESUME`: Storage backpressure signaling when local disk writes need time to catch up.
  - `HEARTBEAT` / `HEARTBEAT_ACK`: Active ping distinguishing connection health from application stalls.
  - `EOF` & `VERIFY_OK`: End-of-file signal and cryptographic confirmation.

### 3. Progressive Streaming Storage (Zero RAM Accumulation)
- Rather than accumulating thousands of `ArrayBuffer` objects in JavaScript memory (which crashed mobile browsers on 1.2 GB+ transfers), ZapShare writes chunks progressively via:
  1. **OPFS (Origin Private File System)**: Universally supported across Chrome, Edge, Safari (macOS & iOS 15.2+), Firefox, and Android Chrome. Direct sandboxed file write with bounded memory (< 16 MB heap during a multi-gigabyte transfer).
  2. **File System Access API**: Direct user-chosen folder write where supported.
  3. **Bounded Memory Fallback**: For restricted environments.
- Chunks are cleaned up immediately from memory after writing to disk.

### 4. Five-Stage Byte Counters & Receiver-Confirmed Progress (Phase 2)
ZapShare decouples transmission stages to never confuse buffered bytes with confirmed bytes:
1. `bytesRead`: Data sliced from the source file.
2. `bytesQueued`: Bytes handed to `RTCDataChannel.send()`.
3. `bytesTransported`: Bytes that exited WebRTC outbound buffer (`bytesQueued - bufferedAmount`).
4. `bytesReceived`: Bytes received over network by receiver.
5. `bytesConfirmed`: Bytes committed to disk and confirmed via ACK checkpoint.
- **User Progress**: Calculated strictly from `bytesConfirmed / totalBytes`.

### 5. Graceful Stop & Cancellation Protocol (Phase 3, 4, 25, 26)
Solves the real-world issue where the sender stops but the receiver keeps receiving:
1. **Immediate Slicing Halt**: Sender stops reading/slicing new chunks instantly.
2. **`STOP_REQUEST`**: Sender transmits stop request with current queued counters.
3. **Write Flush & Checkpoint**: Receiver commits in-flight buffer to storage, determines last verified checkpoint, and replies with `STOP_ACK { confirmedBytes }`.
4. **`CANCEL` & `CANCEL_ACK`**: Sender sends `CANCEL`, receiver acknowledges with `CANCEL_ACK`.
5. **Post-Cancel Gatekeeper**: Receiver drops and logs any unexpected trailing chunks arriving after cancellation request.
6. **State Settled**: UI only shows "Transfer cancelled" after reaching the terminal state.

### 6. Mobile Efficiency, Device Profiles & Transfer Modes (Phase 7, 8, 15, 21, 22)
Engineered for sustainable mobile efficiency (e.g. Realme 6, Android, iOS):
- **Device Profiles**: Automatic detection of Mobile vs. Desktop via multi-signal UA + touch points + screen dimensions.
- **Transfer Modes**:
  1. `⚡ Balanced` (Default): Adaptive chunking (48–96 KB mobile, 64–128 KB desktop) and balanced backpressure.
  2. `🔋 Device Friendly`: Tailored for battery conservation and thermal control. Conservative chunk sizing (32–64 KB), smaller backpressure buffers (768 KB high / 192 KB low), throttled 1000ms UI updates, and pauses heavy canvas particle beams.
  3. `🚀 Maximum Speed`: Aggressive throughput with up to 128 KB chunks and 2.5 MB high-water mark for high-performance desktop rigs.
- **Indirect Resource Strain Heuristics**: Detects prolonged chunk processing (>45ms) or slow disk writes (>80ms) without fake temperature claims, automatically stepping down chunk sizes.
- **Throttled UI Updates**: Aggregates throughput and progress every 500–1000ms to eliminate per-chunk React/DOM re-render CPU spikes.

### 7. Weak-Network Adaptation & Dynamic Checkpoints (Phase 9, 10, 20)
- **Dynamic Network Classification**: Categorizes connection as `VERY_SLOW`, `SLOW`, `MODERATE`, `FAST`, or `VERY_FAST` based on confirmed throughput and RTT.
- **Adaptive Checkpoint Intervals**:
  - Weak/Slow links: 512 KB – 1 MB (small recovery window, zero buffer accumulation).
  - Fast links: 4 MB – 6 MB (minimal control overhead).
- **Bottleneck Detection**: Diagnoses whether limiting factor is `NETWORK`, `RECEIVER_STORAGE`, `BUFFER_PRESSURE`, or `CPU`.

### 8. Resumable Transfers
- Checkpoints are saved periodically during transmission. If the network drops, the receiver requests a resume from the last confirmed byte offset (`resumeFrom`). The sender seeks the file slice and continues without restarting from 0.

### 9. Transfer Verification & Integrity
- Progressive block-level cryptographic hashing (SHA-256) calculates checksums during transfer.
- Validates file name, exact byte count, and block integrity before declaring success.

### 10. Dynamic Secure TURN / STUN Infrastructure
- Dynamic ICE servers loaded via `/api/ice-servers`, preferring Direct LAN and Direct P2P over TURN relay. Hardcoded public credentials have been eliminated.

---

## 🎨 Visual Aesthetics & UI

- **Cosmic Depth**: Deep dark canvas (`#0c0e14`) with atmospheric floating radial glow orbs (`backdrop-blur-2xl`) and interactive stardust particles.
- **Neon Accents**: Electric Violet (`#8B5CF6`), Cyan Aura (`#06B6D4`), and Hot Pulse Pink (`#EC4899`).
- **Typography**: Editorial typography using **Space Grotesk** and **Inter**.
- **Micro-interactions**: Levitating hover elevation, pulsing breathing beacons, real-time photon particle beam stream connecting sender and receiver.
- **Synthesized Web Audio**: Glass resonant blips, connection chimes, and euphoric completion chords generated dynamically using the Web Audio API.
- **Developer Diagnostics Panel**: Press `Ctrl+Shift+D` or click "Diagnostics" on the transfer view to inspect live RTT, ICE candidate pairs, true throughput, buffer levels, and chunk sizes.

---

## 🧪 Benchmark & Verification Matrix

Automated verification tests (`test/unit_and_integration_test.js` & `test/e2e_transfer_simulation.js`):
- **Component Unit Tests**: 11/11 tests pass (State machine, backpressure drain, adaptive chunking, integrity verification, API endpoints, WebSocket PIN pairing, 4-step cancellation protocol, post-cancel chunk dropping, device profiles, network classification, and UI performance throttling).
- **20 MB E2E Transfer Benchmark**: Completed in 0.23s (~87.7 MB/s) with 12 backpressure flow events and zero memory leaks.
- **100 MB E2E Transfer Benchmark**: Completed in 1.07s (~93.8 MB/s) with 66 backpressure flow events and verified SHA-256 integrity.
- **Checkpoint Resume**: Resumed from 12 MB to 30 MB successfully without restarting from 0 MB.
- **Cancellation Protocol Benchmark**: 50 MB transfer cancelled mid-flight; receiver cleanly committed last checkpoint, dropped all in-flight post-cancel packets, and settled in terminal state.
- **Weak Network Simulation**: Completed with zero buffer overflow under simulated high RTT and low bandwidth; dynamic checkpoint adjusted to weak profile.
- **Device Friendly Mode Benchmark**: Verified 32 KB initial chunks, conservative water marks, and heavy visual pausing for thermal and battery efficiency.

---

## 💻 Quick Start

### 1. Install & Start Server
```bash
npm install
npm start
```

### 2. Access ZapShare
- **Local Machine**: [http://localhost:3000](http://localhost:3000)
- **Local Network (Phones & PCs)**: Open `http://<your-local-ip>:3000` (printed in the terminal upon start).
- **Toggle Diagnostics**: `Ctrl+Shift+D`
