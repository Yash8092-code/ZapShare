# ZapShare ⚡

> **Weightless, zero-friction, cross-platform peer-to-peer file transfer tool** (PC/Mac ↔ iOS/Android, and vice versa).
> Zero logins. Zero cloud storage. Zero limits. Direct WebRTC SCTP streaming with 64KB chunking.

---

## 🎨 Visual Direction

- **Cosmic Depth**: Deep dark canvas (`#0c0e14`) with atmospheric floating radial glow orbs (`backdrop-blur-2xl`) and interactive stardust particles.
- **Neon Accents**: Electric Violet (`#8B5CF6`), Cyan Aura (`#06B6D4`), and Hot Pulse Pink (`#EC4899`).
- **Typography**: Editorial typography using **Space Grotesk** and **Inter**.
- **Micro-interactions**: Levitating hover elevation, pulsing breathing beacons, real-time photon particle beam stream connecting sender and receiver.
- **Synthesized Web Audio**: Glass resonant blips, connection chimes, and euphoric completion chords generated dynamically using the Web Audio API.

---

## 🚀 Core Features & Screens

1. **Instant Send (Zero-Friction Drop Zone)**:
   - Drag & drop zone with magnetic grid mesh and floating file selector.
   - Dual-mode pairing: **Glowing QR Code** and **6-Digit PIN** with copy feedback.
   - Live breathing pairing beacon: *"Waiting for receiver to connect..."*.
   - Trust strip: *End-to-End Encrypted* · *Direct Device-to-Device* · *Zero Cloud Retention*.
   - File preview chip with file type icon, name, formatted size, and quick cancel.

2. **Instant Receive (Effortless Pairing & Download)**:
   - Dual-toggle: `[6-Digit Code]` | `[Scan QR]`.
   - **6-Digit Code**: Segmented OTP digit inputs with auto-advance, backspace handling, clipboard paste button, and auto-submission.
   - **Scan QR**: Real camera scanner viewfinder with animated cyan laser bar and viewfinder brackets.
   - **Incoming File Inspection Card**: Shows file name, size, sender device label, and high-contrast *"Accept & Download"* CTA alongside *"Decline"*.

3. **Weightless Transfer in Progress**:
   - **Dual-Node Device Bridge**: Sender Node ⟷ Receiver Node connected by a live photon particle stream canvas whose speed matches the actual transfer throughput.
   - Large percentage readout (e.g. `78%`) with animated radial glow.
   - Sleek gradient progress beam with glowing leading head.
   - The **Three Metrics Only**:
     - ⚡ **Speed**: e.g., `94.2 MB/s`
     - ⏱️ **Time Remaining**: e.g., `4 seconds`
     - 📦 **Transferred**: e.g., `1.43 / 1.84 GB`
   - Honest Connection Quality Badge: *"Direct connection (LAN / Local P2P)"* vs *"Relayed connection (via TURN)"*.
   - UX Safeguard notice: *"Keep this tab open — Backgrounding your browser (especially iOS Safari) may pause or cancel the transfer."*

4. **Transfer Complete (Euphoric Success State)**:
   - Celebratory headline: *"Boom. File Delivered! ⚡"* / *"Boom. File Received! ⚡"*.
   - Finished file summary card with *"Save to Downloads"* (auto-triggers zero-click download on recipient).
   - Ephemeral session notice: *"Ephemeral session purged. Peer keys, signaling memory, and data channels wiped. Zero traces left."*
   - *"Transfer Another File"* reset action.

---

## 🛠️ Real WebRTC Transfer Engine

- **Signaling**: Ephemeral WebSocket relay (`server.js`) matching 6-digit PIN rooms. Auto-cleans rooms after transfer or inactivity.
- **Transport**: Real WebRTC `RTCDataChannel` (SCTP) with `ordered: true`.
- **Chunking**: Browser File API slices chunked into 64KB `ArrayBuffer` slices.
- **Backpressure Handling**: Uses `channel.bufferedAmountLowThreshold` flow control (512KB ceiling) so sender never overloads the memory buffer.
- **Synchronized Metrics**: The receiver reassembles real chunks into memory Blob and sends progress acknowledgment packets back to sender. Both sender and receiver progress bars, speed, and ETA calculate from identical real byte counts.
- **Connection Quality**: Dynamic ICE candidate analysis (`host` vs `relay`/TURN) reports real connection state.

---

## 💻 Quick Start

### 1. Install & Start Server
```bash
npm install
npm start
```

### 2. Access ZapShare
- **On this machine**: [http://localhost:3000](http://localhost:3000)
- **On your phone / other devices on the same Wi-Fi**: Open `http://<your-local-ip>:3000` (printed in the terminal upon start).
