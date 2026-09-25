// ZapShare WebRTC Core: Direct P2P SCTP Engine with Diagnostics & Mobile Safe Streaming
import { sound } from './sound.js';

export class WebRTCManager {
  constructor(callbacks = {}) {
    this.callbacks = callbacks;
    this.ws = null;
    this.pc = null;
    this.dataChannel = null;
    this.role = null; // 'sender' | 'receiver'
    this.pin = null;
    this.fileMeta = null;
    this.file = null;

    // Real transfer metrics
    this.totalBytes = 0;
    this.transferredBytes = 0;
    this.receivedChunks = [];
    this.startTime = 0;
    this.lastSampleTime = 0;
    this.lastSampleBytes = 0;
    this.currentSpeedMB = 0;
    this.connectionType = 'Connecting peers (ICE checking)...';
    this.isTransferring = false;
    this.pendingCandidates = [];
    this.statsInterval = null;

    // Watchdog & Stall Detection
    this.watchdogInterval = null;
    this.lastWatchdogBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;
    this.isRelayed = false;

    // UI Throttling
    this.lastUiUpdateTime = 0;

    this.initWebSocket();
  }

  initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;

    try {
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        console.log('[Signaling] WebSocket connected to relay server.');
        if (this.callbacks.onNetworkStatus) {
          this.callbacks.onNetworkStatus({ online: true, mode: 'websocket' });
        }
      };

      this.ws.onmessage = (e) => {
        try {
          const msg = JSON.parse(e.data);
          this.handleSignalingMessage(msg);
        } catch (err) {
          console.error('[Signaling] Parse error:', err);
        }
      };

      this.ws.onclose = () => {
        console.warn('[Signaling] WebSocket disconnected.');
        if (this.callbacks.onNetworkStatus) {
          this.callbacks.onNetworkStatus({ online: false, mode: 'disconnected' });
        }
        setTimeout(() => this.initWebSocket(), 2500);
      };

      this.ws.onerror = (err) => {
        console.error('[Signaling] WebSocket error:', err);
        if (this.callbacks.onNetworkStatus) {
          this.callbacks.onNetworkStatus({ online: false, mode: 'error' });
        }
      };
    } catch (e) {
      console.warn('[Signaling] WebSocket connection failed:', e);
    }
  }

  sendSignal(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    } else {
      console.warn('[Signaling] Cannot send signal — WebSocket not open.');
    }
  }

  // --- SENDER API ---
  createRoomForFile(file) {
    this.role = 'sender';
    this.file = file;
    this.pin = Math.floor(100000 + Math.random() * 900000).toString();
    this.pendingCandidates = [];

    this.fileMeta = {
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
      device: this.getDeviceLabel()
    };

    this.sendSignal({
      type: 'create-room',
      pin: this.pin,
      fileMeta: this.fileMeta
    });

    console.log(`[Sender] Room requested with PIN: ${this.pin} for file: ${file.name} (${file.size} B)`);
    return { pin: this.pin, fileMeta: this.fileMeta };
  }

  // --- RECEIVER API ---
  joinRoomWithPin(pin) {
    this.role = 'receiver';
    this.pin = pin.replace(/\s+/g, '');
    this.pendingCandidates = [];

    this.sendSignal({
      type: 'join-room',
      pin: this.pin,
      clientInfo: { device: this.getDeviceLabel() }
    });

    console.log(`[Receiver] Joining room with PIN: ${this.pin}`);
  }

  // --- SIGNALING MESSAGE HANDLER ---
  async handleSignalingMessage(msg) {
    switch (msg.type) {
      case 'room-created': {
        console.log('[Signaling] Room registered on server:', msg.pin);
        break;
      }

      case 'peer-joined': {
        console.log('[Signaling] Peer receiver joined:', msg.senderInfo);
        sound.playConnect();
        if (this.callbacks.onPeerConnected) {
          this.callbacks.onPeerConnected({ role: 'receiver', info: msg.senderInfo });
        }
        break;
      }

      case 'room-joined': {
        console.log('[Signaling] Room joined successfully. File meta:', msg.fileMeta);
        sound.playConnect();
        this.fileMeta = msg.fileMeta;
        if (this.callbacks.onIncomingFile) {
          this.callbacks.onIncomingFile(msg.fileMeta);
        }
        break;
      }

      case 'meta-updated': {
        this.fileMeta = msg.fileMeta;
        if (this.callbacks.onIncomingFile) {
          this.callbacks.onIncomingFile(msg.fileMeta);
        }
        break;
      }

      case 'transfer-action': {
        console.log('[Signaling] Transfer action received:', msg.action);
        if (msg.action === 'accept') {
          if (this.role === 'sender') {
            await this.initiateWebRTCAsSender();
          }
        } else if (msg.action === 'decline') {
          sound.playDecline();
          if (this.callbacks.onDeclined) this.callbacks.onDeclined();
        } else if (msg.action === 'cancel') {
          this.cleanupTransfer();
          if (this.callbacks.onCancelled) this.callbacks.onCancelled();
        }
        break;
      }

      case 'signal': {
        await this.handlePeerSignal(msg.data);
        break;
      }

      case 'error': {
        console.error('[Signaling] Error message from server:', msg.message);
        if (this.callbacks.onError) this.callbacks.onError(msg.message);
        break;
      }

      case 'peer-disconnected': {
        console.warn('[Signaling] Peer disconnected.');
        if (this.isTransferring) {
          if (this.callbacks.onStall) {
            this.callbacks.onStall('Peer disconnected prematurely.');
          }
          if (this.callbacks.onError) {
            this.callbacks.onError('Peer disconnected during active transfer.');
          }
        }
        break;
      }
    }
  }

  // --- WEBRTC PEER CONNECTION CREATION ---
  createPeerConnection() {
    // Comprehensive STUN and reliable free TURN relay fallback
    const config = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun.relay.metered.ca:80' },
        {
          urls: [
            'turn:global.relay.metered.ca:80',
            'turn:global.relay.metered.ca:80?transport=tcp',
            'turn:global.relay.metered.ca:443',
            'turn:global.relay.metered.ca:443?transport=tcp'
          ],
          username: 'openrelayproject',
          credential: 'openrelayproject'
        }
      ],
      iceCandidatePoolSize: 10
    };

    const pc = new RTCPeerConnection(config);

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        console.log(`[WebRTC ICE Local Candidate] Type: ${event.candidate.type} | Protocol: ${event.candidate.protocol} | Address: ${event.candidate.address || 'mDNS'} | Port: ${event.candidate.port}`);
        this.sendSignal({
          type: 'signal',
          pin: this.pin,
          data: { candidate: event.candidate }
        });
      } else {
        console.log('[WebRTC ICE] Local candidate gathering completed.');
      }
    };

    pc.oniceconnectionstatechange = () => {
      console.log(`[WebRTC State] iceConnectionState: ${pc.iceConnectionState} | connectionState: ${pc.connectionState}`);
      this.updateConnectionQualityBadge();

      if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
        console.log('[WebRTC State] ✅ Peer connection successfully established!');
        this.detectCandidatePair();
        if (this.isStallWarningActive) {
          this.isStallWarningActive = false;
          if (this.callbacks.onStallRecovered) {
            this.callbacks.onStallRecovered();
          }
        }
      } else if (pc.iceConnectionState === 'failed') {
        console.error('[WebRTC State] ❌ ICE connection failed! Triggering ICE restart...');
        if (this.callbacks.onStall) {
          this.callbacks.onStall('ICE connection failed. Attempting restart...');
        }
        pc.restartIce();
      } else if (pc.iceConnectionState === 'disconnected') {
        console.warn('[WebRTC State] ⚠️ ICE connection disconnected.');
        // Handled by watchdog with rolling window to prevent brief transient flickers
      }
    };

    pc.onconnectionstatechange = () => {
      console.log(`[WebRTC State] connectionState: ${pc.connectionState}`);
      this.updateConnectionQualityBadge();
    };

    return pc;
  }

  // Update badge immediately based on actual ICE state
  updateConnectionQualityBadge() {
    if (!this.pc) return;
    const state = this.pc.iceConnectionState;

    if (state === 'checking') {
      this.connectionType = 'Connecting peers (ICE checking)...';
    } else if (state === 'failed' || state === 'disconnected') {
      this.connectionType = 'Connection lost (ICE failed)';
    }

    if (this.callbacks.onConnectionQuality) {
      this.callbacks.onConnectionQuality(this.connectionType);
    }
  }

  // Inspect the negotiated candidate pair (host vs srflx vs relay)
  async detectCandidatePair() {
    if (!this.pc) return;
    try {
      const stats = await this.pc.getStats();
      let activePair = null;

      stats.forEach((report) => {
        if (report.type === 'transport' && report.selectedCandidatePairId) {
          activePair = stats.get(report.selectedCandidatePairId);
        }
        if (!activePair && report.type === 'candidate-pair' && (report.selected || report.nominated || report.state === 'succeeded')) {
          activePair = report;
        }
      });

      if (activePair) {
        const localCandidate = stats.get(activePair.localCandidateId);
        const remoteCandidate = stats.get(activePair.remoteCandidateId);

        console.log(`[WebRTC Candidate Pair] Local: ${localCandidate?.candidateType} (${localCandidate?.protocol} ${localCandidate?.address}:${localCandidate?.port}) <--> Remote: ${remoteCandidate?.candidateType} (${remoteCandidate?.protocol} ${remoteCandidate?.address}:${remoteCandidate?.port})`);

        if (localCandidate?.candidateType === 'relay' || remoteCandidate?.candidateType === 'relay') {
          this.connectionType = 'Relayed connection (via TURN)';
          this.isRelayed = true;
        } else if (localCandidate?.candidateType === 'host' && remoteCandidate?.candidateType === 'host') {
          this.connectionType = 'Direct connection (LAN / Local P2P)';
          this.isRelayed = false;
        } else {
          this.connectionType = 'Direct connection (P2P STUN)';
          this.isRelayed = false;
        }
      } else {
        this.connectionType = 'Direct connection (P2P SCTP)';
        this.isRelayed = false;
      }

      console.log(`[WebRTC Quality Badge] Showing: ${this.connectionType}`);
      if (this.callbacks.onConnectionQuality) {
        this.callbacks.onConnectionQuality(this.connectionType);
      }
    } catch (e) {
      console.warn('[WebRTC Stats] Could not inspect candidate pair:', e);
    }
  }

  // Sender starts connection on accept
  async initiateWebRTCAsSender() {
    this.isTransferring = true;
    this.totalBytes = this.file.size;
    this.transferredBytes = 0;
    this.startTime = performance.now();
    this.lastSampleTime = performance.now();
    this.lastSampleBytes = 0;
    this.lastUiUpdateTime = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.startWatchdog();

    if (this.callbacks.onTransferStart) {
      this.callbacks.onTransferStart({
        role: 'sender',
        meta: this.fileMeta,
        connectionType: 'Connecting peers (ICE checking)...'
      });
    }

    this.pc = this.createPeerConnection();

    // Create high-throughput RTCDataChannel
    this.dataChannel = this.pc.createDataChannel('zapshare_transfer', {
      ordered: true
    });
    this.dataChannel.binaryType = 'arraybuffer';
    this.setupDataChannel(this.dataChannel);

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);

    console.log('[WebRTC] Sender created offer. Local SDP set. Relaying offer...');
    this.sendSignal({
      type: 'signal',
      pin: this.pin,
      data: { sdp: this.pc.localDescription }
    });
  }

  // Handle SDP offer/answer and ICE candidate signals
  async handlePeerSignal(data) {
    if (data.sdp) {
      if (data.sdp.type === 'offer') {
        console.log('[WebRTC] Receiver received offer. Creating PeerConnection...');
        if (!this.pc) {
          this.pc = this.createPeerConnection();
        }

        this.pc.ondatachannel = (e) => {
          console.log('[WebRTC] Receiver ondatachannel event received! Attaching channel...');
          this.dataChannel = e.channel;
          this.dataChannel.binaryType = 'arraybuffer';
          this.setupDataChannel(this.dataChannel);
        };

        await this.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        console.log('[WebRTC] Receiver setRemoteDescription(offer) succeeded.');

        // Drain any pending candidates
        await this.drainPendingCandidates();

        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        console.log('[WebRTC] Receiver created answer. Relaying answer...');
        this.sendSignal({
          type: 'signal',
          pin: this.pin,
          data: { sdp: this.pc.localDescription }
        });
      } else if (data.sdp.type === 'answer') {
        console.log('[WebRTC] Sender received answer. Setting remote description...');
        if (this.pc) {
          await this.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
          console.log('[WebRTC] Sender setRemoteDescription(answer) succeeded.');
          await this.drainPendingCandidates();
        }
      }
    } else if (data.candidate) {
      const candidate = new RTCIceCandidate(data.candidate);
      if (this.pc && this.pc.remoteDescription && this.pc.remoteDescription.type) {
        try {
          await this.pc.addIceCandidate(candidate);
          console.log(`[WebRTC ICE Remote Candidate] Added candidate: ${candidate.type} | Protocol: ${candidate.protocol}`);
        } catch (e) {
          console.warn('[WebRTC] Error adding ICE candidate:', e);
        }
      } else {
        // Queue candidate until setRemoteDescription completes
        this.pendingCandidates.push(candidate);
        console.log(`[WebRTC ICE Queue] Queued remote candidate (${this.pendingCandidates.length} total)`);
      }
    }
  }

  async drainPendingCandidates() {
    if (!this.pc) return;
    console.log(`[WebRTC ICE Queue] Draining ${this.pendingCandidates.length} queued ICE candidates...`);
    while (this.pendingCandidates.length > 0) {
      const cand = this.pendingCandidates.shift();
      try {
        await this.pc.addIceCandidate(cand);
        console.log(`[WebRTC ICE Queue] Drained candidate: ${cand.type}`);
      } catch (err) {
        console.warn('[WebRTC ICE Queue] Error adding drained candidate:', err);
      }
    }
  }

  setupDataChannel(dc) {
    dc.onopen = () => {
      const sctpMax = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;
      console.log(`[WebRTC DataChannel] OPEN! Role: ${this.role} | sctp.maxMessageSize: ${sctpMax} B | binaryType: ${dc.binaryType}`);

      this.detectCandidatePair();
      this.statsInterval = setInterval(() => this.detectCandidatePair(), 1500);

      if (this.role === 'sender') {
        this.streamFileToPeer();
      }
    };

    dc.onclose = () => {
      console.log('[WebRTC DataChannel] Closed.');
      if (this.statsInterval) clearInterval(this.statsInterval);
    };

    dc.onerror = (err) => {
      console.error('[WebRTC DataChannel] Error:', err);
      if (this.callbacks.onError) this.callbacks.onError('RTCDataChannel transport error: ' + (err.message || 'unknown'));
    };

    dc.onmessage = async (event) => {
      await this.handleIncomingDataChannelMessage(event.data);
    };
  }

  // Receiver accepts incoming file
  acceptIncomingFile() {
    this.sendSignal({
      type: 'transfer-action',
      pin: this.pin,
      action: 'accept'
    });

    this.isTransferring = true;
    this.totalBytes = this.fileMeta ? this.fileMeta.size : 0;
    this.transferredBytes = 0;
    this.receivedChunks = [];
    this.startTime = performance.now();
    this.lastSampleTime = performance.now();
    this.lastSampleBytes = 0;
    this.lastUiUpdateTime = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.startWatchdog();

    if (this.callbacks.onTransferStart) {
      this.callbacks.onTransferStart({
        role: 'receiver',
        meta: this.fileMeta,
        connectionType: 'Connecting peers (ICE checking)...'
      });
    }
  }

  declineIncomingFile() {
    this.sendSignal({
      type: 'transfer-action',
      pin: this.pin,
      action: 'decline'
    });
    this.cleanupTransfer();
  }

  cancelTransfer() {
    this.sendSignal({
      type: 'transfer-action',
      pin: this.pin,
      action: 'cancel'
    });
    this.cleanupTransfer();
  }

  // --- SENDER: STREAMING WITH SAFETY & BUFFER MONITORING ---
  async streamFileToPeer() {
    const dc = this.dataChannel;
    if (!this.file || !dc || dc.readyState !== 'open') {
      console.warn('[WebRTC Send] Cannot stream: channel not open.');
      return;
    }

    const totalSize = this.file.size;
    const sctpMax = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;

    // Adaptive Chunk Sizing based on device, route, and negotiated SCTP limits:
    // - Chrome/Chromium desktop & mobile negotiate sctpMax = 262,144 (256 KB).
    //   On non-relay connections, 128KB (131,072 B) chunks cut function call & framing overhead by 4x.
    // - sctpMax >= 64KB (Safari / WebKit): 64KB (65,536 B).
    // - Constrained / Relayed / Unknown: safe 32KB fallback (32,768 B).
    let chunkSize = 32 * 1024;
    if (sctpMax >= 262144 && !this.isRelayed) {
      chunkSize = 128 * 1024; // 128 KB
    } else if (sctpMax >= 65536 && !this.isRelayed) {
      chunkSize = 64 * 1024; // 64 KB
    } else {
      chunkSize = 32 * 1024; // 32 KB fallback
    }

    const CHUNK_SIZE = chunkSize;
    const BLOCK_SIZE = 4 * 1024 * 1024; // 4 MB memory read block
    const BUFFER_CEILING = 4 * 1024 * 1024; // 4 MB pipeline in flight (keeps pipe full)
    const BUFFER_RESUME = 1024 * 1024; // 1 MB resume threshold

    console.log(`[WebRTC Send] 🚀 Starting transfer of "${this.file.name}" (${(totalSize / (1024 * 1024)).toFixed(2)} MB)`);
    console.log(`[WebRTC Send Config] Negotiated sctp.maxMessageSize: ${sctpMax} B | Adaptive Chunk Size: ${CHUNK_SIZE / 1024} KB | Buffer Ceiling: ${BUFFER_CEILING / (1024 * 1024)} MB | Low Threshold: ${BUFFER_RESUME / (1024 * 1024)} MB | Relayed: ${this.isRelayed}`);

    // 1. Send metadata header
    dc.send(JSON.stringify({
      type: 'HEADER',
      name: this.file.name,
      size: totalSize,
      mime: this.file.type || 'application/octet-stream'
    }));

    dc.bufferedAmountLowThreshold = BUFFER_RESUME;

    let fileOffset = 0;
    let bytesQueued = 0;
    let chunkCount = 0;

    const streamStart = performance.now();

    try {
      while (fileOffset < totalSize) {
        if (!this.isTransferring) break;

        // Read 4MB memory block from file
        const blockSlice = this.file.slice(fileOffset, Math.min(totalSize, fileOffset + BLOCK_SIZE));
        const blockBuffer = await blockSlice.arrayBuffer();
        fileOffset += blockBuffer.byteLength;

        let blockOffset = 0;
        const blockLen = blockBuffer.byteLength;

        while (blockOffset < blockLen) {
          if (!this.isTransferring) break;

          const sliceSize = Math.min(CHUNK_SIZE, blockLen - blockOffset);
          const chunk = blockBuffer.slice(blockOffset, blockOffset + sliceSize);

          // Flow control: if bufferedAmount exceeds 4MB, wait for drain to 1MB
          if (dc.bufferedAmount > BUFFER_CEILING) {
            await new Promise((resolve) => {
              let done = false;
              const timer = setTimeout(() => {
                if (!done) {
                  done = true;
                  dc.removeEventListener('bufferedamountlow', onLow);
                  resolve();
                }
              }, 250); // 250ms fallback

              const onLow = () => {
                if (!done) {
                  done = true;
                  clearTimeout(timer);
                  dc.removeEventListener('bufferedamountlow', onLow);
                  resolve();
                }
              };

              dc.addEventListener('bufferedamountlow', onLow, { once: true });
              if (dc.bufferedAmount <= BUFFER_RESUME) {
                onLow();
              }
            });
          }

          // Send raw binary ArrayBuffer
          dc.send(chunk);
          blockOffset += sliceSize;
          bytesQueued += sliceSize;
          chunkCount++;

          // Periodic progress log every 20 chunks
          if (chunkCount % 20 === 0 || bytesQueued >= totalSize) {
            const currentSpeed = this.currentSpeedMB.toFixed(1);
            console.log(`[WebRTC Send #${chunkCount}] Queued: ${(bytesQueued / (1024 * 1024)).toFixed(2)} / ${(totalSize / (1024 * 1024)).toFixed(2)} MB (${((bytesQueued / totalSize) * 100).toFixed(1)}%) | Speed: ${currentSpeed} MB/s | bufferedAmount: ${(dc.bufferedAmount / 1024).toFixed(0)} KB`);
          }

          const actualTransferred = Math.max(0, bytesQueued - dc.bufferedAmount);
          this.throttledUpdateMetrics(actualTransferred, totalSize);
        }
      }

      // Wait for all buffered data to flush over the wire
      console.log(`[WebRTC Send] All chunks queued (${chunkCount} total). Flushing remaining ${(dc.bufferedAmount / 1024).toFixed(0)} KB in buffer...`);
      while (dc.bufferedAmount > 0) {
        const actualTransferred = Math.max(0, bytesQueued - dc.bufferedAmount);
        this.throttledUpdateMetrics(actualTransferred, totalSize);
        await new Promise((r) => setTimeout(r, 10));
      }

      const streamDuration = (performance.now() - streamStart) / 1000;
      const avgSpeed = (totalSize / (1024 * 1024)) / (streamDuration || 0.001);
      console.log(`[WebRTC Send Metric] ⚡ Transfer finished! Total: ${(totalSize / (1024 * 1024)).toFixed(2)} MB in ${streamDuration.toFixed(2)}s | Achieved Speed: ${avgSpeed.toFixed(2)} MB/s (${chunkCount} chunks @ ${CHUNK_SIZE / 1024} KB/chunk)`);

      this.transferredBytes = totalSize;
      this.throttledUpdateMetrics(totalSize, totalSize, true);

      // Send end-of-file sentinel
      if (this.isTransferring) {
        dc.send(JSON.stringify({ type: 'EOF' }));
      }
    } catch (err) {
      console.error('[WebRTC Send] Streaming error:', err);
      if (this.callbacks.onError) this.callbacks.onError('Transfer failed: ' + err.message);
    }
  }

  // --- RECEIVER: RECEIVE & REASSEMBLE CHUNKS ---
  async handleIncomingDataChannelMessage(data) {
    const dc = this.dataChannel;

    // Handle string control messages (HEADER, EOF, ACK_COMPLETE)
    if (typeof data === 'string') {
      try {
        const msg = JSON.parse(data);

        if (msg.type === 'HEADER') {
          this.fileMeta = { name: msg.name, size: msg.size, type: msg.mime };
          this.totalBytes = msg.size;
          this.transferredBytes = 0;
          this.receivedChunks = [];
          this.startTime = performance.now();
          this.lastSampleTime = performance.now();
          this.lastSampleBytes = 0;
          this.lastUiUpdateTime = 0;
          console.log(`[WebRTC Receive] Starting incoming file: ${msg.name} (${(msg.size / (1024 * 1024)).toFixed(2)} MB)`);
        } else if (msg.type === 'EOF') {
          const duration = (performance.now() - this.startTime) / 1000;
          const avgSpeed = (this.totalBytes / (1024 * 1024)) / (duration || 0.001);
          console.log(`[WebRTC Receive Metric] ⚡ Transfer complete! ${(this.totalBytes / (1024 * 1024)).toFixed(2)} MB received in ${duration.toFixed(2)}s | Achieved Speed: ${avgSpeed.toFixed(2)} MB/s (${this.receivedChunks.length} chunks). Assembling file...`);
          const blob = new Blob(this.receivedChunks, {
            type: (this.fileMeta && this.fileMeta.type) || 'application/octet-stream'
          });

          if (dc && dc.readyState === 'open') {
            dc.send(JSON.stringify({ type: 'ACK_COMPLETE' }));
          }

          this.throttledUpdateMetrics(this.totalBytes, this.totalBytes, true);
          this.handleTransferSuccess(blob);
        } else if (msg.type === 'ACK_COMPLETE') {
          if (this.role === 'sender') {
            console.log('[WebRTC Send] Received ACK_COMPLETE from receiver.');
            this.throttledUpdateMetrics(this.totalBytes, this.totalBytes, true);
            this.handleTransferSuccess(null);
          }
        }
      } catch (e) {
        console.warn('[WebRTC] Control message parse error:', e);
      }
      return;
    }

    // Binary chunk: support both ArrayBuffer and Blob (mobile browser compatibility)
    let buffer = data;
    if (data instanceof Blob) {
      buffer = await data.arrayBuffer();
    }

    if (buffer instanceof ArrayBuffer) {
      this.receivedChunks.push(buffer);
      this.transferredBytes += buffer.byteLength;

      if (this.receivedChunks.length % 20 === 0) {
        console.log(`[WebRTC Receive #${this.receivedChunks.length}] Received: ${(this.transferredBytes / (1024 * 1024)).toFixed(2)} / ${(this.totalBytes / (1024 * 1024)).toFixed(2)} MB | Speed: ${this.currentSpeedMB.toFixed(1)} MB/s`);
      }

      this.throttledUpdateMetrics(this.transferredBytes, this.totalBytes);
    }
  }

  // --- STALL DETECTOR WATCHDOG ---
  startWatchdog() {
    this.stopWatchdog();
    this.lastWatchdogBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.watchdogInterval = setInterval(() => {
      if (!this.isTransferring || this.totalBytes === 0) return;

      const currentBytes = this.transferredBytes;
      const bytesDiff = currentBytes - this.lastWatchdogBytes;
      const iceState = this.pc ? this.pc.iceConnectionState : 'unknown';
      const dcReady = this.dataChannel ? this.dataChannel.readyState : 'none';
      const bufferedAmount = this.dataChannel ? this.dataChannel.bufferedAmount : 0;

      // Track consecutive seconds of 0 byte progress
      if (bytesDiff === 0 && currentBytes < this.totalBytes) {
        this.zeroProgressCount += 1;
      } else {
        if (this.zeroProgressCount > 0) {
          this.zeroProgressCount = 0;
          if (this.isStallWarningActive) {
            this.isStallWarningActive = false;
            console.log(`[WebRTC Watchdog] ✅ Transfer resumed! Progress detected (${bytesDiff} B). Clearing stall warning.`);
            if (this.callbacks.onStallRecovered) {
              this.callbacks.onStallRecovered();
            }
          }
        }
      }

      // Track ICE checking duration
      if (iceState === 'checking') {
        this.checkingDuration += 1;
      } else {
        this.checkingDuration = 0;
      }

      // Determine if a real stall or connection drop has occurred
      // CRITICAL RULE: NEVER show a stall/reconnect warning if iceConnectionState is 'connected' or 'completed'
      // and dataChannel is 'open', even if bytesDiff is 0 (normal backpressure or disk pauses).
      let shouldAlert = false;
      let warningMessage = '';

      if (iceState === 'failed') {
        shouldAlert = true;
        warningMessage = 'Connection failed. Attempting ICE restart...';
      } else if (iceState === 'disconnected') {
        // Disconnected for at least 4 seconds of truly zero byte progress
        if (this.zeroProgressCount >= 4) {
          shouldAlert = true;
          warningMessage = 'Network connection disrupted. Waiting for peer to reconnect...';
        }
      } else if (iceState === 'checking' && this.checkingDuration >= 8) {
        shouldAlert = true;
        warningMessage = `Direct connection taking longer than expected (${this.checkingDuration}s). Negotiating route...`;
      } else if (iceState === 'connected' || iceState === 'completed') {
        // ICE is completely healthy!
        // Never trigger "Transfer stalled... Retrying connection".
        // Log diagnostic notice if zero progress persists during backpressure drain
        if (this.zeroProgressCount >= 5 && this.zeroProgressCount % 5 === 0) {
          console.warn(`[WebRTC Watchdog Diagnostics] Notice: 0 bytes progress for ${this.zeroProgressCount}s, but ICE is healthy (${iceState}) | bufferedAmount: ${bufferedAmount} B | transferred: ${currentBytes}/${this.totalBytes} B`);
        }
      }

      if (shouldAlert) {
        this.isStallWarningActive = true;
        console.warn(`[WebRTC Watchdog Fired] Reason: ${warningMessage} | iceConnectionState: ${iceState} | bufferedAmount: ${bufferedAmount} B | bytesTransferredInWindow: ${bytesDiff} B | zeroProgressDuration: ${this.zeroProgressCount}s`);
        if (this.callbacks.onStall) {
          this.callbacks.onStall(warningMessage);
        }
      } else if (this.isStallWarningActive && (iceState === 'connected' || iceState === 'completed') && bytesDiff > 0) {
        this.isStallWarningActive = false;
        console.log('[WebRTC Watchdog] ✅ Connection confirmed healthy and bytes flowing.');
        if (this.callbacks.onStallRecovered) {
          this.callbacks.onStallRecovered();
        }
      }

      this.lastWatchdogBytes = currentBytes;
    }, 1000);
  }

  stopWatchdog() {
    if (this.watchdogInterval) {
      clearInterval(this.watchdogInterval);
      this.watchdogInterval = null;
    }
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;
  }

  // --- THROTTLED SYNCHRONIZED METRICS ENGINE ---
  throttledUpdateMetrics(currentBytes, totalBytes, force = false) {
    const now = performance.now();
    const elapsedSinceLastUi = now - this.lastUiUpdateTime;

    if (!force && elapsedSinceLastUi < 100 && currentBytes < totalBytes) {
      return;
    }
    this.lastUiUpdateTime = now;

    const elapsedTotal = (now - this.startTime) / 1000;
    const elapsedSample = (now - this.lastSampleTime) / 1000;

    let instantSpeedMB = 0;
    if (elapsedSample > 0.05) {
      const bytesDiff = currentBytes - this.lastSampleBytes;
      instantSpeedMB = (bytesDiff / elapsedSample) / (1024 * 1024);
      this.lastSampleTime = now;
      this.lastSampleBytes = currentBytes;
    } else if (elapsedTotal > 0.05) {
      instantSpeedMB = (currentBytes / elapsedTotal) / (1024 * 1024);
    }
    this.currentSpeedMB = Math.max(0, instantSpeedMB);

    const percent = totalBytes > 0 ? Math.min(100, Math.floor((currentBytes / totalBytes) * 100)) : 0;
    const remainingBytes = Math.max(0, totalBytes - currentBytes);

    let etaSeconds = 0;
    if (currentBytes >= totalBytes) {
      etaSeconds = 0;
    } else if (this.currentSpeedMB > 0) {
      etaSeconds = Math.ceil((remainingBytes / (1024 * 1024)) / this.currentSpeedMB);
    } else if (elapsedTotal > 0 && currentBytes > 0) {
      const avgSpeedMB = (currentBytes / elapsedTotal) / (1024 * 1024);
      etaSeconds = Math.ceil((remainingBytes / (1024 * 1024)) / (avgSpeedMB || 1));
    }

    if (this.callbacks.onProgress) {
      this.callbacks.onProgress({
        percent,
        speedMB: this.currentSpeedMB,
        etaSeconds,
        transferredBytes: currentBytes,
        totalBytes
      });
    }
  }

  handleTransferSuccess(blob = null) {
    this.isTransferring = false;
    this.stopWatchdog();
    if (this.callbacks.onStallRecovered) this.callbacks.onStallRecovered();
    if (this.statsInterval) clearInterval(this.statsInterval);

    sound.playSuccess();

    this.sendSignal({
      type: 'transfer-complete',
      pin: this.pin
    });

    if (this.callbacks.onComplete) {
      this.callbacks.onComplete({
        role: this.role,
        meta: this.fileMeta,
        blob: blob,
        downloadUrl: blob ? URL.createObjectURL(blob) : null
      });
    }

    this.receivedChunks = [];
  }

  cleanupTransfer() {
    this.isTransferring = false;
    this.stopWatchdog();
    if (this.callbacks.onStallRecovered) this.callbacks.onStallRecovered();
    if (this.statsInterval) clearInterval(this.statsInterval);
    if (this.dataChannel) {
      try { this.dataChannel.close(); } catch (e) {}
      this.dataChannel = null;
    }
    if (this.pc) {
      try { this.pc.close(); } catch (e) {}
      this.pc = null;
    }
    this.receivedChunks = [];
    this.pendingCandidates = [];
  }

  getDeviceLabel() {
    const ua = navigator.userAgent;
    if (/iPhone|iPad|iPod/i.test(ua)) return 'Apple iOS Device';
    if (/Android/i.test(ua)) return 'Android Device';
    if (/Macintosh/i.test(ua)) return 'macOS Workstation';
    if (/Windows/i.test(ua)) return 'Windows PC';
    if (/Linux/i.test(ua)) return 'Linux Machine';
    return 'Web Client';
  }
}
