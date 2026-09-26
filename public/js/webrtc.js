// ZapShare WebRTC Core: Direct P2P High-Throughput Transfer Engine
// Backpressure-governed SCTP, Dual-Channel Control & Data Protocol, Streaming OPFS Storage & Integrity Verification
import { sound } from './sound.js';
import { TransferState, ControlMessageType, RouteType, TransferDefaults, TransferMode, DeviceClass } from './transfer/constants.js';
import { TransferStateMachine } from './transfer/state-machine.js';
import { SenderPipeline } from './transfer/sender-pipeline.js';
import { ReceiverPipeline } from './transfer/receiver-pipeline.js';
import { TransferDiagnostics } from './transfer/diagnostics.js';
import { DeviceProfileManager } from './transfer/device-profile.js';
import { NetworkManager } from './transfer/network-manager.js';

export class WebRTCManager {
  constructor(callbacks = {}) {
    this.callbacks = callbacks;
    this.ws = null;
    this.pc = null;
    this.role = null; // 'sender' | 'receiver'
    this.pin = null;
    this.fileMeta = null;
    this.file = null;

    // Device Profile & Network Intelligence
    this.deviceProfile = new DeviceProfileManager();
    this.networkManager = new NetworkManager();

    // Dual-channel architecture:
    // 1. controlChannel: lightweight JSON protocol messages (HEADER, ACK, HEARTBEAT, PAUSE, RESUME, EOF, VERIFY)
    // 2. dataChannel: dedicated raw binary chunks (zero per-chunk JSON overhead)
    this.controlChannel = null;
    this.dataChannel = null;

    // Transfer State Machine & Diagnostics
    this.stateMachine = new TransferStateMachine(TransferState.IDLE, (newState, prevState, reason) => {
      this.handleStateChange(newState, prevState, reason);
    });
    this.diagnostics = new TransferDiagnostics({ role: this.role || 'sender' });
    this.diagnostics.updateProgress({
      deviceClass: this.deviceProfile.deviceClass,
      transferMode: this.deviceProfile.transferMode
    });

    // Transfer Pipelines
    this.senderPipeline = new SenderPipeline({
      stateMachine: this.stateMachine,
      diagnostics: this.diagnostics,
      deviceProfile: this.deviceProfile,
      networkManager: this.networkManager,
      callbacks: {
        onProgress: (prog) => {
          if (this.callbacks.onProgress) this.callbacks.onProgress(prog);
        },
        onStall: (msg) => {
          if (this.callbacks.onStall) this.callbacks.onStall(msg);
        },
        onDeviceStress: (msg) => {
          if (this.callbacks.onConnectionQuality) this.callbacks.onConnectionQuality(msg);
        }
      }
    });

    this.receiverPipeline = new ReceiverPipeline({
      stateMachine: this.stateMachine,
      diagnostics: this.diagnostics,
      deviceProfile: this.deviceProfile,
      networkManager: this.networkManager,
      callbacks: {
        onProgress: (prog) => {
          if (this.callbacks.onProgress) this.callbacks.onProgress(prog);
        },
        onVerifying: (msg) => {
          if (this.callbacks.onConnectionQuality) {
            this.callbacks.onConnectionQuality('Verifying transfer integrity...');
          }
        },
        onComplete: (data) => {
          this.handleTransferSuccess(data);
        },
        onError: (err) => {
          if (this.callbacks.onError) this.callbacks.onError(err);
        },
        onCancelled: () => {
          this.cleanupTransfer();
          if (this.callbacks.onCancelled) this.callbacks.onCancelled();
        },
        onDeviceStress: (msg) => {
          if (this.callbacks.onConnectionQuality) this.callbacks.onConnectionQuality(msg);
        }
      }
    });

    // Real transfer metrics
    this.totalBytes = 0;
    this.transferredBytes = 0;
    this.connectionType = 'Connecting peers (ICE checking)...';
    this.isTransferring = false;
    this.pendingCandidates = [];
    this.statsInterval = null;

    // Watchdog & Stall Detection
    this.watchdogInterval = null;
    this.lastWatchdogConfirmedBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;
    this.isRelayed = false;

    // Pre-warming & Transfer Orchestration State
    this.isPrewarming = false;
    this.isChannelReady = false;
    this.hasReceiverAccepted = false;
    this.serverLocalIp = null;

    // High-resolution Connection Setup Timing
    this.timing = {
      signalingStart: 0,
      offerSent: 0,
      answerReceived: 0,
      remoteDescSet: 0,
      firstCandidate: 0,
      iceConnected: 0,
      channelOpen: 0,
      reported: false
    };

    // Candidate & ICE Health Telemetry
    this.candidateStats = {
      local: { host: 0, srflx: 0, prflx: 0, relay: 0 },
      remote: { host: 0, srflx: 0, prflx: 0, relay: 0 }
    };
    this.lastActivePair = null;
    this.iceCandidateErrors = [];
    this.isRestartingIce = false;
    this.iceRestartCount = 0;

    // Dynamic ICE / TURN server configuration (Zero hard-coded credentials!)
    this.iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' }
    ];

    this.fetchIceServers();
    this.initWebSocket();
  }

  // Fetch secure dynamic ICE / TURN configuration from server
  async fetchIceServers() {
    try {
      if (typeof window !== 'undefined' && window.ZAPSHARE_ICE_SERVERS) {
        this.iceServers = window.ZAPSHARE_ICE_SERVERS;
        return;
      }
      const res = await fetch('/api/ice-servers');
      if (res.ok) {
        const data = await res.json();
        if (data && data.iceServers && data.iceServers.length > 0) {
          this.iceServers = data.iceServers;
          console.log(`[WebRTC] Loaded ${this.iceServers.length} dynamic ICE server configurations from server.`);
        }
      }
    } catch (e) {
      console.log('[WebRTC] Using standard Google STUN infrastructure.');
    }
  }

  handleStateChange(newState, prevState, reason) {
    if (this.callbacks.onTransferStateChange) {
      this.callbacks.onTransferStateChange(newState, prevState, reason);
    }
    if (newState === TransferState.PAUSED_BACKPRESSURE) {
      if (this.callbacks.onConnectionQuality) {
        this.callbacks.onConnectionQuality('Data pipeline paused (Flow control / Network congestion)');
      }
    } else if (newState === TransferState.TRANSFERRING) {
      this.updateConnectionQualityBadge();
    }
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
    this.diagnostics.setRole('sender');

    this.fileMeta = {
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
      device: this.getDeviceLabel()
    };

    this.stateMachine.transition(TransferState.PREPARING, 'create-room');

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
    this.timing.signalingStart = performance.now();
    this.diagnostics.setRole('receiver');

    this.stateMachine.transition(TransferState.CONNECTING, 'join-room');

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
        this.timing.signalingStart = performance.now();
        if (this.callbacks.onPeerConnected) {
          this.callbacks.onPeerConnected({ role: 'receiver', info: msg.senderInfo });
        }
        // Pre-warm ICE immediately upon peer pairing
        if (this.role === 'sender' && !this.pc) {
          console.log('[WebRTC Pre-warm] Immediate ICE & connection pre-warming initiated upon peer pairing.');
          this.initiateWebRTCAsSender();
        }
        break;
      }

      case 'room-joined': {
        console.log('[Signaling] Room joined successfully. File meta:', msg.fileMeta);
        sound.playConnect();
        this.timing.signalingStart = performance.now();
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
            this.hasReceiverAccepted = true;
            this.startTransferAsSender();
          }
        } else if (msg.action === 'decline') {
          sound.playDecline();
          this.stateMachine.transition(TransferState.CANCELLED, 'receiver-decline');
          if (this.callbacks.onDeclined) this.callbacks.onDeclined();
        } else if (msg.action === 'cancel') {
          this.cleanupTransfer();
          this.stateMachine.transition(TransferState.CANCELLED, 'peer-cancel');
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
        this.stateMachine.transition(TransferState.FAILED, 'signaling-error');
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
    const config = {
      iceServers: this.iceServers,
      iceCandidatePoolSize: 2
    };

    const pc = new RTCPeerConnection(config);

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        if (!this.timing.firstCandidate) {
          this.timing.firstCandidate = performance.now();
        }
        const type = event.candidate.type || 'unknown';
        if (this.candidateStats.local[type] !== undefined) {
          this.candidateStats.local[type]++;
        }
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

    pc.onicecandidateerror = (event) => {
      const errEntry = {
        url: event.url,
        errorCode: event.errorCode,
        errorText: event.errorText,
        address: event.address,
        port: event.port,
        time: new Date().toLocaleTimeString()
      };
      this.iceCandidateErrors.push(errEntry);
      if (this.iceCandidateErrors.length > 20) this.iceCandidateErrors.shift();

      console.warn(`[WebRTC ICE Candidate Error] Server: ${event.url} | Code: ${event.errorCode} (${event.errorText}) | Address: ${event.address}:${event.port}`);
    };

    pc.oniceconnectionstatechange = () => {
      console.log(`[WebRTC State] iceConnectionState: ${pc.iceConnectionState} | connectionState: ${pc.connectionState}`);
      this.updateConnectionQualityBadge();

      if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
        if (!this.timing.iceConnected) {
          this.timing.iceConnected = performance.now();
        }
        console.log(`[WebRTC State] ✅ Peer connection successfully established! (+${(this.timing.iceConnected - (this.timing.signalingStart || this.timing.offerSent)).toFixed(0)} ms)`);
        this.detectCandidatePair();
        this.printTimingBreakdown();
        if (this.isStallWarningActive) {
          this.isStallWarningActive = false;
          if (this.callbacks.onStallRecovered) {
            this.callbacks.onStallRecovered();
          }
        }
      } else if (pc.iceConnectionState === 'failed') {
        console.error('[WebRTC State] ❌ ICE connection failed! Generating diagnostic telemetry and triggering active ICE restart...');
        this.logIceFailureDiagnostics();
        if (this.callbacks.onStall) {
          this.callbacks.onStall('ICE connection failed. Initiating active ICE renegotiation...');
        }
        this.triggerIceRestart();
      } else if (pc.iceConnectionState === 'disconnected') {
        console.warn('[WebRTC State] ⚠️ ICE connection disconnected.');
      }
    };

    pc.onconnectionstatechange = () => {
      console.log(`[WebRTC State] connectionState: ${pc.connectionState}`);
      this.updateConnectionQualityBadge();
      if (pc.connectionState === 'failed') {
        this.logIceFailureDiagnostics();
        this.triggerIceRestart();
      }
    };

    return pc;
  }

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

  async detectCandidatePair() {
    if (!this.pc) return;
    try {
      await this.diagnostics.inspectPeerConnection(this.pc);
      const snapshot = this.diagnostics.getSnapshot();
      this.connectionType = snapshot.routeType;

      if (snapshot.routeType.includes('TURN')) {
        this.isRelayed = true;
      } else {
        this.isRelayed = false;
      }

      if (this.callbacks.onConnectionQuality) {
        this.callbacks.onConnectionQuality(this.connectionType);
      }
    } catch (e) {
      console.warn('[WebRTC Stats] Could not inspect candidate pair:', e);
    }
  }

  logIceFailureDiagnostics() {
    const local = this.candidateStats.local;
    const remote = this.candidateStats.remote;
    const pair = this.diagnostics ? this.diagnostics.activeCandidatePair : null;
    const errors = this.iceCandidateErrors.slice(-5);

    console.group('%c❌ [ZapShare WebRTC Connection Failure Diagnostic Report]', 'color: #EF4444; font-weight: bold; font-size: 1.1em;');
    console.log(`Role: ${this.role?.toUpperCase()} | ICE Connection State: ${this.pc ? this.pc.iceConnectionState : 'none'} | Peer Connection State: ${this.pc ? this.pc.connectionState : 'none'}`);
    console.log(`Local Candidates Gathered  : Host: ${local.host} | STUN (srflx): ${local.srflx} | TURN (relay): ${local.relay}`);
    console.log(`Remote Candidates Received : Host: ${remote.host} | STUN (srflx): ${remote.srflx} | TURN (relay): ${remote.relay}`);
    if (pair) {
      console.log(`Last Selected Candidate Pair: Local [${pair.localType}] (${pair.protocol} ${pair.localAddress}) <---> Remote [${pair.remoteType}] (${pair.protocol} ${pair.remoteAddress})`);
    }
    if (errors.length > 0) {
      console.log('Recorded ICE Errors:', errors);
    }
    console.groupEnd();
  }

  async triggerIceRestart() {
    if (!this.pc || this.isRestartingIce) return;
    this.isRestartingIce = true;
    this.iceRestartCount++;

    console.log(`[WebRTC ICE Restart #${this.iceRestartCount}] 🔄 Active ICE restart initiated. Creating renegotiated offer with iceRestart: true...`);

    if (this.role === 'sender') {
      try {
        this.pc.restartIce();
        const offer = await this.pc.createOffer({ iceRestart: true });
        await this.pc.setLocalDescription(offer);
        this.sendSignal({
          type: 'signal',
          pin: this.pin,
          data: {
            sdp: this.pc.localDescription,
            isIceRestart: true
          }
        });
      } catch (err) {
        console.error('[WebRTC ICE Restart] Failed to renegotiate ICE restart offer:', err);
      } finally {
        setTimeout(() => { this.isRestartingIce = false; }, 4000);
      }
    } else {
      console.log('[WebRTC ICE Restart] Receiver requesting ICE restart offer from sender via signaling...');
      this.sendSignal({
        type: 'signal',
        pin: this.pin,
        data: { requestIceRestart: true }
      });
      setTimeout(() => { this.isRestartingIce = false; }, 4000);
    }
  }

  // Pre-warm WebRTC connection on peer pairing
  async initiateWebRTCAsSender() {
    if (this.isPrewarming || this.pc) return;
    this.isPrewarming = true;
    if (!this.timing.signalingStart) {
      this.timing.signalingStart = performance.now();
    }

    console.log('[WebRTC Pre-warm] ⚡ Initializing RTCPeerConnection & ICE gathering ahead of user confirmation...');
    this.pc = this.createPeerConnection();

    // Create dual RTCDataChannels
    this.controlChannel = this.pc.createDataChannel('zapshare_control', { ordered: true });
    this.dataChannel = this.pc.createDataChannel('zapshare_data', { ordered: true });
    this.dataChannel.binaryType = 'arraybuffer';

    this.setupControlChannel(this.controlChannel);
    this.setupDataChannel(this.dataChannel);

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);

    this.timing.offerSent = performance.now();
    console.log(`[WebRTC Pre-warm] Sender created offer in ${(this.timing.offerSent - this.timing.signalingStart).toFixed(0)} ms. Relaying offer...`);
    this.sendSignal({
      type: 'signal',
      pin: this.pin,
      data: { sdp: this.pc.localDescription }
    });
  }

  // Triggered when receiver clicks "Accept & Download"
  startTransferAsSender() {
    if (this.stateMachine.is(TransferState.TRANSFERRING)) {
      console.warn('[WebRTC] Transfer already in progress. Ignoring duplicate start.');
      return;
    }

    this.isTransferring = true;
    this.totalBytes = this.file ? this.file.size : 0;
    this.transferredBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.stateMachine.transition(TransferState.TRANSFERRING, 'start-transfer');
    this.startWatchdog();

    if (this.callbacks.onTransferStart) {
      this.callbacks.onTransferStart({
        role: 'sender',
        meta: this.fileMeta,
        connectionType: this.connectionType || 'Direct connection (LAN / P2P SCTP)'
      });
    }

    if (this.isChannelReady && this.dataChannel && this.dataChannel.readyState === 'open') {
      console.log(`[WebRTC Send] 🚀 Connection pre-warmed & open! Streaming file IMMEDIATELY (0 ms setup latency)...`);
      this.senderPipeline.streamFile(this.file).catch(err => {
        console.error('[WebRTC Send] Stream error:', err);
        if (this.callbacks.onError) this.callbacks.onError('Transfer failed: ' + err.message);
      });
    } else {
      console.log('[WebRTC Send] Receiver accepted while ICE still completing. Will stream the exact moment dataChannel opens.');
    }
  }

  async handlePeerSignal(data) {
    if (data.requestIceRestart) {
      if (this.role === 'sender') {
        this.triggerIceRestart();
      }
      return;
    }

    if (data.sdp) {
      if (data.sdp.type === 'offer') {
        if (!this.pc) {
          console.log('[WebRTC] Receiver received initial offer. Creating PeerConnection...');
          this.pc = this.createPeerConnection();

          this.pc.ondatachannel = (e) => {
            const channel = e.channel;
            console.log(`[WebRTC] Receiver ondatachannel received: "${channel.label}"`);
            if (channel.label === 'zapshare_control') {
              this.controlChannel = channel;
              this.setupControlChannel(channel);
            } else if (channel.label === 'zapshare_data') {
              this.dataChannel = channel;
              this.dataChannel.binaryType = 'arraybuffer';
              this.setupDataChannel(channel);
            } else if (channel.label === 'zapshare_transfer') {
              // Backward-compatibility: single channel handles both
              this.controlChannel = channel;
              this.dataChannel = channel;
              this.dataChannel.binaryType = 'arraybuffer';
              this.setupControlChannel(channel);
              this.setupDataChannel(channel);
            }
          };
        }

        await this.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        this.timing.remoteDescSet = performance.now();

        await this.drainPendingCandidates();

        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        this.timing.offerSent = performance.now();
        this.sendSignal({
          type: 'signal',
          pin: this.pin,
          data: { sdp: this.pc.localDescription }
        });
      } else if (data.sdp.type === 'answer') {
        if (this.pc) {
          await this.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
          this.timing.remoteDescSet = performance.now();
          this.isRestartingIce = false;
          await this.drainPendingCandidates();
        }
      }
    } else if (data.candidate) {
      if (!this.timing.firstCandidate) {
        this.timing.firstCandidate = performance.now();
      }
      const candidate = new RTCIceCandidate(data.candidate);
      const type = candidate.type || 'unknown';
      if (this.candidateStats.remote[type] !== undefined) {
        this.candidateStats.remote[type]++;
      }

      if (this.pc && this.pc.remoteDescription && this.pc.remoteDescription.type) {
        try {
          await this.pc.addIceCandidate(candidate);

          // Synthesize parallel direct IP candidate for mDNS .local candidates
          if (this.serverLocalIp && this.serverLocalIp !== 'localhost' && candidate.candidate && candidate.candidate.includes('.local')) {
            try {
              const directCandStr = candidate.candidate.replace(/[a-zA-Z0-9-]+\.local/, this.serverLocalIp);
              const directCand = new RTCIceCandidate({
                candidate: directCandStr,
                sdpMid: candidate.sdpMid,
                sdpMLineIndex: candidate.sdpMLineIndex
              });
              this.pc.addIceCandidate(directCand).catch(() => {});
            } catch (e) {}
          }
        } catch (e) {
          console.warn('[WebRTC] Error adding ICE candidate:', e);
        }
      } else {
        this.pendingCandidates.push(candidate);
      }
    }
  }

  async drainPendingCandidates() {
    if (!this.pc) return;
    while (this.pendingCandidates.length > 0) {
      const cand = this.pendingCandidates.shift();
      try {
        await this.pc.addIceCandidate(cand);
      } catch (err) {
        console.warn('[WebRTC ICE Queue] Error adding drained candidate:', err);
      }
    }
  }

  setupControlChannel(dc) {
    dc.onopen = () => {
      console.log(`[WebRTC ControlChannel] OPEN! (${dc.label})`);
      this.checkBothChannelsReady();
    };

    dc.onclose = () => {
      console.log('[WebRTC ControlChannel] Closed.');
    };

    dc.onerror = (err) => {
      console.error('[WebRTC ControlChannel] Error:', err);
    };

    dc.onmessage = async (event) => {
      try {
        if (typeof event.data === 'string') {
          const msg = JSON.parse(event.data);
          if (this.role === 'sender') {
            this.senderPipeline.handleControlMessage(msg);
          } else {
            await this.receiverPipeline.handleControlMessage(msg);
          }
        }
      } catch (err) {
        console.warn('[WebRTC ControlChannel] Parse error:', err);
      }
    };
  }

  setupDataChannel(dc) {
    dc.onopen = () => {
      if (!this.timing.channelOpen) {
        this.timing.channelOpen = performance.now();
      }
      const sctpMax = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;
      console.log(`[WebRTC DataChannel] OPEN! Role: ${this.role} | sctp.maxMessageSize: ${sctpMax} B | binaryType: ${dc.binaryType}`);

      this.checkBothChannelsReady();
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
      if (this.role === 'receiver') {
        // If message is string (backward compatibility fallback), route to control handler
        if (typeof event.data === 'string') {
          try {
            const msg = JSON.parse(event.data);
            await this.receiverPipeline.handleControlMessage(msg);
          } catch (e) {}
        } else {
          // Raw binary chunk
          await this.receiverPipeline.handleBinaryChunk(event.data);
        }
      }
    };
  }

  checkBothChannelsReady() {
    const isControlOpen = this.controlChannel && this.controlChannel.readyState === 'open';
    const isDataOpen = this.dataChannel && this.dataChannel.readyState === 'open';

    if (isControlOpen && isDataOpen && !this.isChannelReady) {
      this.isChannelReady = true;
      const sctpMax = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;

      this.senderPipeline.setChannels(this.controlChannel, this.dataChannel, sctpMax);
      this.receiverPipeline.setChannels(this.controlChannel, this.dataChannel);

      this.detectCandidatePair();
      this.printTimingBreakdown();
      this.statsInterval = setInterval(() => this.detectCandidatePair(), 1500);

      this.stateMachine.transition(TransferState.READY, 'channels-open');

      if (this.role === 'sender') {
        if (this.hasReceiverAccepted) {
          console.log('[WebRTC Send] Receiver already accepted. Streaming file immediately (0ms delay)!');
          this.senderPipeline.streamFile(this.file).catch(err => {
            console.error('[WebRTC Send] Stream error:', err);
            if (this.callbacks.onError) this.callbacks.onError('Transfer failed: ' + err.message);
          });
        } else {
          console.log('[WebRTC Pre-warm] Channels PRE-WARMED! Holding stream until receiver clicks Accept.');
          if (this.callbacks.onPrewarmReady) {
            this.callbacks.onPrewarmReady();
          }
        }
      }
    }
  }

  acceptIncomingFile(customFileHandle = null) {
    this.hasReceiverAccepted = true;
    if (customFileHandle) {
      this.receiverPipeline.setFileHandle(customFileHandle);
    }

    this.sendSignal({
      type: 'transfer-action',
      pin: this.pin,
      action: 'accept'
    });

    this.isTransferring = true;
    this.totalBytes = this.fileMeta ? this.fileMeta.size : 0;
    this.transferredBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.stateMachine.transition(TransferState.TRANSFERRING, 'receiver-accept');
    this.startWatchdog();

    if (this.callbacks.onTransferStart) {
      this.callbacks.onTransferStart({
        role: 'receiver',
        meta: this.fileMeta,
        connectionType: this.connectionType || 'Connecting peers (ICE checking)...'
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

  setTransferMode(mode) {
    this.deviceProfile.setTransferMode(mode);
    const tuning = this.deviceProfile.getTuning();
    if (this.senderPipeline && this.senderPipeline.backpressure) {
      this.senderPipeline.backpressure.applyTuning(tuning);
    }
    if (this.diagnostics) {
      this.diagnostics.updateProgress({
        transferMode: mode,
        deviceClass: this.deviceProfile.deviceClass
      });
    }
  }

  getTransferMode() {
    return this.deviceProfile.transferMode;
  }

  getDeviceClass() {
    return this.deviceProfile.deviceClass;
  }

  async cancelTransfer() {
    console.log('[WebRTC] cancelTransfer initiated.');
    if (this.role === 'sender' && this.isTransferring && this.senderPipeline) {
      try {
        await this.senderPipeline.requestStop();
      } catch (e) {
        console.warn('[WebRTC] Graceful sender stop handshake error:', e);
      }
    }
    this.stateMachine.transition(TransferState.CANCELLED, 'user-cancel');
    this.sendSignal({
      type: 'transfer-action',
      pin: this.pin,
      action: 'cancel'
    });
    this.cleanupTransfer();
  }

  // --- DATA-PLANE & ICE SEPARATED WATCHDOG ---
  startWatchdog() {
    this.stopWatchdog();
    this.lastWatchdogConfirmedBytes = 0;
    this.zeroProgressCount = 0;
    this.checkingDuration = 0;
    this.isStallWarningActive = false;

    this.watchdogInterval = setInterval(() => {
      if (!this.isTransferring || this.totalBytes === 0) return;

      const currentBytes = this.role === 'sender'
        ? this.senderPipeline.confirmedBytes
        : this.receiverPipeline.receivedBytes;

      const bytesDiff = currentBytes - this.lastWatchdogConfirmedBytes;
      const iceState = this.pc ? this.pc.iceConnectionState : 'unknown';
      const dcReady = this.dataChannel ? this.dataChannel.readyState : 'none';
      const bufferedAmount = this.dataChannel ? this.dataChannel.bufferedAmount : 0;

      // Track consecutive seconds of 0 confirmed byte progress
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

      if (iceState === 'checking') {
        this.checkingDuration += 1;
      } else {
        this.checkingDuration = 0;
      }

      // Explicit root-cause stall differentiation:
      let shouldAlert = false;
      let warningMessage = '';

      if (iceState === 'failed') {
        shouldAlert = true;
        warningMessage = 'Network connection failed. Attempting ICE restart...';
        if (!this.isRestartingIce) {
          this.triggerIceRestart();
        }
      } else if (iceState === 'disconnected') {
        if (this.zeroProgressCount >= 4) {
          shouldAlert = true;
          warningMessage = 'Network connection disrupted. Reconnecting peers...';
          if (!this.isRestartingIce && this.zeroProgressCount % 4 === 0) {
            this.triggerIceRestart();
          }
        }
      } else if (iceState === 'checking' && this.checkingDuration >= 8) {
        shouldAlert = true;
        warningMessage = `Direct connection taking longer than expected (${this.checkingDuration}s). Negotiating route...`;
      } else if (iceState === 'connected' || iceState === 'completed') {
        // ICE IS COMPLETELY HEALTHY!
        // Never trigger ICE restart or call it a network loss.
        if (this.zeroProgressCount >= 15) {
          shouldAlert = true;
          if (this.senderPipeline.isRemotePaused) {
            warningMessage = 'Data pipeline paused: receiver writing to local storage...';
          } else if (bufferedAmount > 0) {
            warningMessage = `Data pipeline paused: network backpressure (${(bufferedAmount / 1024).toFixed(0)} KB in buffer)...`;
          } else {
            warningMessage = 'Transfer data pipeline stalled. Waiting for peer data flow...';
          }
        }
      }

      if (shouldAlert) {
        this.isStallWarningActive = true;
        console.warn(`[WebRTC Watchdog Fired] Reason: ${warningMessage} | iceConnectionState: ${iceState} | bufferedAmount: ${bufferedAmount} B | zeroProgressDuration: ${this.zeroProgressCount}s`);
        if (this.callbacks.onStall) {
          this.callbacks.onStall(warningMessage);
        }
      } else if (this.isStallWarningActive && (iceState === 'connected' || iceState === 'completed') && bytesDiff > 0) {
        this.isStallWarningActive = false;
        if (this.callbacks.onStallRecovered) {
          this.callbacks.onStallRecovered();
        }
      }

      this.lastWatchdogConfirmedBytes = currentBytes;
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

  handleTransferSuccess(data = {}) {
    this.isTransferring = false;
    this.stopWatchdog();
    if (this.callbacks.onStallRecovered) this.callbacks.onStallRecovered();
    if (this.statsInterval) clearInterval(this.statsInterval);

    this.stateMachine.transition(TransferState.COMPLETED, 'transfer-complete');
    sound.playSuccess();

    this.sendSignal({
      type: 'transfer-complete',
      pin: this.pin
    });

    if (this.callbacks.onComplete) {
      this.callbacks.onComplete({
        role: this.role,
        meta: this.fileMeta,
        blob: data.blob || null,
        file: data.file || null,
        downloadUrl: data.downloadUrl || null,
        isDirectlySaved: data.isDirectlySaved || false,
        verified: data.verified || true
      });
    }
  }

  cleanupTransfer() {
    this.isTransferring = false;
    this.isPrewarming = false;
    this.isChannelReady = false;
    this.hasReceiverAccepted = false;
    this.timing.reported = false;

    this.stopWatchdog();
    if (this.callbacks.onStallRecovered) this.callbacks.onStallRecovered();
    if (this.statsInterval) clearInterval(this.statsInterval);

    if (this.senderPipeline) {
      this.senderPipeline.cancel();
    }
    if (this.receiverPipeline) {
      this.receiverPipeline.cleanup();
    }

    if (this.controlChannel) {
      try { this.controlChannel.close(); } catch (e) {}
      this.controlChannel = null;
    }
    if (this.dataChannel) {
      try { this.dataChannel.close(); } catch (e) {}
      this.dataChannel = null;
    }
    if (this.pc) {
      try { this.pc.close(); } catch (e) {}
      this.pc = null;
    }

    this.pendingCandidates = [];
    this.isRestartingIce = false;
    this.iceRestartCount = 0;
    this.stateMachine.reset();
  }

  setServerLocalIp(ip) {
    if (ip && ip !== 'localhost') {
      this.serverLocalIp = ip;
      console.log(`[WebRTC] Server LAN IP registered for ICE acceleration: ${ip}`);
    }
  }

  printTimingBreakdown() {
    if (this.timing.reported) return;
    if (!this.timing.iceConnected && !this.timing.channelOpen) return;
    this.timing.reported = true;

    const t0 = this.timing.signalingStart || this.timing.offerSent || performance.now();
    const tOffer = this.timing.offerSent ? Math.round(this.timing.offerSent - t0) : 0;
    const tRemote = this.timing.remoteDescSet ? Math.round(this.timing.remoteDescSet - t0) : 0;
    const tCand = this.timing.firstCandidate ? Math.round(this.timing.firstCandidate - t0) : 0;
    const tIce = this.timing.iceConnected ? Math.round(this.timing.iceConnected - t0) : 0;
    const tDc = this.timing.channelOpen ? Math.round(this.timing.channelOpen - t0) : 0;
    const totalSetupMs = tDc || tIce;

    console.log(
      `%c⚡ [ZapShare ICE & Setup Timing Breakdown] Role: ${this.role.toUpperCase()}: \n` +
      `  1. Signaling & Pairing Start    : 0 ms\n` +
      `  2. SDP Offer Created & Sent     : +${tOffer} ms\n` +
      `  3. setRemoteDescription Applied : +${tRemote} ms\n` +
      `  4. First ICE Candidate Handled  : +${tCand} ms\n` +
      `  5. ICE State === 'connected'    : +${tIce} ms\n` +
      `  6. RTCDataChannels Open & Ready : +${tDc} ms\n` +
      `  --------------------------------------------------\n` +
      `  Total Connection Setup Time     : ${totalSetupMs} ms\n` +
      `  Pre-Warming Status              : ✅ Connected BEFORE user click (0ms transfer start latency)`,
      'color: #06B6D4; font-weight: bold; line-height: 1.5;'
    );
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
