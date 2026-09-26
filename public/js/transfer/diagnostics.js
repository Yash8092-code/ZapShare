// ZapShare Comprehensive Transfer Diagnostics (Phases 2, 27)
// Tracks 5 separate byte counters, network bottlenecks, device profile, and dev panel
import { RouteType, NetworkClass, BottleneckType, TransferMode, DeviceClass } from './constants.js';

export class TransferDiagnostics {
  constructor(options = {}) {
    this.role = options.role || 'sender';

    // The Five Independent Byte Counters (Phase 2)
    this.totalBytes = 0;
    this.bytesRead = 0;
    this.bytesQueued = 0;
    this.bytesTransported = 0;
    this.bytesReceived = 0;
    this.bytesConfirmed = 0;

    // Speeds & Throughput (Calculated from bytesConfirmed)
    this.startTime = 0;
    this.lastSampleTime = 0;
    this.lastSampleConfirmedBytes = 0;
    this.effectiveSpeedMB = 0;
    this.peakSpeedMB = 0;

    // WebRTC connection stats
    this.iceState = 'new';
    this.peerState = 'new';
    this.dataChannelState = 'none';
    this.controlChannelState = 'none';
    this.routeType = RouteType.UNKNOWN;
    this.activeCandidatePair = null;
    this.rttMs = null;
    this.reconnectCount = 0;

    // Pipeline & Flow Control
    this.backpressureWaitTimeMs = 0;
    this.currentChunkSize = 64 * 1024;
    this.storageMode = 'none';
    this.storageLatencyMs = 0;
    this.queueSize = 0;

    // Device Profile & Network Intelligence
    this.deviceClass = DeviceClass.DESKTOP;
    this.transferMode = TransferMode.BALANCED;
    this.networkClass = NetworkClass.MODERATE;
    this.bottleneck = BottleneckType.NONE;
    this.postCancelDroppedBytes = 0;
    this.cancellationState = 'none';
    this.workerStatus = 'active';

    // Dev panel DOM element
    this.panelElement = null;
    this.isPanelVisible = false;

    this.setupKeyboardShortcut();
  }

  setRole(role) {
    this.role = role;
  }

  start(totalBytes) {
    this.totalBytes = totalBytes;
    this.bytesRead = 0;
    this.bytesQueued = 0;
    this.bytesTransported = 0;
    this.bytesReceived = 0;
    this.bytesConfirmed = 0;
    this.startTime = performance.now();
    this.lastSampleTime = performance.now();
    this.lastSampleConfirmedBytes = 0;
    this.effectiveSpeedMB = 0;
    this.peakSpeedMB = 0;
    this.postCancelDroppedBytes = 0;
  }

  updateProgress(metrics = {}) {
    if (metrics.bytesRead !== undefined) this.bytesRead = metrics.bytesRead;
    if (metrics.bytesQueued !== undefined) this.bytesQueued = metrics.bytesQueued;
    if (metrics.bytesTransported !== undefined) this.bytesTransported = metrics.bytesTransported;
    if (metrics.bytesReceived !== undefined) this.bytesReceived = metrics.bytesReceived;
    if (metrics.bytesConfirmed !== undefined) this.bytesConfirmed = metrics.bytesConfirmed;

    if (metrics.currentChunkSize !== undefined) this.currentChunkSize = metrics.currentChunkSize;
    if (metrics.backpressureWaitTimeMs !== undefined) this.backpressureWaitTimeMs = metrics.backpressureWaitTimeMs;
    if (metrics.storageMode !== undefined) this.storageMode = metrics.storageMode;
    if (metrics.storageLatencyMs !== undefined) this.storageLatencyMs = metrics.storageLatencyMs;
    if (metrics.queueSize !== undefined) this.queueSize = metrics.queueSize;
    if (metrics.deviceClass !== undefined) this.deviceClass = metrics.deviceClass;
    if (metrics.transferMode !== undefined) this.transferMode = metrics.transferMode;
    if (metrics.networkClass !== undefined) this.networkClass = metrics.networkClass;
    if (metrics.bottleneck !== undefined) this.bottleneck = metrics.bottleneck;
    if (metrics.cancellationState !== undefined) this.cancellationState = metrics.cancellationState;

    const now = performance.now();
    const elapsedSample = (now - this.lastSampleTime) / 1000;

    // Throughput calculated exclusively on receiver-confirmed bytes (or received bytes for receiver)
    const currentProgressBytes = this.bytesConfirmed || this.bytesReceived;
    if (elapsedSample >= 0.2) {
      const bytesDiff = currentProgressBytes - this.lastSampleConfirmedBytes;
      if (bytesDiff >= 0) {
        this.effectiveSpeedMB = Math.max(0, (bytesDiff / elapsedSample) / (1024 * 1024));
        if (this.effectiveSpeedMB > this.peakSpeedMB) {
          this.peakSpeedMB = this.effectiveSpeedMB;
        }
      }
      this.lastSampleTime = now;
      this.lastSampleConfirmedBytes = currentProgressBytes;
    }

    if (this.isPanelVisible) {
      this.renderPanel();
    }
  }

  recordPostCancelDrop(droppedBytes) {
    this.postCancelDroppedBytes = droppedBytes;
    if (this.isPanelVisible) this.renderPanel();
  }

  async inspectPeerConnection(pc) {
    if (!pc) return;
    try {
      this.iceState = pc.iceConnectionState;
      this.peerState = pc.connectionState;

      const stats = await pc.getStats();
      let selectedPair = null;

      stats.forEach((report) => {
        if (report.type === 'transport' && report.selectedCandidatePairId) {
          selectedPair = stats.get(report.selectedCandidatePairId);
        }
        if (!selectedPair && report.type === 'candidate-pair' && (report.selected || report.nominated || report.state === 'succeeded')) {
          selectedPair = report;
        }
      });

      if (selectedPair) {
        const local = stats.get(selectedPair.localCandidateId);
        const remote = stats.get(selectedPair.remoteCandidateId);

        if (selectedPair.currentRoundTripTime !== undefined) {
          this.rttMs = Math.round(selectedPair.currentRoundTripTime * 1000);
        }

        this.activeCandidatePair = {
          localType: local?.candidateType || 'unknown',
          remoteType: remote?.candidateType || 'unknown',
          protocol: local?.protocol || remote?.protocol || 'udp',
          localAddress: local?.address || 'local',
          remoteAddress: remote?.address || 'remote'
        };

        if (local?.candidateType === 'relay' || remote?.candidateType === 'relay') {
          this.routeType = RouteType.TURN;
        } else if (local?.candidateType === 'host' && remote?.candidateType === 'host') {
          this.routeType = RouteType.LAN;
        } else {
          this.routeType = RouteType.STUN;
        }
      }
    } catch (e) {
      console.warn('[Diagnostics] Error getting stats:', e);
    }
  }

  getSnapshot() {
    const elapsedSec = (performance.now() - this.startTime) / 1000;
    const progressBytes = this.bytesConfirmed || this.bytesReceived;
    const avgSpeedMB = elapsedSec > 0.1 ? (progressBytes / elapsedSec) / (1024 * 1024) : 0;

    return {
      role: this.role,
      iceState: this.iceState,
      peerState: this.peerState,
      dataChannelState: this.dataChannelState,
      routeType: this.routeType,
      activeCandidatePair: this.activeCandidatePair,
      rttMs: this.rttMs,
      totalBytes: this.totalBytes,
      bytesRead: this.bytesRead,
      bytesQueued: this.bytesQueued,
      bytesTransported: this.bytesTransported,
      bytesReceived: this.bytesReceived,
      bytesConfirmed: this.bytesConfirmed,
      effectiveSpeedMB: this.effectiveSpeedMB,
      avgSpeedMB,
      peakSpeedMB: this.peakSpeedMB,
      currentChunkSize: this.currentChunkSize,
      backpressureWaitTimeMs: this.backpressureWaitTimeMs,
      storageMode: this.storageMode,
      storageLatencyMs: this.storageLatencyMs,
      queueSize: this.queueSize,
      deviceClass: this.deviceClass,
      transferMode: this.transferMode,
      networkClass: this.networkClass,
      bottleneck: this.bottleneck,
      postCancelDroppedBytes: this.postCancelDroppedBytes,
      cancellationState: this.cancellationState,
      reconnectCount: this.reconnectCount
    };
  }

  setupKeyboardShortcut() {
    if (typeof window === 'undefined') return;
    window.addEventListener('keydown', (e) => {
      if ((e.ctrlKey || e.metaKey) && e.shiftKey && (e.key === 'D' || e.key === 'd')) {
        e.preventDefault();
        this.togglePanel();
      }
    });
  }

  togglePanel() {
    this.isPanelVisible = !this.isPanelVisible;
    if (this.isPanelVisible) {
      this.showPanel();
    } else {
      this.hidePanel();
    }
  }

  showPanel() {
    if (!this.panelElement) {
      this.createPanel();
    }
    if (this.panelElement) {
      this.panelElement.style.display = 'block';
      this.renderPanel();
    }
  }

  hidePanel() {
    if (this.panelElement) {
      this.panelElement.style.display = 'none';
    }
  }

  createPanel() {
    const panel = document.createElement('div');
    panel.id = 'zapshare-dev-diagnostics';
    panel.style.cssText = `
      position: fixed;
      bottom: 16px;
      right: 16px;
      width: 380px;
      max-height: 520px;
      background: rgba(10, 14, 26, 0.96);
      border: 1px solid rgba(6, 182, 212, 0.4);
      border-radius: 14px;
      box-shadow: 0 12px 35px rgba(0, 0, 0, 0.7);
      backdrop-filter: blur(16px);
      color: #E2E8F0;
      font-family: 'Space Grotesk', monospace, sans-serif;
      font-size: 11px;
      line-height: 1.45;
      padding: 14px;
      z-index: 99999;
      overflow-y: auto;
      display: none;
    `;
    document.body.appendChild(panel);
    this.panelElement = panel;
  }

  renderPanel() {
    if (!this.panelElement) return;
    const s = this.getSnapshot();
    const formatBytes = (bytes) => {
      if (!bytes) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
    };

    this.panelElement.innerHTML = `
      <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 6px; margin-bottom: 8px;">
        <span style="font-weight: 700; color: #06B6D4; letter-spacing: 0.05em;">⚡ ZAPSHARE DIAGNOSTICS</span>
        <button id="btn-close-diag" style="background: none; border: none; color: #94A3B8; cursor: pointer; font-size: 14px;">✕</button>
      </div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 5px; margin-bottom: 8px;">
        <div><strong>Role:</strong> ${s.role.toUpperCase()} (${s.deviceClass})</div>
        <div><strong>Mode:</strong> <span style="color: #38BDF8;">${s.transferMode}</span></div>
        <div><strong>Route:</strong> <span style="color: ${s.routeType.includes('LAN') ? '#10B981' : s.routeType.includes('STUN') ? '#06B6D4' : '#F59E0B'}">${s.routeType.split(' ')[0]}</span></div>
        <div><strong>Network:</strong> <span style="color: ${s.networkClass === 'VERY_SLOW' ? '#EF4444' : s.networkClass === 'SLOW' ? '#F59E0B' : '#10B981'}">${s.networkClass}</span></div>
        <div><strong>Bottleneck:</strong> <span style="color: ${s.bottleneck === 'NONE' ? '#10B981' : '#F59E0B'}; font-weight: bold;">${s.bottleneck}</span></div>
        <div><strong>RTT:</strong> ${s.rttMs !== null ? s.rttMs + ' ms' : '--'}</div>
        <div><strong>ICE State:</strong> ${s.iceState}</div>
        <div><strong>Storage:</strong> ${s.storageMode}</div>
      </div>
      <div style="border-top: 1px solid rgba(255,255,255,0.08); padding-top: 6px; margin-bottom: 6px;">
        <div style="color: #94A3B8; font-size: 10px; margin-bottom: 3px;"><strong>FIVE BYTE COUNTERS:</strong></div>
        <div>1. Read: <strong>${formatBytes(s.bytesRead)}</strong></div>
        <div>2. Queued: <strong>${formatBytes(s.bytesQueued)}</strong></div>
        <div>3. Transported: <strong>${formatBytes(s.bytesTransported)}</strong></div>
        <div>4. Received: <strong>${formatBytes(s.bytesReceived)}</strong></div>
        <div>5. Confirmed: <strong style="color: #10B981;">${formatBytes(s.bytesConfirmed)}</strong> (${s.totalBytes > 0 ? ((s.bytesConfirmed / s.totalBytes) * 100).toFixed(1) : 0}%)</div>
      </div>
      <div style="border-top: 1px solid rgba(255,255,255,0.08); padding-top: 6px; margin-bottom: 4px;">
        <div><strong>Chunk:</strong> ${s.currentChunkSize / 1024} KB | <strong>Wait:</strong> ${s.backpressureWaitTimeMs} ms</div>
        <div><strong>Speed:</strong> <span style="color: #06B6D4; font-weight: bold;">${s.effectiveSpeedMB.toFixed(2)} MB/s</span> (Peak: ${s.peakSpeedMB.toFixed(2)} MB/s)</div>
        ${s.postCancelDroppedBytes > 0 ? `<div style="color: #F87171;"><strong>Post-Cancel Dropped:</strong> ${formatBytes(s.postCancelDroppedBytes)}</div>` : ''}
      </div>
      <div style="font-size: 9px; color: #64748B; text-align: right; margin-top: 4px;">
        Toggle: Ctrl+Shift+D
      </div>
    `;

    const closeBtn = this.panelElement.querySelector('#btn-close-diag');
    if (closeBtn) {
      closeBtn.onclick = () => this.hidePanel();
    }
  }

  destroy() {
    if (this.panelElement && this.panelElement.parentNode) {
      this.panelElement.parentNode.removeChild(this.panelElement);
      this.panelElement = null;
    }
  }
}
