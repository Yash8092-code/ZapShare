// ZapShare Transfer Diagnostics & Live Telemetry Panel
// Accurate metrics separation (queued vs confirmed bytes), route detection, and developer panel
import { RouteType } from './constants.js';

export class TransferDiagnostics {
  constructor(options = {}) {
    this.role = options.role || 'sender';
    this.totalBytes = 0;
    this.queuedBytes = 0;
    this.transportBytes = 0;
    this.confirmedBytes = 0;

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
    this.backpressureWaitTimeMs = 0;
    this.currentChunkSize = 64 * 1024;
    this.storageMode = 'none';
    this.queueSize = 0;

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
    this.queuedBytes = 0;
    this.transportBytes = 0;
    this.confirmedBytes = 0;
    this.startTime = performance.now();
    this.lastSampleTime = performance.now();
    this.lastSampleConfirmedBytes = 0;
    this.effectiveSpeedMB = 0;
    this.peakSpeedMB = 0;
  }

  updateProgress({ queuedBytes, transportBytes, confirmedBytes, currentChunkSize, backpressureWaitTimeMs, storageMode, queueSize }) {
    if (queuedBytes !== undefined) this.queuedBytes = queuedBytes;
    if (transportBytes !== undefined) this.transportBytes = transportBytes;
    if (confirmedBytes !== undefined) this.confirmedBytes = confirmedBytes;
    if (currentChunkSize !== undefined) this.currentChunkSize = currentChunkSize;
    if (backpressureWaitTimeMs !== undefined) this.backpressureWaitTimeMs = backpressureWaitTimeMs;
    if (storageMode !== undefined) this.storageMode = storageMode;
    if (queueSize !== undefined) this.queueSize = queueSize;

    const now = performance.now();
    const elapsedSample = (now - this.lastSampleTime) / 1000;

    if (elapsedSample >= 0.1) {
      const bytesDiff = (this.confirmedBytes || this.transportBytes) - this.lastSampleConfirmedBytes;
      if (bytesDiff >= 0) {
        this.effectiveSpeedMB = Math.max(0, (bytesDiff / elapsedSample) / (1024 * 1024));
        if (this.effectiveSpeedMB > this.peakSpeedMB) {
          this.peakSpeedMB = this.effectiveSpeedMB;
        }
      }
      this.lastSampleTime = now;
      this.lastSampleConfirmedBytes = this.confirmedBytes || this.transportBytes;
    }

    if (this.isPanelVisible) {
      this.renderPanel();
    }
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

  getRouteLabel() {
    return this.routeType;
  }

  getSnapshot() {
    const elapsedSec = (performance.now() - this.startTime) / 1000;
    const avgSpeedMB = elapsedSec > 0.1 ? (this.confirmedBytes / elapsedSec) / (1024 * 1024) : 0;

    return {
      role: this.role,
      iceState: this.iceState,
      peerState: this.peerState,
      dataChannelState: this.dataChannelState,
      routeType: this.routeType,
      activeCandidatePair: this.activeCandidatePair,
      rttMs: this.rttMs,
      totalBytes: this.totalBytes,
      queuedBytes: this.queuedBytes,
      transportBytes: this.transportBytes,
      confirmedBytes: this.confirmedBytes,
      effectiveSpeedMB: this.effectiveSpeedMB,
      avgSpeedMB,
      peakSpeedMB: this.peakSpeedMB,
      currentChunkSize: this.currentChunkSize,
      backpressureWaitTimeMs: this.backpressureWaitTimeMs,
      storageMode: this.storageMode,
      queueSize: this.queueSize,
      reconnectCount: this.reconnectCount
    };
  }

  // --- DEV DIAGNOSTICS OVERLAY PANEL ---
  setupKeyboardShortcut() {
    if (typeof window === 'undefined') return;
    window.addEventListener('keydown', (e) => {
      // Toggle with Ctrl+Shift+D or Cmd+Shift+D
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
      width: 360px;
      max-height: 480px;
      background: rgba(10, 14, 26, 0.94);
      border: 1px solid rgba(6, 182, 212, 0.35);
      border-radius: 14px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.6);
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
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 8px;">
        <div><strong>Role:</strong> ${s.role.toUpperCase()}</div>
        <div><strong>Route:</strong> <span style="color: ${s.routeType.includes('LAN') ? '#10B981' : s.routeType.includes('STUN') ? '#06B6D4' : '#F59E0B'}">${s.routeType.split(' ')[0]}</span></div>
        <div><strong>ICE State:</strong> ${s.iceState}</div>
        <div><strong>RTT:</strong> ${s.rttMs !== null ? s.rttMs + ' ms' : '--'}</div>
        <div><strong>Pair:</strong> ${s.activeCandidatePair ? `${s.activeCandidatePair.localType} ➔ ${s.activeCandidatePair.remoteType}` : '--'}</div>
        <div><strong>Storage:</strong> ${s.storageMode}</div>
      </div>
      <div style="border-top: 1px solid rgba(255,255,255,0.08); padding-top: 6px; margin-bottom: 6px;">
        <div><strong>Chunk Size:</strong> ${s.currentChunkSize / 1024} KB</div>
        <div><strong>Backpressure Wait:</strong> ${s.backpressureWaitTimeMs} ms</div>
        <div><strong>Speed:</strong> <span style="color: #06B6D4; font-weight: bold;">${s.effectiveSpeedMB.toFixed(2)} MB/s</span> (Peak: ${s.peakSpeedMB.toFixed(2)} MB/s)</div>
        <div><strong>Queued:</strong> ${formatBytes(s.queuedBytes)} / ${formatBytes(s.totalBytes)}</div>
        <div><strong>Confirmed:</strong> ${formatBytes(s.confirmedBytes)} (${s.totalBytes > 0 ? ((s.confirmedBytes / s.totalBytes) * 100).toFixed(1) : 0}%)</div>
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
