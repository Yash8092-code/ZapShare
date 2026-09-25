// ZapShare Application Controller
import { sound } from './sound.js';
import { ParticleEngine } from './particles.js';
import { qrEngine } from './qr-helper.js';
import { WebRTCManager } from './webrtc.js';

// Initialize Particle Engine
const particles = new ParticleEngine();

// Format bytes into human-readable strings
function formatBytes(bytes, decimals = 2) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// App State
let currentMode = 'send'; // 'send' | 'receive'
let currentView = 'send'; // 'send' | 'receive' | 'transfer' | 'complete'
let currentRole = 'sender';
let serverNetworkInfo = null;

// DOM Elements
const tabSend = document.getElementById('tab-send');
const tabReceive = document.getElementById('tab-receive');
const btnBrandHome = document.getElementById('btn-brand-home');
const btnToggleSound = document.getElementById('btn-toggle-sound');
const soundIconOn = document.getElementById('sound-icon-on');
const soundIconOff = document.getElementById('sound-icon-off');

const viewSend = document.getElementById('view-send');
const viewReceive = document.getElementById('view-receive');
const viewTransfer = document.getElementById('view-transfer');
const viewComplete = document.getElementById('view-complete');

// Send View Elements
const sendDropStage = document.getElementById('send-drop-stage');
const sendPairingStage = document.getElementById('send-pairing-stage');
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');

const sendFileName = document.getElementById('send-file-name');
const sendFileSize = document.getElementById('send-file-size');
const btnCancelFile = document.getElementById('btn-cancel-file');
const btnCancelSend = document.getElementById('btn-cancel-send');

const btnShowQr = document.getElementById('btn-show-qr');
const btnShowPin = document.getElementById('btn-show-pin');
const paneSendQr = document.getElementById('pane-send-qr');
const paneSendPin = document.getElementById('pane-send-pin');
const sendQrBox = document.getElementById('send-qr-box');
const shareLinkInput = document.getElementById('share-link-input');
const btnCopyLink = document.getElementById('btn-copy-link');
const copyLinkLabel = document.getElementById('copy-link-label');
const btnCopyPin = document.getElementById('btn-copy-pin');
const copyPinLabel = document.getElementById('copy-pin-label');

// Receive View Elements
const receiveInputStage = document.getElementById('receive-input-stage');
const receiveInspectStage = document.getElementById('receive-inspect-stage');
const btnRecvPinMode = document.getElementById('btn-recv-pin-mode');
const btnRecvQrMode = document.getElementById('btn-recv-qr-mode');
const paneRecvPin = document.getElementById('pane-recv-pin');
const paneRecvQr = document.getElementById('pane-recv-qr');

const otpInputs = [
  document.getElementById('otp-0'),
  document.getElementById('otp-1'),
  document.getElementById('otp-2'),
  document.getElementById('otp-3'),
  document.getElementById('otp-4'),
  document.getElementById('otp-5')
];
const btnPastePin = document.getElementById('btn-paste-pin');
const btnSubmitPin = document.getElementById('btn-submit-pin');
const recvErrorBanner = document.getElementById('recv-error-banner');
const recvErrorText = document.getElementById('recv-error-text');

const qrVideo = document.getElementById('qr-video');
const btnStopCamera = document.getElementById('btn-stop-camera');

const inspectFileName = document.getElementById('inspect-file-name');
const inspectFileSize = document.getElementById('inspect-file-size');
const inspectSenderDevice = document.getElementById('inspect-sender-device');
const btnAcceptDownload = document.getElementById('btn-accept-download');
const btnDeclineFile = document.getElementById('btn-decline-file');

// Transfer View Elements
const bridgeSenderName = document.getElementById('bridge-sender-name');
const bridgeReceiverName = document.getElementById('bridge-receiver-name');
const transferPercent = document.getElementById('transfer-percent');
const transferProgressBar = document.getElementById('transfer-progress-bar');
const metricSpeed = document.getElementById('metric-speed');
const metricEta = document.getElementById('metric-eta');
const metricTransferred = document.getElementById('metric-transferred');
const metricTotal = document.getElementById('metric-total');
const connQualityBadge = document.getElementById('conn-quality-badge');
const connQualityText = document.getElementById('conn-quality-text');
const btnAbortTransfer = document.getElementById('btn-abort-transfer');
const stallWarningBanner = document.getElementById('stall-warning-banner');
const stallWarningText = document.getElementById('stall-warning-text');

// Complete View Elements
const completeHeadline = document.getElementById('complete-headline');
const completeFileName = document.getElementById('complete-file-name');
const completeFileSize = document.getElementById('complete-file-size');
const btnSaveDownload = document.getElementById('btn-save-download');
const btnResetFlow = document.getElementById('btn-reset-flow');

// Fetch network URL info from server for phone pairing QR code
fetch('/api/info')
  .then(res => res.json())
  .then(data => {
    serverNetworkInfo = data;
    if (data && data.localIp && webrtc) {
      webrtc.setServerLocalIp(data.localIp);
    }
  })
  .catch(() => {
    serverNetworkInfo = { fullUrl: window.location.origin };
  });

// Setup WebRTC Manager
const webrtc = new WebRTCManager({
  onNetworkStatus: (status) => {
    const dot = document.querySelector('.status-dot');
    const label = document.getElementById('network-status-text');
    if (dot && label) {
      if (status.online) {
        dot.style.backgroundColor = '#10B981';
        label.textContent = 'P2P Ready';
      } else {
        dot.style.backgroundColor = '#F59E0B';
        label.textContent = 'Mesh Mode';
      }
    }
  },

  onPeerConnected: ({ role, info }) => {
    // Both peers paired!
    if (role === 'receiver') {
      bridgeReceiverName.textContent = info && info.device ? info.device : 'Receiver';
    }
    const beaconText = document.querySelector('.beacon-text');
    if (beaconText) {
      beaconText.textContent = 'Peer connected · Pre-warming P2P connection...';
    }
  },

  onPrewarmReady: () => {
    const beaconText = document.querySelector('.beacon-text');
    if (beaconText) {
      beaconText.textContent = 'Peer paired · Direct P2P link ready (0ms latency)';
    }
    const inspectBadge = document.querySelector('.inspect-badge span');
    if (inspectBadge) {
      inspectBadge.textContent = 'Verified Peer · Direct P2P Pre-Warmed (0ms)';
    }
  },

  onIncomingFile: (meta) => {
    // Show Inspection Card in Receive Mode
    if (meta) {
      inspectFileName.textContent = meta.name;
      inspectFileSize.textContent = formatBytes(meta.size);
      inspectSenderDevice.textContent = meta.device || 'Remote Device';

      receiveInputStage.classList.add('hidden');
      receiveInspectStage.classList.remove('hidden');
      switchView('receive');
    }
  },

  onTransferStart: ({ role, meta, connectionType }) => {
    currentRole = role;
    switchView('transfer');
    particles.startBridgeTransfer();

    if (meta) {
      if (role === 'sender') {
        bridgeSenderName.textContent = 'You (' + meta.device + ')';
        bridgeReceiverName.textContent = 'Receiver Device';
      } else {
        bridgeSenderName.textContent = meta.device || 'Sender';
        bridgeReceiverName.textContent = 'You (' + webrtc.getDeviceLabel() + ')';
      }
      metricTotal.textContent = formatBytes(meta.size);
    }

    if (connQualityText) {
      connQualityText.textContent = connectionType || 'Direct connection (LAN / P2P SCTP)';
    }

    transferPercent.textContent = '0';
    transferProgressBar.style.width = '0%';
    metricSpeed.textContent = '0.0';
    metricEta.textContent = '--';
    metricTransferred.textContent = '0 B';
  },

  onProgress: ({ percent, speedMB, etaSeconds, transferredBytes, totalBytes }) => {
    transferPercent.textContent = percent;
    transferProgressBar.style.width = `${percent}%`;
    metricSpeed.textContent = speedMB.toFixed(1);
    particles.setTransferSpeed(speedMB);

    if (etaSeconds <= 0) {
      metricEta.textContent = 'Finishing...';
    } else if (etaSeconds < 60) {
      metricEta.textContent = `${etaSeconds}s`;
    } else {
      const mins = Math.floor(etaSeconds / 60);
      const secs = etaSeconds % 60;
      metricEta.textContent = `${mins}m ${secs}s`;
    }

    metricTransferred.textContent = formatBytes(transferredBytes);
    metricTotal.textContent = formatBytes(totalBytes);
  },

  onConnectionQuality: (qualityText) => {
    if (connQualityText) {
      connQualityText.textContent = qualityText;
      const dot = document.querySelector('.quality-dot');
      if (dot) {
        if (qualityText.includes('checking')) {
          dot.className = 'quality-dot checking';
        } else if (qualityText.includes('failed') || qualityText.includes('lost')) {
          dot.className = 'quality-dot failed';
        } else if (qualityText.includes('Relayed') || qualityText.includes('TURN')) {
          dot.className = 'quality-dot relayed';
        } else {
          dot.className = 'quality-dot direct';
        }
      }
    }
  },

  onStall: (msg) => {
    if (stallWarningBanner && stallWarningText) {
      stallWarningText.textContent = msg;
      stallWarningBanner.classList.remove('hidden');
    }
  },

  onStallRecovered: () => {
    if (stallWarningBanner) {
      stallWarningBanner.classList.add('hidden');
    }
  },

  onComplete: ({ role, meta, blob, downloadUrl }) => {
    particles.stopBridgeTransfer();
    if (stallWarningBanner) stallWarningBanner.classList.add('hidden');
    switchView('complete');

    if (role === 'sender') {
      completeHeadline.textContent = 'Boom. File Delivered! ⚡';
      btnSaveDownload.classList.add('hidden');
    } else {
      completeHeadline.textContent = 'Boom. File Received! ⚡';
      if (downloadUrl && meta) {
        btnSaveDownload.href = downloadUrl;
        btnSaveDownload.download = meta.name;
        btnSaveDownload.classList.remove('hidden');

        // Automatically trigger download for zero-friction experience
        const autoDownload = document.createElement('a');
        autoDownload.href = downloadUrl;
        autoDownload.download = meta.name;
        document.body.appendChild(autoDownload);
        autoDownload.click();
        document.body.removeChild(autoDownload);
      }
    }

    if (meta) {
      completeFileName.textContent = meta.name;
      completeFileSize.textContent = formatBytes(meta.size);
    }
  },

  onError: (msg) => {
    showReceiveError(msg);
  },

  onDeclined: () => {
    alert('The receiver declined the file transfer.');
    resetSendFlow();
  },

  onCancelled: () => {
    alert('The transfer was cancelled.');
    resetAll();
  }
});

// View Switching Controller
function switchView(viewName) {
  currentView = viewName;
  [viewSend, viewReceive, viewTransfer, viewComplete].forEach(v => v.classList.add('hidden'));

  if (viewName === 'send') {
    viewSend.classList.remove('hidden');
    tabSend.classList.add('active');
    tabReceive.classList.remove('active');
  } else if (viewName === 'receive') {
    viewReceive.classList.remove('hidden');
    tabReceive.classList.add('active');
    tabSend.classList.remove('active');
  } else if (viewName === 'transfer') {
    viewTransfer.classList.remove('hidden');
  } else if (viewName === 'complete') {
    viewComplete.classList.remove('hidden');
  }
}

// Navigation Tabs
tabSend.addEventListener('click', () => {
  sound.playClick();
  currentMode = 'send';
  switchView('send');
});

tabReceive.addEventListener('click', () => {
  sound.playClick();
  currentMode = 'receive';
  switchView('receive');
  // Auto focus first OTP input
  setTimeout(() => otpInputs[0].focus(), 100);
});

btnBrandHome.addEventListener('click', () => {
  sound.playClick();
  resetAll();
});

// Audio Toggle Button
btnToggleSound.addEventListener('click', () => {
  const isMuted = sound.toggleMute();
  if (isMuted) {
    soundIconOn.classList.add('hidden');
    soundIconOff.classList.remove('hidden');
  } else {
    soundIconOn.classList.remove('hidden');
    soundIconOff.classList.add('hidden');
    sound.playClick();
  }
});

// ==========================================================================
// SCREEN 1: DROP ZONE & SEND LOGIC
// ==========================================================================

// Drag & Drop Interactions
['dragenter', 'dragover'].forEach(eventName => {
  dropZone.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropZone.classList.add('drag-active');
  });
});

['dragleave', 'drop'].forEach(eventName => {
  dropZone.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropZone.classList.remove('drag-active');
  });
});

dropZone.addEventListener('drop', (e) => {
  const files = e.dataTransfer.files;
  if (files && files.length > 0) {
    handleFileSelection(files[0]);
  }
});

dropZone.addEventListener('click', (e) => {
  // If clicked directly on the drop zone, trigger file picker
  if (e.target.tagName !== 'LABEL' && e.target.tagName !== 'INPUT') {
    fileInput.click();
  }
});

fileInput.addEventListener('change', () => {
  if (fileInput.files && fileInput.files.length > 0) {
    handleFileSelection(fileInput.files[0]);
  }
});

function handleFileSelection(file) {
  sound.playDrop();

  // Create room & register with WebRTC manager
  const { pin, fileMeta } = webrtc.createRoomForFile(file);

  // Update UI Elements
  sendFileName.textContent = fileMeta.name;
  sendFileSize.textContent = formatBytes(fileMeta.size);

  // Render 6-digit PIN boxes
  for (let i = 0; i < 6; i++) {
    const digitEl = document.getElementById(`pin-digit-${i}`);
    if (digitEl) digitEl.textContent = pin[i] || '-';
  }

  // Generate pairing URL: on cloud deployments (e.g. Vercel), use window.location.origin; on localhost use LAN IP from server
  const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
  const baseUrl = (!isLocalhost || !serverNetworkInfo?.fullUrl) ? window.location.origin : serverNetworkInfo.fullUrl;
  const pairingUrl = `${baseUrl}/?pin=${pin}`;
  shareLinkInput.value = pairingUrl;

  // Render QR Code
  qrEngine.renderQRCode(pairingUrl, sendQrBox, 200);

  // Switch stages
  sendDropStage.classList.add('hidden');
  sendPairingStage.classList.remove('hidden');
}

// Pairing tab switch (QR vs PIN)
btnShowQr.addEventListener('click', () => {
  sound.playClick();
  btnShowQr.classList.add('active');
  btnShowPin.classList.remove('active');
  paneSendQr.classList.remove('hidden');
  paneSendPin.classList.add('hidden');
});

btnShowPin.addEventListener('click', () => {
  sound.playClick();
  btnShowPin.classList.add('active');
  btnShowQr.classList.remove('active');
  paneSendPin.classList.remove('hidden');
  paneSendQr.classList.add('hidden');
});

// Copy link & Copy PIN
btnCopyLink.addEventListener('click', () => {
  sound.playClick();
  navigator.clipboard.writeText(shareLinkInput.value);
  copyLinkLabel.textContent = 'Copied!';
  setTimeout(() => { copyLinkLabel.textContent = 'Copy Link'; }, 2000);
});

btnCopyPin.addEventListener('click', () => {
  sound.playClick();
  if (webrtc.pin) {
    navigator.clipboard.writeText(webrtc.pin);
    copyPinLabel.textContent = 'Copied!';
    setTimeout(() => { copyPinLabel.textContent = 'Copy 6-Digit Code'; }, 2000);
  }
});

btnCancelFile.addEventListener('click', () => {
  sound.playClick();
  resetSendFlow();
});

btnCancelSend.addEventListener('click', () => {
  sound.playClick();
  resetSendFlow();
});

function resetSendFlow() {
  webrtc.cleanupTransfer();
  fileInput.value = '';
  sendDropStage.classList.remove('hidden');
  sendPairingStage.classList.add('hidden');
}

// ==========================================================================
// SCREEN 2: RECEIVE & INSPECT LOGIC
// ==========================================================================

// Toggle between PIN mode & QR Scanner mode
btnRecvPinMode.addEventListener('click', () => {
  sound.playClick();
  btnRecvPinMode.classList.add('active');
  btnRecvQrMode.classList.remove('active');
  paneRecvPin.classList.remove('hidden');
  paneRecvQr.classList.add('hidden');
  qrEngine.stopCamera();
});

btnRecvQrMode.addEventListener('click', () => {
  sound.playClick();
  btnRecvQrMode.classList.add('active');
  btnRecvPinMode.classList.remove('active');
  paneRecvQr.classList.remove('hidden');
  paneRecvPin.classList.add('hidden');

  // Start Camera
  qrEngine.startCamera(qrVideo, (scannedText) => {
    sound.playSuccess();
    // Parse scanned link or PIN
    let pin = scannedText;
    if (scannedText.includes('pin=')) {
      const match = scannedText.match(/pin=([0-9]{6})/);
      if (match) pin = match[1];
    }
    pin = pin.replace(/\D/g, '');
    if (pin.length === 6) {
      fillOtpInputs(pin);
      webrtc.joinRoomWithPin(pin);
    }
  }, (err) => {
    showReceiveError(err);
  });
});

btnStopCamera.addEventListener('click', () => {
  sound.playClick();
  btnRecvPinMode.click();
});

// OTP Input Handling (Auto-advance & Backspace)
otpInputs.forEach((input, idx) => {
  input.addEventListener('input', (e) => {
    const val = e.target.value.replace(/\D/g, '');
    input.value = val ? val[0] : '';

    if (val && idx < 5) {
      otpInputs[idx + 1].focus();
    }

    checkOtpComplete();
  });

  input.addEventListener('keydown', (e) => {
    if (e.key === 'Backspace' && !input.value && idx > 0) {
      otpInputs[idx - 1].focus();
    }
  });

  input.addEventListener('paste', (e) => {
    e.preventDefault();
    const pasted = (e.clipboardData || window.clipboardData).getData('text');
    const digits = pasted.replace(/\D/g, '').slice(0, 6);
    fillOtpInputs(digits);
  });
});

function fillOtpInputs(pinStr) {
  for (let i = 0; i < 6; i++) {
    otpInputs[i].value = pinStr[i] || '';
  }
  if (pinStr.length === 6) {
    otpInputs[5].focus();
    checkOtpComplete();
  } else if (pinStr.length > 0) {
    otpInputs[Math.min(pinStr.length, 5)].focus();
  }
}

function checkOtpComplete() {
  const pin = otpInputs.map(i => i.value).join('');
  if (pin.length === 6) {
    recvErrorBanner.classList.add('hidden');
  }
}

// Paste PIN from Clipboard Button
btnPastePin.addEventListener('click', async () => {
  sound.playClick();
  try {
    const text = await navigator.clipboard.readText();
    const digits = text.replace(/\D/g, '').slice(0, 6);
    if (digits) {
      fillOtpInputs(digits);
    }
  } catch (err) {
    showReceiveError('Please allow clipboard permission or enter manually.');
  }
});

// Submit PIN Button
btnSubmitPin.addEventListener('click', () => {
  const pin = otpInputs.map(i => i.value).join('');
  if (pin.length !== 6) {
    showReceiveError('Please enter all 6 digits of the pairing code.');
    return;
  }
  sound.playClick();
  recvErrorBanner.classList.add('hidden');
  webrtc.joinRoomWithPin(pin);
});

function showReceiveError(msg) {
  recvErrorText.textContent = msg || 'Could not connect. Please verify the 6-digit code.';
  recvErrorBanner.classList.remove('hidden');
  sound.playDecline();
}

// Accept & Download Incoming File
btnAcceptDownload.addEventListener('click', () => {
  sound.playClick();
  webrtc.acceptIncomingFile();
});

// Decline Incoming File
btnDeclineFile.addEventListener('click', () => {
  sound.playClick();
  webrtc.declineIncomingFile();
  resetReceiveFlow();
});

function resetReceiveFlow() {
  receiveInspectStage.classList.add('hidden');
  receiveInputStage.classList.remove('hidden');
  otpInputs.forEach(i => i.value = '');
  qrEngine.stopCamera();
}

// ==========================================================================
// SCREEN 3: TRANSFER IN PROGRESS
// ==========================================================================

btnAbortTransfer.addEventListener('click', () => {
  sound.playClick();
  webrtc.cancelTransfer();
  particles.stopBridgeTransfer();
  resetAll();
});

// ==========================================================================
// SCREEN 4: COMPLETE & RESET
// ==========================================================================

btnResetFlow.addEventListener('click', () => {
  sound.playClick();
  resetAll();
});

function resetAll() {
  particles.stopBridgeTransfer();
  if (stallWarningBanner) stallWarningBanner.classList.add('hidden');
  webrtc.cleanupTransfer();
  resetSendFlow();
  resetReceiveFlow();
  switchView('send');
}

// Auto-check URL query parameters for pre-filled PIN (e.g. ?pin=849201)
const urlParams = new URLSearchParams(window.location.search);
const queryPin = urlParams.get('pin');
if (queryPin && queryPin.length === 6) {
  switchView('receive');
  fillOtpInputs(queryPin);
  setTimeout(() => {
    webrtc.joinRoomWithPin(queryPin);
  }, 500);
}
