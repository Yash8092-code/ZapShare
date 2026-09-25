// ZapShare QR Engine: Robust ISO 18004 QR Generator & Multi-Engine Camera Scanner
// Powered by local qrcode-generator and jsQR (Zero external network dependencies)

export class QREngine {
  constructor() {
    this.videoStream = null;
    this.scanRafId = null;
    this.scanIntervalId = null;
    this.isScanning = false;
    this.barcodeDetector = null;

    if (typeof window !== 'undefined' && 'BarcodeDetector' in window) {
      try {
        this.barcodeDetector = new window.BarcodeDetector({ formats: ['qr_code'] });
      } catch (e) {
        console.info('Native BarcodeDetector not active; using fast pure-JS jsQR scanner.');
      }
    }
  }

  // Generate crisp, high-contrast ISO 18004 SVG QR Code
  // Designed for instant detection on iOS Camera, Google Lens, Samsung Camera, & in-app scanner
  renderQRCode(text, containerElement, size = 200) {
    if (!containerElement || !text) return;

    if (typeof window.qrcode !== 'function') {
      console.error('QR Generator library (qrcode.js) not loaded.');
      containerElement.innerHTML = `<div style="color:#F87171;font-size:0.85rem;padding:20px;text-align:center;">QR library loading...</div>`;
      return;
    }

    try {
      // TypeNumber 0 = auto-calculate based on text length; 'M' error correction gives ~15% redundancy
      const qr = window.qrcode(0, 'M');
      qr.addData(text);
      qr.make();

      const moduleCount = qr.getModuleCount();
      const margin = 2; // ISO standard quiet zone
      const totalModules = moduleCount + margin * 2;
      const cellSize = size / totalModules;

      let svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" class="zap-qr-svg" shape-rendering="crispEdges">`;
      
      // High-contrast clean white quiet zone backing for instant mobile camera thresholding
      svg += `<rect width="${size}" height="${size}" rx="14" fill="#FFFFFF" />`;

      // Render dark modules
      for (let r = 0; r < moduleCount; r++) {
        for (let c = 0; c < moduleCount; c++) {
          if (qr.isDark(r, c)) {
            const x = (c + margin) * cellSize;
            const y = (r + margin) * cellSize;
            // Finder pattern vs data cell
            const isFinder = (r < 7 && c < 7) || (r < 7 && c >= moduleCount - 7) || (r >= moduleCount - 7 && c < 7);
            const fill = isFinder ? '#0a0d16' : '#111827';
            svg += `<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${(cellSize + 0.05).toFixed(2)}" height="${(cellSize + 0.05).toFixed(2)}" fill="${fill}" />`;
          }
        }
      }

      svg += `</svg>`;
      containerElement.innerHTML = svg;
    } catch (err) {
      console.error('Failed to render QR Code:', err);
      containerElement.innerHTML = `<div style="color:#F87171;font-size:0.85rem;padding:20px;text-align:center;">Failed to generate QR code</div>`;
    }
  }

  // Multi-tier camera stream acquisition with graceful degradation:
  // Tier 1: Rear camera with ideal HD resolution (mobile default)
  // Tier 2: Rear camera with loose constraints
  // Tier 3: Front camera ('user' facingMode, laptop/webcam default)
  // Tier 4: Any available video device (desktop fallback without facingMode)
  async getCameraStream() {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      const err = new Error('Camera API (getUserMedia) not supported in this browser environment. Ensure HTTPS is active.');
      err.name = 'NotSupportedError';
      throw err;
    }

    // Tier 1: Rear camera HD
    try {
      console.log('[QR Camera] Attempting Tier 1: Rear camera HD (facingMode: environment)...');
      return await navigator.mediaDevices.getUserMedia({
        video: {
          facingMode: { ideal: 'environment' },
          width: { ideal: 1280 },
          height: { ideal: 720 }
        },
        audio: false
      });
    } catch (e1) {
      console.warn('[QR Camera] Tier 1 failed:', e1.name, e1.message);
      // If user denied permission explicitly, abort immediately to respect user choice
      if (e1.name === 'NotAllowedError' || e1.name === 'PermissionDeniedError') {
        throw e1;
      }
    }

    // Tier 2: Rear camera loose constraints
    try {
      console.log('[QR Camera] Attempting Tier 2: Rear camera basic constraints...');
      return await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: 'environment' } },
        audio: false
      });
    } catch (e2) {
      console.warn('[QR Camera] Tier 2 failed:', e2.name, e2.message);
      if (e2.name === 'NotAllowedError' || e2.name === 'PermissionDeniedError') {
        throw e2;
      }
    }

    // Tier 3: Front camera / User facing
    try {
      console.log('[QR Camera] Attempting Tier 3: Front/User camera...');
      return await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'user' },
        audio: false
      });
    } catch (e3) {
      console.warn('[QR Camera] Tier 3 failed:', e3.name, e3.message);
      if (e3.name === 'NotAllowedError' || e3.name === 'PermissionDeniedError') {
        throw e3;
      }
    }

    // Tier 4: Absolute fallback - any video device (desktop PCs, external webcams, virtual cameras)
    console.log('[QR Camera] Attempting Tier 4: Any available video capture device (desktop fallback)...');
    return await navigator.mediaDevices.getUserMedia({
      video: true,
      audio: false
    });
  }

  // Camera QR Scanner with dual fallback: Native BarcodeDetector -> jsQR
  async startCamera(videoElement, onScanCallback, onErrorCallback) {
    this.stopCamera();

    try {
      this.videoStream = await this.getCameraStream();

      videoElement.setAttribute('playsinline', 'true');
      videoElement.setAttribute('autoplay', 'true');
      videoElement.muted = true;
      videoElement.srcObject = this.videoStream;
      await videoElement.play();

      console.log('[QR Camera] Camera stream active and playing.');
      this.isScanning = true;

      // Offscreen canvas for frame analysis
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d', { willReadFrequently: true });

      let lastScanTime = 0;
      const scanThrottleMs = 120; // ~8 scans/sec for responsive detection with minimal CPU load

      const scanLoop = async (now) => {
        if (!this.isScanning || !this.videoStream) return;

        if (videoElement.readyState === videoElement.HAVE_ENOUGH_DATA) {
          if (now - lastScanTime >= scanThrottleMs) {
            lastScanTime = now;

            // Downscale to max 640px wide for speed
            const scale = Math.min(1, 640 / (videoElement.videoWidth || 640));
            const targetW = Math.max(160, Math.floor(videoElement.videoWidth * scale));
            const targetH = Math.max(120, Math.floor(videoElement.videoHeight * scale));

            if (canvas.width !== targetW || canvas.height !== targetH) {
              canvas.width = targetW;
              canvas.height = targetH;
            }

            ctx.drawImage(videoElement, 0, 0, targetW, targetH);
            const imageData = ctx.getImageData(0, 0, targetW, targetH);

            let detectedData = null;

            // 1. Try native BarcodeDetector if available
            if (this.barcodeDetector) {
              try {
                const barcodes = await this.barcodeDetector.detect(canvas);
                if (barcodes && barcodes.length > 0 && barcodes[0].rawValue) {
                  detectedData = barcodes[0].rawValue;
                }
              } catch (e) {
                // Ignore and fallback to jsQR
              }
            }

            // 2. Pure JS jsQR engine (works across all browsers including iOS Safari, Firefox, Chrome)
            if (!detectedData && typeof window.jsQR === 'function') {
              try {
                const code = window.jsQR(imageData.data, imageData.width, imageData.height, {
                  inversionAttempts: 'attemptBoth'
                });
                if (code && code.data) {
                  detectedData = code.data;
                }
              } catch (e) {
                // Processing error on corrupt frame, continue loop
              }
            }

            if (detectedData) {
              this.stopCamera();
              if (onScanCallback) onScanCallback(detectedData);
              return;
            }
          }
        }

        if (this.isScanning) {
          this.scanRafId = requestAnimationFrame(scanLoop);
        }
      };

      this.scanRafId = requestAnimationFrame(scanLoop);

    } catch (err) {
      console.error('[QR Camera] Camera acquisition failed:', err.name, err.message, err);
      this.isScanning = false;
      this.stopCamera();

      // Formulate detailed, user-actionable error info
      let userTitle = 'Camera Unavailable';
      let userAdvice = 'Could not access the camera on this device.';

      switch (err.name) {
        case 'NotAllowedError':
        case 'PermissionDeniedError':
          userTitle = 'Camera Permission Blocked';
          userAdvice = 'Camera access was blocked by the browser. Click the site settings/lock icon in your address bar to allow camera access.';
          break;
        case 'NotFoundError':
        case 'DevicesNotFoundError':
          userTitle = 'No Camera Detected';
          userAdvice = 'No camera hardware found on this device (e.g. desktop PC without a webcam). Please use 6-digit PIN code pairing.';
          break;
        case 'NotReadableError':
        case 'TrackStartError':
          userTitle = 'Camera In Use / Hardware Busy';
          userAdvice = 'The camera is being used by another application (Zoom, Teams, or another tab). Close other apps or use PIN code.';
          break;
        case 'OverconstrainedError':
          userTitle = 'Camera Format Unsupported';
          userAdvice = 'Your camera does not meet requested video constraints. Please use 6-digit PIN pairing.';
          break;
        case 'SecurityError':
          userTitle = 'Insecure Context / Security Policy';
          userAdvice = 'Browser security policy prevented camera access. Please use HTTPS.';
          break;
        case 'NotSupportedError':
          userTitle = 'Camera API Not Supported';
          userAdvice = 'getUserMedia is not supported by your current browser environment.';
          break;
        default:
          userTitle = 'Camera Error (' + (err.name || 'Unknown') + ')';
          userAdvice = err.message || 'An unexpected error occurred while starting the camera.';
          break;
      }

      const errorPayload = {
        name: err.name || 'Error',
        message: userAdvice,
        title: userTitle,
        rawMessage: err.message || ''
      };

      if (onErrorCallback) onErrorCallback(errorPayload);
    }
  }

  stopCamera() {
    this.isScanning = false;
    if (this.scanRafId) {
      cancelAnimationFrame(this.scanRafId);
      this.scanRafId = null;
    }
    if (this.videoStream) {
      try {
        this.videoStream.getTracks().forEach(track => track.stop());
      } catch (e) {}
      this.videoStream = null;
    }
  }
}

export const qrEngine = new QREngine();
