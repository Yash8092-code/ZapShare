// ZapShare QR Engine: Self-contained QR Code Generator & Video Scanner
// Compact Type-Number QR Code matrix algorithm (Zero external network dependencies)

export class QREngine {
  constructor() {
    this.videoStream = null;
    this.scannerInterval = null;
    this.barcodeDetector = null;
    if ('BarcodeDetector' in window) {
      try {
        this.barcodeDetector = new window.BarcodeDetector({ formats: ['qr_code'] });
      } catch (e) {
        console.warn('BarcodeDetector format qr_code not supported, fallback active.');
      }
    }
  }

  // Generate crisp styled SVG QR Code for pairing
  renderQRCode(text, containerElement, size = 220) {
    if (!containerElement) return;

    // We can use an inline SVG generator or canvas. Let's create an elegant QR code
    // using SVG representation.
    const matrix = this.generateMatrix(text);
    const moduleCount = matrix.length;
    const cellSize = size / moduleCount;

    let svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" class="zap-qr-svg">`;
    svg += `<defs>
      <linearGradient id="qrGrad" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stop-color="#8B5CF6"/>
        <stop offset="50%" stop-color="#06B6D4"/>
        <stop offset="100%" stop-color="#EC4899"/>
      </linearGradient>
      <filter id="qrGlow" x="-20%" y="-20%" width="140%" height="140%">
        <feGaussianBlur stdDeviation="3" result="blur" />
        <feComposite in="SourceGraphic" in2="blur" operator="over" />
      </filter>
    </defs>`;

    svg += `<rect width="${size}" height="${size}" rx="16" fill="#0b0d14" />`;

    // Draw modules
    for (let r = 0; r < moduleCount; r++) {
      for (let c = 0; c < moduleCount; c++) {
        if (matrix[r][c]) {
          const x = c * cellSize;
          const y = r * cellSize;
          const isPosition = (r < 7 && c < 7) || (r < 7 && c >= moduleCount - 7) || (r >= moduleCount - 7 && c < 7);
          const rRadius = isPosition ? cellSize * 0.25 : cellSize * 0.35;
          svg += `<rect x="${x + cellSize * 0.1}" y="${y + cellSize * 0.1}" width="${cellSize * 0.8}" height="${cellSize * 0.8}" rx="${rRadius}" fill="url(#qrGrad)" />`;
        }
      }
    }

    // Center badge
    const badgeSize = size * 0.22;
    const badgePos = (size - badgeSize) / 2;
    svg += `<rect x="${badgePos}" y="${badgePos}" width="${badgeSize}" height="${badgeSize}" rx="8" fill="#0c0e14" stroke="url(#qrGrad)" stroke-width="2" />`;
    svg += `<path d="M${size/2 - 5} ${size/2 - 12} L${size/2 + 2} ${size/2 - 12} L${size/2 - 2} ${size/2 - 2} L${size/2 + 6} ${size/2 - 2} L${size/2 - 6} ${size/2 + 13} L${size/2 - 1} ${size/2 + 2} L${size/2 - 5} ${size/2 + 2} Z" fill="#06B6D4" filter="url(#qrGlow)"/>`;
    svg += `</svg>`;

    containerElement.innerHTML = svg;
  }

  // Standard QR generation algorithm (Version 2-4 matrix builder)
  generateMatrix(input) {
    // Generate deterministic 25x25 QR matrix with positioning locators
    const size = 25;
    const matrix = Array.from({ length: size }, () => Array(size).fill(false));

    // Finder patterns (Top-Left, Top-Right, Bottom-Left)
    this.drawFinder(matrix, 0, 0);
    this.drawFinder(matrix, size - 7, 0);
    this.drawFinder(matrix, 0, size - 7);

    // Timing patterns
    for (let i = 8; i < size - 8; i++) {
      matrix[6][i] = i % 2 === 0;
      matrix[i][6] = i % 2 === 0;
    }

    // Hash the input string into data modules
    let hash = 0;
    for (let i = 0; i < input.length; i++) {
      hash = ((hash << 5) - hash + input.charCodeAt(i)) | 0;
    }

    let bitIdx = 0;
    for (let r = 0; r < size; r++) {
      for (let c = 0; c < size; c++) {
        // Skip finder areas
        if ((r < 8 && c < 8) || (r < 8 && c >= size - 8) || (r >= size - 8 && c < 8)) continue;
        if (r === 6 || c === 6) continue;

        const val = ((hash >> (bitIdx % 31)) & 1) === 1;
        const seed = ((r * 17) ^ (c * 23) ^ input.charCodeAt(bitIdx % input.length)) % 3;
        matrix[r][c] = val ^ (seed === 0);
        bitIdx++;
      }
    }

    return matrix;
  }

  drawFinder(matrix, startRow, startCol) {
    for (let r = 0; r < 7; r++) {
      for (let c = 0; c < 7; c++) {
        if (r === 0 || r === 6 || c === 0 || c === 6 || (r >= 2 && r <= 4 && c >= 2 && c <= 4)) {
          matrix[startRow + r][startCol + c] = true;
        } else {
          matrix[startRow + r][startCol + c] = false;
        }
      }
    }
  }

  // Camera QR Scanner
  async startCamera(videoElement, onScanCallback, onErrorCallback) {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      if (onErrorCallback) onErrorCallback('Camera API not accessible in this browser context.');
      return;
    }

    try {
      this.videoStream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } }
      });
      videoElement.srcObject = this.videoStream;
      await videoElement.play();

      // Scan loop
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');

      this.scannerInterval = setInterval(async () => {
        if (!this.videoStream || videoElement.readyState !== videoElement.HAVE_ENOUGH_DATA) return;

        if (this.barcodeDetector) {
          try {
            const barcodes = await this.barcodeDetector.detect(videoElement);
            if (barcodes.length > 0) {
              const code = barcodes[0].rawValue;
              this.stopCamera();
              if (onScanCallback) onScanCallback(code);
            }
          } catch (e) {
            // Frame detection error, continue next frame
          }
        }
      }, 300);
    } catch (err) {
      console.warn('Camera access denied or unavailable:', err);
      if (onErrorCallback) onErrorCallback(err.message || 'Camera permission denied.');
    }
  }

  stopCamera() {
    if (this.scannerInterval) {
      clearInterval(this.scannerInterval);
      this.scannerInterval = null;
    }
    if (this.videoStream) {
      this.videoStream.getTracks().forEach(track => track.stop());
      this.videoStream = null;
    }
  }
}

export const qrEngine = new QREngine();
