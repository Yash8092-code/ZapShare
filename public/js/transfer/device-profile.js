// ZapShare Device Profile & Transfer Mode Engine (Phases 7, 8, 21, 22)
// Tailored for mobile efficiency (e.g. Realme 6, Android, iOS) vs. Desktop performance
import { DeviceClass, TransferMode } from './constants.js';

export class DeviceProfileManager {
  constructor() {
    this.deviceClass = this.detectDeviceClass();
    this.deviceDetails = this.inspectDeviceDetails();
    this.transferMode = this.loadTransferMode();
  }

  detectDeviceClass() {
    if (typeof window === 'undefined' || typeof navigator === 'undefined') {
      return DeviceClass.DESKTOP;
    }

    const ua = navigator.userAgent || '';
    const isMobileUA = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini|Mobile|realme/i.test(ua);
    const hasTouch = (navigator.maxTouchPoints && navigator.maxTouchPoints > 1);
    const isSmallScreen = typeof window.screen !== 'undefined' && Math.min(window.screen.width, window.screen.height) <= 800;

    if (isMobileUA || (hasTouch && isSmallScreen)) {
      return DeviceClass.MOBILE;
    }
    return DeviceClass.DESKTOP;
  }

  inspectDeviceDetails() {
    if (typeof navigator === 'undefined') return { cores: 4, memory: 4, brand: 'Standard' };
    const ua = navigator.userAgent || '';
    let brand = 'Desktop PC';
    if (/realme/i.test(ua)) brand = 'Realme Mobile';
    else if (/Android/i.test(ua)) brand = 'Android Mobile';
    else if (/iPhone/i.test(ua)) brand = 'Apple iPhone';
    else if (/iPad/i.test(ua)) brand = 'Apple iPad';
    else if (/Macintosh/i.test(ua)) brand = 'macOS';
    else if (/Windows/i.test(ua)) brand = 'Windows PC';
    else if (/Linux/i.test(ua)) brand = 'Linux PC';

    return {
      cores: navigator.hardwareConcurrency || 4,
      memoryGB: navigator.deviceMemory || 4,
      brand,
      isMobile: this.deviceClass === DeviceClass.MOBILE
    };
  }

  loadTransferMode() {
    try {
      if (typeof localStorage !== 'undefined') {
        const saved = localStorage.getItem('zapshare_transfer_mode');
        if (saved && Object.values(TransferMode).includes(saved)) {
          return saved;
        }
      }
    } catch (e) {}
    return TransferMode.BALANCED; // Default mode across all platforms
  }

  setTransferMode(mode) {
    if (Object.values(TransferMode).includes(mode)) {
      this.transferMode = mode;
      try {
        if (typeof localStorage !== 'undefined') {
          localStorage.setItem('zapshare_transfer_mode', mode);
        }
      } catch (e) {}
      console.log(`[DeviceProfile] Transfer mode set to: ${mode}`);
    }
  }

  getTuning(sctpMaxMessageSize = null) {
    const isMobile = this.deviceClass === DeviceClass.MOBILE;

    // Base defaults by device class
    let initialChunk = isMobile ? 48 * 1024 : 64 * 1024;
    let minChunk = 32 * 1024;
    let maxChunk = isMobile ? 96 * 1024 : 128 * 1024;
    let highWater = isMobile ? 1 * 1024 * 1024 : 2 * 1024 * 1024;
    let lowWater = isMobile ? 256 * 1024 : 512 * 1024;
    let maxInMemoryQueue = isMobile ? 8 * 1024 * 1024 : 16 * 1024 * 1024;
    let uiThrottleMs = isMobile ? 750 : 500;
    let enableHeavyVisuals = !isMobile;

    // Mode Overrides
    if (this.transferMode === TransferMode.DEVICE_FRIENDLY) {
      initialChunk = 32 * 1024;
      maxChunk = 64 * 1024;
      highWater = 768 * 1024;
      lowWater = 192 * 1024;
      maxInMemoryQueue = 4 * 1024 * 1024;
      uiThrottleMs = 1000;
      enableHeavyVisuals = false; // Reduce canvas particle / thermal work
    } else if (this.transferMode === TransferMode.MAXIMUM_SPEED) {
      initialChunk = 64 * 1024;
      maxChunk = 128 * 1024;
      highWater = 2.5 * 1024 * 1024;
      lowWater = 512 * 1024;
      maxInMemoryQueue = 24 * 1024 * 1024;
      uiThrottleMs = 400;
      enableHeavyVisuals = true;
    }

    // Never exceed SCTP max negotiated message size
    if (sctpMaxMessageSize && sctpMaxMessageSize > 0) {
      maxChunk = Math.min(maxChunk, sctpMaxMessageSize);
      if (initialChunk > maxChunk) initialChunk = maxChunk;
    }

    return {
      deviceClass: this.deviceClass,
      transferMode: this.transferMode,
      brand: this.deviceDetails.brand,
      initialChunk,
      minChunk,
      maxChunk,
      highWater,
      lowWater,
      maxInMemoryQueue,
      uiThrottleMs,
      enableHeavyVisuals
    };
  }
}
