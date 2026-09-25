// ZapShare Particle Engine: Cosmic Stardust & Real-time WebRTC Photon Stream
export class ParticleEngine {
  constructor() {
    this.bgCanvas = document.getElementById('bg-canvas');
    this.bgCtx = this.bgCanvas ? this.bgCanvas.getContext('2d') : null;
    this.bgParticles = [];
    this.mouse = { x: -1000, y: -1000 };

    this.bridgeCanvas = document.getElementById('bridge-canvas');
    this.bridgeCtx = this.bridgeCanvas ? this.bridgeCanvas.getContext('2d') : null;
    this.bridgeParticles = [];
    this.isTransferring = false;
    this.transferSpeedMB = 0;
    this.pulsePhase = 0;

    this.initBg();
    this.setupListeners();
    this.animate = this.animate.bind(this);
    requestAnimationFrame(this.animate);
  }

  initBg() {
    if (!this.bgCanvas) return;
    this.resizeBg();
    const count = Math.min(Math.floor((window.innerWidth * window.innerHeight) / 16000), 75);
    this.bgParticles = [];
    for (let i = 0; i < count; i++) {
      this.bgParticles.push({
        x: Math.random() * this.bgCanvas.width,
        y: Math.random() * this.bgCanvas.height,
        vx: (Math.random() - 0.5) * 0.35,
        vy: (Math.random() - 0.5) * 0.35,
        size: Math.random() * 1.8 + 0.6,
        alpha: Math.random() * 0.5 + 0.2,
        color: Math.random() > 0.6 ? '#8B5CF6' : (Math.random() > 0.5 ? '#06B6D4' : '#ffffff')
      });
    }
  }

  resizeBg() {
    if (!this.bgCanvas) return;
    this.bgCanvas.width = window.innerWidth;
    this.bgCanvas.height = window.innerHeight;
  }

  resizeBridge() {
    if (!this.bridgeCanvas) return;
    const rect = this.bridgeCanvas.parentElement.getBoundingClientRect();
    this.bridgeCanvas.width = rect.width;
    this.bridgeCanvas.height = Math.max(rect.height, 120);
  }

  setupListeners() {
    window.addEventListener('resize', () => {
      this.resizeBg();
      this.resizeBridge();
    });

    window.addEventListener('mousemove', (e) => {
      this.mouse.x = e.clientX;
      this.mouse.y = e.clientY;
    });

    window.addEventListener('mouseleave', () => {
      this.mouse.x = -1000;
      this.mouse.y = -1000;
    });
  }

  startBridgeTransfer() {
    this.isTransferring = true;
    this.bridgeParticles = [];
    this.resizeBridge();
  }

  stopBridgeTransfer() {
    this.isTransferring = false;
    this.bridgeParticles = [];
  }

  setTransferSpeed(speedMBps) {
    this.transferSpeedMB = Math.max(0.1, speedMBps);
  }

  spawnBridgeParticle(w, h) {
    // Left node center to right node center
    const startX = 60;
    const startY = h / 2;
    const endX = w - 60;
    const endY = h / 2;

    const speedFactor = Math.min(Math.max(this.transferSpeedMB / 20, 1.2), 6);
    this.bridgeParticles.push({
      progress: 0,
      speed: (0.008 + Math.random() * 0.012) * speedFactor,
      offsetY: (Math.random() - 0.5) * 24,
      size: Math.random() * 3 + 2,
      color: Math.random() > 0.4 ? '#06B6D4' : '#8B5CF6'
    });
  }

  animate() {
    // Render Background Stardust
    if (this.bgCtx && this.bgCanvas) {
      this.bgCtx.clearRect(0, 0, this.bgCanvas.width, this.bgCanvas.height);
      const w = this.bgCanvas.width;
      const h = this.bgCanvas.height;

      for (let p of this.bgParticles) {
        p.x += p.vx;
        p.y += p.vy;

        if (p.x < 0) p.x = w;
        if (p.x > w) p.x = 0;
        if (p.y < 0) p.y = h;
        if (p.y > h) p.y = 0;

        // Gentle mouse interaction
        const dx = this.mouse.x - p.x;
        const dy = this.mouse.y - p.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 140) {
          const force = (140 - dist) / 140 * 0.4;
          p.x -= (dx / dist) * force;
          p.y -= (dy / dist) * force;
        }

        this.bgCtx.beginPath();
        this.bgCtx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
        this.bgCtx.fillStyle = p.color;
        this.bgCtx.globalAlpha = p.alpha;
        this.bgCtx.shadowBlur = 8;
        this.bgCtx.shadowColor = p.color;
        this.bgCtx.fill();
        this.bgCtx.shadowBlur = 0;
      }
      this.bgCtx.globalAlpha = 1.0;
    }

    // Render Bridge Particle Stream
    if (this.bridgeCtx && this.bridgeCanvas && this.isTransferring) {
      const w = this.bridgeCanvas.width;
      const h = this.bridgeCanvas.height;
      this.bridgeCtx.clearRect(0, 0, w, h);

      const startX = 60;
      const startY = h / 2;
      const endX = w - 60;
      const endY = h / 2;

      // Draw the central conduit glow line
      this.bridgeCtx.beginPath();
      this.bridgeCtx.moveTo(startX, startY);
      this.bridgeCtx.bezierCurveTo(w * 0.35, startY - 18, w * 0.65, endY + 18, endX, endY);
      this.bridgeCtx.strokeStyle = 'rgba(139, 92, 246, 0.2)';
      this.bridgeCtx.lineWidth = 3;
      this.bridgeCtx.stroke();

      // Core electric line
      this.bridgeCtx.beginPath();
      this.bridgeCtx.moveTo(startX, startY);
      this.bridgeCtx.bezierCurveTo(w * 0.35, startY - 18, w * 0.65, endY + 18, endX, endY);
      this.bridgeCtx.strokeStyle = 'rgba(6, 182, 212, 0.4)';
      this.bridgeCtx.lineWidth = 1;
      this.bridgeCtx.stroke();

      // Spawn rate based on transfer speed
      const spawnChance = Math.min(0.3 + (this.transferSpeedMB / 50) * 0.5, 0.95);
      if (Math.random() < spawnChance) {
        this.spawnBridgeParticle(w, h);
      }

      // Update & render beam particles
      for (let i = this.bridgeParticles.length - 1; i >= 0; i--) {
        const p = this.bridgeParticles[i];
        p.progress += p.speed;

        if (p.progress >= 1) {
          this.bridgeParticles.splice(i, 1);
          continue;
        }

        // Compute Bezier point
        const t = p.progress;
        const cp1x = w * 0.35, cp1y = startY - 18;
        const cp2x = w * 0.65, cp2y = endY + 18;

        const x = Math.pow(1 - t, 3) * startX +
                  3 * Math.pow(1 - t, 2) * t * cp1x +
                  3 * (1 - t) * Math.pow(t, 2) * cp2x +
                  Math.pow(t, 3) * endX;

        const baseY = Math.pow(1 - t, 3) * startY +
                      3 * Math.pow(1 - t, 2) * t * cp1y +
                      3 * (1 - t) * Math.pow(t, 2) * cp2y +
                      Math.pow(t, 3) * endY;

        const y = baseY + Math.sin(t * Math.PI) * p.offsetY;

        this.bridgeCtx.beginPath();
        this.bridgeCtx.arc(x, y, p.size, 0, Math.PI * 2);
        this.bridgeCtx.fillStyle = p.color;
        this.bridgeCtx.fill();
      }

      // Receiver node pulse glow
      this.pulsePhase += 0.08;
      const pulseRadius = 18 + Math.sin(this.pulsePhase) * 6;
      this.bridgeCtx.beginPath();
      this.bridgeCtx.arc(endX, endY, pulseRadius, 0, Math.PI * 2);
      this.bridgeCtx.strokeStyle = 'rgba(6, 182, 212, 0.4)';
      this.bridgeCtx.lineWidth = 2;
      this.bridgeCtx.stroke();
    }

    requestAnimationFrame(this.animate);
  }
}
