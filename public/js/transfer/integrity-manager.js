// ZapShare Integrity Manager
// Chunk-safe, low-memory cryptographic hashing & transfer verification

export class IntegrityManager {
  constructor(options = {}) {
    this.blockSize = options.blockSize || 4 * 1024 * 1024; // 4 MB block hashing
    this.currentBlockBytes = [];
    this.currentBlockSize = 0;
    this.blockDigests = [];
    this.totalBytesHashed = 0;
    this.worker = null;
    this.isWorkerReady = false;

    this.initWorker();
  }

  initWorker() {
    try {
      if (typeof Worker !== 'undefined') {
        this.worker = new Worker(new URL('./transfer-worker.js', import.meta.url), { type: 'module' });
        this.worker.onmessage = (e) => {
          // Handled per action if needed
        };
        this.isWorkerReady = true;
      }
    } catch (err) {
      console.warn('[IntegrityManager] Worker unavailable, using main-thread WebCrypto:', err);
      this.worker = null;
      this.isWorkerReady = false;
    }
  }

  // Update hash with incoming chunk
  async update(chunk) {
    const u8 = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    this.currentBlockBytes.push(u8);
    this.currentBlockSize += u8.byteLength;
    this.totalBytesHashed += u8.byteLength;

    // Once a block reaches 4MB, digest it and free chunk memory!
    if (this.currentBlockSize >= this.blockSize) {
      await this.flushCurrentBlock();
    }
  }

  async flushCurrentBlock() {
    if (this.currentBlockBytes.length === 0) return;

    const blockBuffer = new Uint8Array(this.currentBlockSize);
    let offset = 0;
    for (const piece of this.currentBlockBytes) {
      blockBuffer.set(piece, offset);
      offset += piece.byteLength;
    }
    this.currentBlockBytes = []; // Free memory immediately
    this.currentBlockSize = 0;

    try {
      const digestBuffer = await crypto.subtle.digest('SHA-256', blockBuffer);
      const digestHex = Array.from(new Uint8Array(digestBuffer))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
      this.blockDigests.push(digestHex);
    } catch (err) {
      console.warn('[IntegrityManager] Block digest error:', err);
      this.blockDigests.push(`block_${this.blockDigests.length}`);
    }
  }

  // Finalize digest for the entire file
  async finalize() {
    // Flush any remaining tail bytes
    await this.flushCurrentBlock();

    if (this.blockDigests.length === 0) {
      return 'empty_file';
    }

    // Hash the concatenated block digests to produce the final composite checksum
    try {
      const encoder = new TextEncoder();
      const combinedDigests = encoder.encode(this.blockDigests.join(':'));
      const finalBuffer = await crypto.subtle.digest('SHA-256', combinedDigests);
      const finalHex = Array.from(new Uint8Array(finalBuffer))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
      return finalHex;
    } catch (e) {
      return this.blockDigests.join('').slice(0, 32);
    }
  }

  /**
   * Helper for sender: computes checksum on File by reading progressive slices
   */
  static async computeFileChecksum(file, onProgress = null) {
    const manager = new IntegrityManager();
    const totalSize = file.size;
    const sliceSize = 4 * 1024 * 1024;
    let offset = 0;

    while (offset < totalSize) {
      const slice = file.slice(offset, Math.min(totalSize, offset + sliceSize));
      const buffer = await slice.arrayBuffer();
      await manager.update(buffer);
      offset += buffer.byteLength;
      if (onProgress) {
        onProgress(offset, totalSize);
      }
    }

    return await manager.finalize();
  }

  /**
   * Verifies file transfer parameters
   */
  static verifyTransfer({ expectedName, receivedName, expectedSize, receivedSize, expectedHash, actualHash }) {
    const sizeOk = expectedSize === receivedSize;
    const nameOk = !expectedName || !receivedName || expectedName === receivedName;
    const hashOk = !expectedHash || !actualHash || expectedHash === actualHash;

    const isValid = sizeOk && nameOk && hashOk;

    let reason = 'OK';
    if (!sizeOk) {
      reason = `Size mismatch: expected ${expectedSize} B, received ${receivedSize} B`;
    } else if (!nameOk) {
      reason = `Name mismatch: expected "${expectedName}", received "${receivedName}"`;
    } else if (!hashOk) {
      reason = `Integrity hash mismatch: expected ${expectedHash.slice(0, 10)}..., received ${actualHash.slice(0, 10)}...`;
    }

    return {
      isValid,
      sizeOk,
      nameOk,
      hashOk,
      reason
    };
  }

  destroy() {
    if (this.worker) {
      try { this.worker.terminate(); } catch (e) {}
      this.worker = null;
    }
    this.currentBlockBytes = [];
    this.blockDigests = [];
  }
}
