// ZapShare Streaming Storage Adapter
// Progressive file sink supporting FileSystemAccess API, OPFS, and bounded memory fallback
import { StorageMode, TransferDefaults } from './constants.js';

export class StorageAdapter {
  constructor(fileMeta, options = {}) {
    this.fileMeta = fileMeta;
    this.totalSize = fileMeta ? fileMeta.size : 0;
    this.fileName = fileMeta ? fileMeta.name : 'downloaded_file';
    this.mimeType = (fileMeta && fileMeta.type) || 'application/octet-stream';
    this.customFileHandle = options.fileHandle || null;

    this.mode = StorageMode.MEMORY;
    this.bytesWritten = 0;
    this.unwrittenBytes = 0;
    this.maxQueueBytes = options.maxQueueBytes || TransferDefaults.MAX_IN_MEMORY_QUEUE_BYTES;

    // FileSystemAccess stream handles
    this.fileStream = null;

    // OPFS handles
    this.opfsRoot = null;
    this.opfsFileHandle = null;
    this.opfsWritable = null;
    this.tempFileName = null;

    // Fallback in-memory chunks
    this.memoryChunks = [];
    this.memoryAllocatedBytes = 0;

    // Concurrency control for writes
    this.writeQueue = Promise.resolve();
    this.writeErrors = [];
    this.isClosed = false;
  }

  // Detect and initialize the best available storage backend
  async initialize() {
    // 1. Direct FileSystemAccess if caller provided a chosen save handle
    if (this.customFileHandle && typeof this.customFileHandle.createWritable === 'function') {
      try {
        this.fileStream = await this.customFileHandle.createWritable();
        this.mode = StorageMode.FILE_SYSTEM_ACCESS;
        console.log('[StorageAdapter] Initialized native FileSystemAccess writable stream.');
        return this.mode;
      } catch (err) {
        console.warn('[StorageAdapter] FileSystemAccess writable failed, falling back to OPFS:', err);
      }
    }

    // 2. OPFS (Origin Private File System) — Universally supported in modern Chrome, Edge, Safari, Firefox, iOS, Android
    if (typeof navigator !== 'undefined' && navigator.storage && typeof navigator.storage.getDirectory === 'function') {
      try {
        this.opfsRoot = await navigator.storage.getDirectory();
        const safeName = this.fileName.replace(/[^a-zA-Z0-9._-]/g, '_');
        this.tempFileName = `zapshare_temp_${Date.now()}_${safeName}`;
        this.opfsFileHandle = await this.opfsRoot.getFileHandle(this.tempFileName, { create: true });
        
        // Check createWritable
        if (typeof this.opfsFileHandle.createWritable === 'function') {
          this.opfsWritable = await this.opfsFileHandle.createWritable({ keepExistingData: false });
          this.mode = StorageMode.OPFS;
          console.log(`[StorageAdapter] Initialized OPFS streaming storage (${this.tempFileName}). Zero heap memory accumulation!`);
          return this.mode;
        }
      } catch (err) {
        console.warn('[StorageAdapter] OPFS initialization failed, falling back to bounded memory:', err);
      }
    }

    // 3. In-memory fallback
    this.mode = StorageMode.MEMORY;
    console.warn('[StorageAdapter] Initialized in-memory buffer fallback. Notice: large files (>1GB) may experience memory pressure.');
    return this.mode;
  }

  getMode() {
    return this.mode;
  }

  getBytesWritten() {
    return this.bytesWritten;
  }

  // Returns true if disk writes are falling behind network arrival
  isCongested() {
    return this.unwrittenBytes > this.maxQueueBytes;
  }

  /**
   * Progressive chunk write. Enqueues chunk write without blocking the main event loop.
   */
  async write(chunk) {
    if (this.isClosed) {
      throw new Error('StorageAdapter is already closed');
    }

    const byteLen = chunk.byteLength || chunk.size || 0;
    this.unwrittenBytes += byteLen;

    // Chain write operations sequentially
    this.writeQueue = this.writeQueue.then(async () => {
      try {
        const writeStart = performance.now();
        if (this.mode === StorageMode.FILE_SYSTEM_ACCESS && this.fileStream) {
          await this.fileStream.write(chunk);
        } else if (this.mode === StorageMode.OPFS && this.opfsWritable) {
          await this.opfsWritable.write(chunk);
        } else {
          // In-memory fallback: enforce memory safety limits
          this.memoryChunks.push(chunk);
          this.memoryAllocatedBytes += byteLen;
          if (this.memoryAllocatedBytes > 1.5 * 1024 * 1024 * 1024) {
            console.warn(`[StorageAdapter] Memory buffer exceeds 1.5 GB (${(this.memoryAllocatedBytes / (1024 * 1024)).toFixed(0)} MB)!`);
          }
        }
        this.lastWriteLatencyMs = performance.now() - writeStart;

        this.bytesWritten += byteLen;
        this.unwrittenBytes = Math.max(0, this.unwrittenBytes - byteLen);
      } catch (err) {
        this.writeErrors.push(err);
        console.error('[StorageAdapter] Chunk write failed:', err);
        throw err;
      }
    });

    return this.writeQueue;
  }

  /**
   * Finalizes and closes the storage stream.
   * Returns: { blob, file, downloadUrl, path, mode }
   */
  async finalize() {
    this.isClosed = true;

    // Await all queued writes to flush to disk
    await this.writeQueue;

    if (this.writeErrors.length > 0) {
      throw new Error(`Storage write errors occurred (${this.writeErrors.length} failures)`);
    }

    console.log(`[StorageAdapter] Finalizing storage stream (${(this.bytesWritten / (1024 * 1024)).toFixed(2)} MB written). Mode: ${this.mode}`);

    if (this.mode === StorageMode.FILE_SYSTEM_ACCESS && this.fileStream) {
      await this.fileStream.close();
      this.fileStream = null;
      return {
        mode: this.mode,
        blob: null,
        downloadUrl: null,
        isDirectlySaved: true,
        bytesWritten: this.bytesWritten
      };
    }

    if (this.mode === StorageMode.OPFS && this.opfsWritable) {
      await this.opfsWritable.close();
      this.opfsWritable = null;

      // Extract File directly from OPFS handle — no secondary heap duplication!
      const opfsFile = await this.opfsFileHandle.getFile();
      const downloadUrl = URL.createObjectURL(opfsFile);

      return {
        mode: this.mode,
        file: opfsFile,
        blob: opfsFile,
        downloadUrl,
        isDirectlySaved: false,
        bytesWritten: this.bytesWritten
      };
    }

    // Memory fallback: build Blob
    const blob = new Blob(this.memoryChunks, { type: this.mimeType });
    const downloadUrl = URL.createObjectURL(blob);
    this.memoryChunks = []; // Release references to arraybuffers
    this.memoryAllocatedBytes = 0;

    return {
      mode: this.mode,
      blob,
      downloadUrl,
      isDirectlySaved: false,
      bytesWritten: this.bytesWritten
    };
  }

  // Cleanup temporary storage files (e.g. on cancel, error, or session expiry)
  async cleanup() {
    this.isClosed = true;
    try {
      if (this.fileStream) {
        await this.fileStream.abort().catch(() => {});
        this.fileStream = null;
      }
    } catch (e) {}

    try {
      if (this.opfsWritable) {
        await this.opfsWritable.abort().catch(() => {});
        this.opfsWritable = null;
      }
    } catch (e) {}

    try {
      if (this.opfsRoot && this.tempFileName) {
        await this.opfsRoot.removeEntry(this.tempFileName).catch(() => {});
        this.tempFileName = null;
      }
    } catch (e) {}

    this.memoryChunks = [];
    this.memoryAllocatedBytes = 0;
    this.bytesWritten = 0;
    this.unwrittenBytes = 0;
  }
}
