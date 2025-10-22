class FilePolyfill extends Blob {
  constructor(fileBits, fileName, options = {}) {
    if (typeof fileName !== "string" || fileName.length === 0) {
      throw new TypeError("File name must be a non-empty string");
    }
    super(fileBits ?? [], options);
    this.name = fileName.replace(/\//g, ":");
    const lastModified = options.lastModified ?? Date.now();
    this.lastModified = Number.isFinite(lastModified)
      ? Number(lastModified)
      : Date.now();
  }

  get [Symbol.toStringTag]() {
    return "File";
  }
}

if (typeof globalThis.File === "undefined") {
  globalThis.File = FilePolyfill;
}
