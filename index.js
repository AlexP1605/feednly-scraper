import "./polyfills.js";

// Use a dynamic import so the polyfills are guaranteed to be applied
// before the rest of the application (and its dependencies) execute.
await import("./server.js");
