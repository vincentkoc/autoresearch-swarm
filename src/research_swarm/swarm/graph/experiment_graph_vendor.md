# Offline layout engine

The worker bundles ISC-licensed d3-force 3.0.0 with d3-quadtree 3.0.1,
d3-dispatch 3.0.1 and d3-timer 3.0.1. Their license notices are embedded in
WORKER and retained in every exported HTML file.

Rebuild in a temporary directory with npm install --ignore-scripts and these
exact versions plus esbuild 0.25.12. Export forceSimulation, forceManyBody,
forceLink, forceX, forceY, forceCollide from d3-force in entry.js, then run:
esbuild entry.js --bundle --format=iife --global-name=Force --minify --legal-comments=none

Retain the dependency LICENSE text before the bundle and the application worker
after it. No runtime npm install, CDN request or network connection is required.
