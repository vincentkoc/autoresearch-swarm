// Real-browser qualification of the synthetic report only. Node 22+, no npm packages.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';

// Overrides allow the same synthetic proof on a desktop with Chrome installed.
const demo = process.env.REPORT_VIEW_HTML
  ? pathToFileURL(resolve(process.env.REPORT_VIEW_HTML))
  : new URL('../../examples/hill_climb/synthetic-demo.html', import.meta.url);
const output = process.env.REPORT_VIEW_OUTPUT_DIR
  ? pathToFileURL(resolve(process.env.REPORT_VIEW_OUTPUT_DIR) + '/')
  : new URL('../../artifacts/report-view/', import.meta.url);
const title = 'SYNTHETIC DEMO · campaign cockpit';
const controller = new AbortController();
const { signal } = controller;
const deadline = setTimeout(() => controller.abort(new Error('Browser smoke exceeded 90s')), 90_000);
const interrupt = () => controller.abort(new Error('Browser smoke interrupted'));
process.once('SIGINT', interrupt);
process.once('SIGTERM', interrupt);

async function bounded(promise, label, milliseconds = 5_000) {
  signal.throwIfAborted();
  let timer;
  let abort;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), milliseconds);
        abort = () => reject(signal.reason);
        signal.addEventListener('abort', abort, { once: true });
      }),
    ]);
  } finally {
    clearTimeout(timer);
    signal.removeEventListener('abort', abort);
  }
}

class CDP {
  constructor(url) {
    this.socket = new WebSocket(url);
    this.nextId = 0;
    this.pending = new Map();
    this.errors = [];
    this.loaded = new Set();
    this.opened = new Promise((resolve, reject) => {
      this.socket.addEventListener('open', resolve, { once: true });
      this.socket.addEventListener('error', () => reject(new Error('Chrome CDP connection failed')), { once: true });
    });
    this.socket.addEventListener('message', ({ data }) => {
      const message = JSON.parse(data);
      if (message.id) {
        const pending = this.pending.get(message.id);
        if (!pending) return;
        if (message.error) pending.reject(new Error(JSON.stringify(message.error)));
        else pending.resolve(message.result);
      } else if (message.method === 'Page.lifecycleEvent' && message.params.name === 'load') {
        this.loaded.add(message.params.loaderId);
      } else if (message.method === 'Runtime.exceptionThrown') {
        this.errors.push(message.params.exceptionDetails);
      } else if (message.method === 'Log.entryAdded' && message.params.entry.level === 'error') {
        this.errors.push(message.params.entry);
      }
    });
    this.socket.addEventListener('close', () => {
      for (const pending of this.pending.values()) pending.reject(new Error('Chrome CDP closed'));
    });
  }

  async send(method, params = {}, sessionId) {
    const id = ++this.nextId;
    try {
      return await bounded(new Promise((resolve, reject) => {
        this.pending.set(id, { resolve, reject });
        this.socket.send(JSON.stringify({ id, method, params, ...(sessionId ? {sessionId} : {}) }));
      }), method);
    } finally {
      this.pending.delete(id);
    }
  }

  async evaluate(expression) {
    const result = await this.send('Runtime.evaluate', {
      expression, returnByValue: true, awaitPromise: true,
    });
    assert.equal(result.exceptionDetails, undefined, JSON.stringify(result.exceptionDetails));
    return result.result.value;
  }
}

async function waitUntil(check, label, milliseconds = 15_000) {
  const until = Date.now() + milliseconds;
  while (Date.now() < until) {
    signal.throwIfAborted();
    const value = await check();
    if (value) return value;
    await delay(100, undefined, { signal });
  }
  throw new Error(`${label} timed out`);
}

async function settle(cdp) {
  await cdp.evaluate(`document.fonts.ready.then(() => new Promise(resolve =>
    requestAnimationFrame(() => requestAnimationFrame(() => resolve(true)))))`);
}

async function assertPanel(cdp, panel, width) {
  const state = await cdp.evaluate(`(() => {
    const visible = element => !element.hidden && getComputedStyle(element).display !== 'none'
      && element.getBoundingClientRect().height > 0;
    return {
      title: document.title,
      background: getComputedStyle(document.body).backgroundColor,
      width: innerWidth,
      height: innerHeight,
      pageHeight: document.documentElement.scrollHeight,
      headerHeight: document.querySelector('header').getBoundingClientRect().height,
      graphWidth: document.querySelector('.graph-panel').getBoundingClientRect().width,
      sidebarWidth: document.querySelector('.sidebar').getBoundingClientRect().width,
      rightWidth: document.querySelector('.right').getBoundingClientRect().width,
      documentWidth: document.documentElement.scrollWidth,
      bodyWidth: document.body.scrollWidth,
      panels: Object.fromEntries([...document.querySelectorAll('[data-panel]')]
        .map(element => [element.dataset.panel, visible(element)])),
      selected: [...document.querySelectorAll('[data-tab][aria-selected="true"]')]
        .map(element => element.dataset.tab),
      nodes: document.querySelectorAll('.lineage svg g').length,
      greyEdges: document.querySelectorAll('.lineage path[stroke="#bbc1bd"]').length,
      tealEdges: document.querySelectorAll('.lineage path[stroke="#168273"]').length,
      failed: document.body.textContent.includes('synthetic-run-A-trial-failed'),
      unscored: document.body.textContent.includes('synthetic-run-A-trial-unscored'),
    };
  })()`);
  assert.equal(state.title, title);
  assert.equal(state.background, 'rgb(250, 250, 248)', 'Off-white body background');
  assert.equal(state.width, width);
  if (width === 1440) {
    assert.equal(state.headerHeight, 52, 'Compact header, not a report masthead');
    assert.ok(state.pageHeight <= state.height, 'Desktop workspace fits one viewport');
    assert.ok(state.graphWidth > state.width * .5, 'Graph dominates the workspace');
    assert.ok(state.sidebarWidth < state.width * .2, 'Narrow context rail');
    assert.ok(state.rightWidth > state.width * .25, 'Readable leaderboard and activity rail');
  }
  assert.ok(state.documentWidth <= width, `Document overflow: ${JSON.stringify(state)}`);
  assert.ok(state.bodyWidth <= width, `Body overflow: ${JSON.stringify(state)}`);
  assert.deepEqual(state.panels, { lineage: true });
  assert.deepEqual(state.selected, []);
  assert.equal(state.nodes, 50, 'All scored and unscored candidates remain in the campaign');
  assert.equal(state.greyEdges + state.tealEdges, 49, 'All captured parent paths remain');
  assert.ok(state.greyEdges > 0 && state.tealEdges > 0, 'Muted branches and highlighted ancestry');
  assert.ok(state.failed && state.unscored, 'Failed and unscored trials remain visible in evidence');
  assert.deepEqual(cdp.errors, [], 'No page exceptions, CSP violations or browser log errors');
}

async function testRails(cdp, width) {
  const initial = await cdp.evaluate("({left: document.querySelector('.sidebar').getBoundingClientRect().width, right: document.querySelector('.right').getBoundingClientRect().width, links: [...document.querySelectorAll('a')].map(a=>a.href)})");
  assert.ok(initial.links.includes('https://github.com/vincentkoc/autoresearch-swarm'));
  for (const side of ['left', 'right']) {
    const selector = side === 'left' ? '.sidebar' : '.right';
    const railWidth = () => cdp.evaluate("document.querySelector('" + selector + "').getBoundingClientRect().width");
    const toggle = () => cdp.evaluate('document.querySelector(\'[data-toggle="' + side + '"]\').click()');
    await toggle();
    assert.equal(await railWidth(), 0, 'Rail collapses completely');
    if (width === 1440) {
      assert.ok(await cdp.evaluate("document.querySelector('.graph-panel').getBoundingClientRect().width")
        > width * .5, 'Collapsing a rail preserves graph placement');
    }
    await toggle();
    assert.ok(await railWidth() > 0);
    if (width !== 1440) continue;
    const point = await cdp.evaluate('(function(){const r=document.querySelector(\'[data-resize="' + side + '"]\').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()');
    const delta = side === 'left' ? 40 : -40;
    await cdp.send('Input.dispatchMouseEvent', {type:'mousePressed', ...point, button:'left', clickCount:1});
    await cdp.send('Input.dispatchMouseEvent', {type:'mouseMoved', x:point.x+delta, y:point.y, button:'left', buttons:1});
    await cdp.send('Input.dispatchMouseEvent', {type:'mouseReleased', x:point.x+delta, y:point.y, button:'left', clickCount:1});
    const resized = await railWidth();
    assert.ok(Math.abs(resized - initial[side] - 40) < 2, 'Pointer drag resizes rail');
    await cdp.evaluate('document.querySelector(\'[data-resize="' + side + '"]\').focus()');
    const key = side === 'left' ? 'ArrowLeft' : 'ArrowRight';
    await cdp.send('Input.dispatchKeyEvent', {type:'keyDown', key, code:key});
    await cdp.send('Input.dispatchKeyEvent', {type:'keyUp', key, code:key});
    assert.ok(Math.abs(await railWidth() - resized + 20) < 2, 'Keyboard resizes rail');
    await cdp.evaluate("document.querySelector('main').style.removeProperty('--" + side + "-width')");
  }
  if (width === 1440) {
    for (const side of ['left', 'right']) {
      const opposite = side === 'left' ? 'right' : 'left';
      const dragWide = async target => {
        const point = await cdp.evaluate('(function(){const r=document.querySelector(\'[data-resize="' + target + '"]\').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()');
        const x = point.x + (target === 'left' ? 1000 : -1000);
        await cdp.send('Input.dispatchMouseEvent', {type:'mousePressed', ...point, button:'left', clickCount:1});
        await cdp.send('Input.dispatchMouseEvent', {type:'mouseMoved', x, y:point.y, button:'left', buttons:1});
        await cdp.send('Input.dispatchMouseEvent', {type:'mouseReleased', x, y:point.y, button:'left', clickCount:1});
      };
      const toggle = () => cdp.evaluate('document.querySelector(\'[data-toggle="' + side + '"]\').click()');
      await dragWide(side);
      await toggle();
      await dragWide(opposite);
      await toggle();
      assert.ok(await cdp.evaluate("document.querySelector('.graph-panel').getBoundingClientRect().width") >= 280,
        'Restoring a wide collapsed rail preserves the graph minimum');
      await cdp.evaluate("document.querySelector('main').style.removeProperty('--left-width'); document.querySelector('main').style.removeProperty('--right-width')");
    }
  }
  await cdp.evaluate('document.activeElement.blur(); window.scrollTo(0, 0)');
}

async function screenshot(cdp, name) {
  const { cssContentSize } = await cdp.send('Page.getLayoutMetrics');
  assert.ok(cssContentSize.height > 0 && cssContentSize.height <= 20_000, 'Bounded screenshot height');
  const { data } = await cdp.send('Page.captureScreenshot', {
    format: 'png', captureBeyondViewport: true,
    clip: { x: 0, y: 0, width: cssContentSize.width, height: cssContentSize.height, scale: 1 },
  });
  const png = Buffer.from(data, 'base64');
  assert.equal(png.subarray(0, 8).toString('hex'), '89504e470d0a1a0a');
  await writeFile(new URL(name, output), png);
}

async function main() {
  // Operators must supply only the generated synthetic demo, never campaign data.
  assert.ok((await readFile(demo, 'utf8')).includes(title), 'Generate the synthetic demo first');
  await mkdir(output, { recursive: true });
  // Never let a previous invocation's screenshots masquerade as this run's evidence.
  for (const name of ['lineage-desktop.png', 'score-desktop.png', 'lineage-mobile.png',
    'score-mobile.png', 'score-short-desktop.png']) {
    await rm(new URL(name, output), { force: true });
  }
  const profile = await mkdtemp(join(tmpdir(), 'swarm-report-view-'));
  let chrome;
  let server;
  let exited;
  let cdp;
  let startupError;
  let stderr = '';
  try {
    chrome = spawn(process.env.CHROME_BIN || 'google-chrome', [
      '--headless', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
      '--disable-background-networking', '--remote-debugging-address=127.0.0.1',
      '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank',
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    exited = new Promise(resolve => chrome.once('close', resolve));
    chrome.once('error', error => {
      startupError = new Error(`Could not launch Google Chrome; install it or set CHROME_BIN (${error.code})`);
    });
    chrome.stderr.on('data', chunk => { stderr = (stderr + chunk.toString()).slice(-8_000); });
    const port = await waitUntil(async () => {
      if (startupError) throw startupError;
      if (chrome.exitCode !== null || chrome.signalCode !== null) {
        throw new Error(`Chrome exited before CDP was ready: ${stderr}`);
      }
      try {
        const text = await readFile(join(profile, 'DevToolsActivePort'), 'utf8');
        const value = Number(text.split('\n')[0]);
        return Number.isInteger(value) && value > 0 && value < 65536 ? value : false;
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
        return false;
      }
    }, 'Chrome startup');
    const response = await bounded(fetch(`http://127.0.0.1:${port}/json/list`, {
      signal: AbortSignal.any([signal, AbortSignal.timeout(5_000)]),
    }), 'Chrome target discovery');
    assert.ok(response.ok, 'Chrome CDP target discovery');
    const targets = await bounded(response.json(), 'Chrome target list');
    const target = targets.find(item => item.type === 'page' && item.url === 'about:blank');
    assert.ok(target?.webSocketDebuggerUrl, 'Isolated Chrome page target');
    cdp = new CDP(target.webSocketDebuggerUrl);
    await bounded(cdp.opened, 'Chrome CDP connection');
    await cdp.send('Page.enable');
    await cdp.send('Page.setLifecycleEventsEnabled', { enabled: true });
    await cdp.send('Runtime.enable');
    await cdp.send('Log.enable');
    // Catch CSP failures, async page errors and rejected promises from the first document.
    await cdp.send('Page.addScriptToEvaluateOnNewDocument', { source: `
      window.__smokeErrors = [];
      addEventListener('error', event => window.__smokeErrors.push(event.message || 'Resource error'));
      addEventListener('unhandledrejection', event => window.__smokeErrors.push(String(event.reason)));
      addEventListener('securitypolicyviolation', event => window.__smokeErrors.push(event.violatedDirective));
    ` });
    for (const [width, height] of [[1440, 1000], [1440, 700], [390, 844]]) {
      await cdp.send('Emulation.setDeviceMetricsOverride', {
        width, height, deviceScaleFactor: 1, mobile: width === 390,
      });
      const navigation = await cdp.send('Page.navigate', { url: demo.href });
      assert.equal(navigation.errorText, undefined, 'Synthetic demo navigation');
      await waitUntil(() => cdp.loaded.has(navigation.loaderId), 'Demo load');
      assert.equal(await cdp.evaluate('location.href'), demo.href);
      await settle(cdp);
      await assertPanel(cdp, 'lineage', width);
      await testRails(cdp, width);
      await assertPanel(cdp, 'lineage', width);
      await cdp.evaluate("document.querySelector('.chart-node').focus()");
      await cdp.send('Input.dispatchKeyEvent', {type: 'keyDown', key: 'Enter', code: 'Enter'});
      await cdp.send('Input.dispatchKeyEvent', {type: 'keyUp', key: 'Enter', code: 'Enter'});
      assert.match(await cdp.evaluate("document.querySelector('.graph-selection').textContent"),
        /synthetic-run-A-candidate-0/, 'Keyboard point inspection shows exact candidate');
      await cdp.evaluate("document.activeElement.blur()");
      if (height !== 700) {
        await screenshot(cdp, width === 1440 ? 'lineage-desktop.png' : 'lineage-mobile.png');
      }
      assert.deepEqual(await cdp.evaluate('window.__smokeErrors'), [], 'No page/CSP errors');
    }
    // Serve the actual shell against synthetic paginated API responses.
    const shell = await readFile(new URL('synthetic-server-shell.html', demo), 'utf8');
    const report = await readFile(demo, 'utf8');
    const requests = [];
    server = createServer((request, response) => {
      const url = new URL(request.url, 'http://localhost');
      if (url.pathname === '/') return response.end(shell);
      if (request.headers.authorization !== 'Bearer synthetic-read-token') {
        response.writeHead(401); return response.end();
      }
      requests.push(url.pathname + url.search);
      response.setHeader('Content-Type', 'application/json');
      if (url.pathname === '/v1/campaigns') {
        const second = url.searchParams.has('cursor');
        return response.end(JSON.stringify({
          items: Array.from({length: second ? 1 : 100}, (_, i) => ({
            id: 'campaign-' + (second ? 100 : i), data: {name: 'Synthetic ' + i},
          })), next_cursor: second ? null : 'second-page',
        }));
      }
      if (url.pathname.endsWith('/snapshot')) {
        return response.end(JSON.stringify({observed_cohorts: []}));
      }
      response.setHeader('Content-Type', 'text/html');
      response.end(report);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const url = 'http://127.0.0.1:' + server.address().port + '/';
    await cdp.send('Emulation.setDeviceMetricsOverride', {
      width: 1440, height: 1080, deviceScaleFactor: 1, mobile: false,
    });
    const served = await cdp.send('Page.navigate', {url});
    await waitUntil(() => cdp.loaded.has(served.loaderId), 'Campaign shell load');
    await cdp.evaluate(`document.querySelector('#token').value = 'synthetic-read-token';
      document.querySelector('form').requestSubmit()`);
    await waitUntil(() => cdp.evaluate("document.querySelector('#status').textContent.includes('Read-only capture')"),
      'Authenticated campaign render');
    assert.equal(await cdp.evaluate("document.querySelector('#campaign').options.length"), 101);
    assert.equal(await cdp.evaluate("document.querySelector('#token').value"), '');
    assert.ok(requests.some(path => path.includes('cursor=second-page')));
    const embedded = await waitUntil(async () => {
      const {targetInfos} = await cdp.send('Target.getTargets');
      return targetInfos.find(info => info.type === 'iframe');
    }, 'Isolated campaign frame');
    const {sessionId} = await cdp.send('Target.attachToTarget', {
      targetId: embedded.targetId, flatten: true,
    });
    await cdp.send('Runtime.enable', {}, sessionId);
    await waitUntil(async () => {
      const result = await cdp.send('Runtime.evaluate', {
        expression: "Boolean(document.querySelector('[data-toggle=left]'))",
        returnByValue: true,
      }, sessionId);
      return result.result.value;
    }, 'Embedded campaign controls');
    const checked = await cdp.send('Runtime.evaluate', {
      expression: `document.querySelector('[data-toggle=left]').click();
        document.querySelector('.sidebar').hidden`, returnByValue: true,
    }, sessionId);
    assert.equal(checked.result.value, true, 'Embedded report script executes under shell CSP');
    await cdp.evaluate(`document.querySelector('#campaign').value = 'campaign-100';
      document.querySelector('#campaign').dispatchEvent(new Event('change'))`);
    await waitUntil(() => requests.includes('/v1/campaigns/campaign-100/view'), 'Select paginated campaign');
    await waitUntil(() => cdp.evaluate("document.querySelector('#status').textContent.includes('Read-only capture')"),
      'Selected campaign rendered');
    await settle(cdp);
    await screenshot(cdp, 'campaign-served-desktop.png');
    await cdp.evaluate("document.querySelector('#disconnect').click()");
    assert.equal(await cdp.evaluate("document.querySelector('iframe').hasAttribute('srcdoc')"), false);
    assert.deepEqual(await cdp.evaluate('window.__smokeErrors'), [], 'Shell CSP and page errors');
    assert.deepEqual(cdp.errors, [], 'No browser exceptions');
    console.log(`PASS: synthetic report, desktop/mobile layout, campaign-only view, no exceptions; PNGs: ${fileURLToPath(output)}`);
  } finally {
    cdp?.socket.close();
    if (server) {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
    }
    if (chrome && exited) {
      chrome.kill('SIGTERM');
      const closed = await Promise.race([exited.then(() => true), delay(3_000).then(() => false)]);
      if (!closed) {
        chrome.kill('SIGKILL');
        const killed = await Promise.race([exited.then(() => true), delay(3_000).then(() => false)]);
        assert.ok(killed, 'Chrome did not exit; retained profile for diagnosis');
      }
    }
    await rm(profile, { recursive: true, force: true });
  }
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  clearTimeout(deadline);
  process.removeListener('SIGINT', interrupt);
  process.removeListener('SIGTERM', interrupt);
}
