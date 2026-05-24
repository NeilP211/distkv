// DistKV live console — SSE client + canvas topology renderer + controls.

const RPC_MS = 700;           // visual travel time for an RPC dot
const RPC_COLORS = {
  vote: "#ffcf5f", voteResp: "#ffe9a8",
  append: "#5fff87", appendResp: "#6fb3ff",
  snapshot: "#ff6fd8", snapshotResp: "#ffb0ec", other: "#5c7a64",
};

const state = {
  nodes: [],            // [{id,role,term,leader,commit,lastIndex,up}]
  partitions: null,     // [[id...], ...] or null
  logs: {},             // id -> [{index,term,kind,summary}]
  rpcs: [],             // active animations [{from,to,kind,dropped,t0}]
  pos: {},              // id -> {x,y} (canvas coords, recomputed each frame)
};

// ---- DOM refs ----
const $ = (s) => document.querySelector(s);
const nodeList = $("#nodeList");
const eventLog = $("#eventLog");
const faultBtns = $("#faultBtns");
const partBuilder = $("#partBuilder");
const logView = $("#logView");
const summary = $("#clusterSummary");
const canvas = $("#topo");
const ctx = canvas.getContext("2d");

let evtSeq = 0;

// ---- command helper ----
async function cmd(action, body = {}) {
  try {
    const r = await fetch(`/cmd/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    return await r.json();
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

// ---- SSE ----
function connect() {
  const es = new EventSource("/events");
  es.addEventListener("state", (e) => onState(JSON.parse(e.data)));
  es.addEventListener("rpc", (e) => onRpc(JSON.parse(e.data)));
  es.addEventListener("event", (e) => onEvent(JSON.parse(e.data)));
  es.addEventListener("log", (e) => onLog(JSON.parse(e.data)));
  es.addEventListener("kvresult", (e) => onKvResult(JSON.parse(e.data)));
  es.onerror = () => { summary.textContent = "reconnecting…"; };
}

function onState(s) {
  state.nodes = s.nodes || [];
  state.partitions = s.partitions || null;
  renderNodeList();
  renderFaultBtns();
  renderPartBuilder();
  renderLogView();
  const leader = state.nodes.find((n) => n.up && n.role === "Leader" && n.leader === n.id);
  const upCount = state.nodes.filter((n) => n.up).length;
  const term = Math.max(0, ...state.nodes.map((n) => n.term));
  summary.textContent =
    `${upCount}/${state.nodes.length} up · term ${term} · ` +
    (leader ? `leader ${leader.id}` : "NO LEADER") +
    (state.partitions ? " · PARTITIONED" : "") +
    ` · speed ${s.speed}`;
  // keep speed buttons in sync
  document.querySelectorAll(".spd").forEach((b) =>
    b.classList.toggle("active", b.dataset.speed === s.speed));
}

function onRpc(r) {
  // cap concurrent animations so a burst can't overwhelm the canvas
  if (state.rpcs.length > 400) state.rpcs.shift();
  state.rpcs.push({ ...r, t0: performance.now() });
}

function onEvent(ev) {
  addLogLine(ev.seq, ev.kind, ev.text);
}

function onLog(l) {
  state.logs[l.id] = l.entries || [];
  renderLogView();
}

function onKvResult(res) {
  const detail = res.ok
    ? (res.op === "get"
        ? (res.found ? `${res.key} = ${res.value}` : `${res.key} not found`)
        : `${res.op} ${res.key} ok`)
    : `${res.op} ${res.key} FAILED: ${res.error}`;
  addLogLine(++evtSeq + 9000, "kv", detail);
}

function addLogLine(seq, kind, text) {
  const li = document.createElement("li");
  li.innerHTML = `<span class="seq">${String(seq).padStart(3, "0")}</span><span class="${kind}">${escapeHtml(text)}</span>`;
  eventLog.prepend(li);
  while (eventLog.children.length > 200) eventLog.lastChild.remove();
}

function escapeHtml(s) {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// ---- node list ----
function renderNodeList() {
  nodeList.innerHTML = "";
  for (const n of state.nodes) {
    const li = document.createElement("li");
    if (!n.up) li.classList.add("down");
    li.innerHTML =
      `<div class="nrow"><span class="nid">${n.id}</span>` +
      `<span class="role ${n.role}">${n.role}</span></div>` +
      `<div class="meta">term ${n.term} · commit ${n.commit} · log ${n.lastIndex}</div>`;
    nodeList.appendChild(li);
  }
}

// ---- fault buttons (crash / recover toggle) ----
function renderFaultBtns() {
  faultBtns.innerHTML = "";
  for (const n of state.nodes) {
    const b = document.createElement("button");
    b.textContent = n.up ? `kill ${n.id}` : `wake ${n.id}`;
    if (!n.up) b.style.borderColor = "#4a2b2b";
    b.onclick = () => cmd(n.up ? "crash" : "recover", { id: n.id });
    faultBtns.appendChild(b);
  }
}

// ---- partition builder (toggle nodes into side B) ----
const sideB = new Set();
function renderPartBuilder() {
  partBuilder.innerHTML = "";
  for (const n of state.nodes) {
    const chip = document.createElement("span");
    chip.className = "chip " + (sideB.has(n.id) ? "gB" : "gA");
    chip.textContent = n.id;
    chip.onclick = () => {
      if (sideB.has(n.id)) sideB.delete(n.id); else sideB.add(n.id);
      renderPartBuilder();
    };
    partBuilder.appendChild(chip);
  }
}
$("#partApply").onclick = () => {
  const a = state.nodes.map((n) => n.id).filter((id) => !sideB.has(id));
  const b = [...sideB];
  if (a.length === 0 || b.length === 0) return; // need two non-empty sides
  cmd("partition", { groups: [a, b] });
};
$("#partHeal").onclick = () => { sideB.clear(); renderPartBuilder(); cmd("heal"); };

// ---- drop slider ----
const dropRange = $("#dropRange"), dropOut = $("#dropOut");
let dropTimer = null;
dropRange.oninput = () => {
  dropOut.textContent = dropRange.value + "%";
  clearTimeout(dropTimer);
  dropTimer = setTimeout(() => cmd("drop", { rate: dropRange.value / 100 }), 120);
};

// ---- speed / reset ----
document.querySelectorAll(".spd").forEach((b) =>
  b.onclick = () => cmd("speed", { speed: b.dataset.speed }));
$("#resetBtn").onclick = () => { sideB.clear(); cmd("reset"); };

// ---- KV ops ----
document.querySelectorAll(".kv-btns button").forEach((b) =>
  b.onclick = async () => {
    const key = $("#kvKey").value.trim();
    const value = $("#kvVal").value;
    if (!key) return;
    const res = await cmd(b.dataset.op, { key, value });
    const el = $("#kvResult");
    el.className = "kv-result " + (res.ok ? "ok" : "err");
    el.textContent = res.ok
      ? (b.dataset.op === "get" ? (res.found ? `${key} = ${res.value}` : `${key} not found`) : `${b.dataset.op} ok`)
      : `error: ${res.error}`;
  });

// ---- log viewer ----
function renderLogView() {
  logView.innerHTML = "";
  for (const n of state.nodes) {
    const col = document.createElement("div");
    col.className = "logcol";
    let html = `<div class="head">${n.id}</div>`;
    const entries = state.logs[n.id] || [];
    for (const e of entries.slice(-40)) {
      const committed = e.index <= n.commit ? " committed" : "";
      html += `<div class="ent${committed}"><span class="ix">${e.index}</span>${escapeHtml(e.summary)}</div>`;
    }
    col.innerHTML = html;
    logView.appendChild(col);
  }
}

// ---- canvas topology ----
function resizeCanvas() {
  const dpr = window.devicePixelRatio || 1;
  const r = canvas.getBoundingClientRect();
  canvas.width = r.width * dpr;
  canvas.height = r.height * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
window.addEventListener("resize", resizeCanvas);

function partitionGroupOf(id) {
  if (!state.partitions) return 0;
  for (let i = 0; i < state.partitions.length; i++)
    if (state.partitions[i].includes(id)) return i;
  return -1;
}

function layout() {
  const r = canvas.getBoundingClientRect();
  const cx = r.width / 2, cy = r.height / 2;
  const radius = Math.min(r.width, r.height) * 0.36;
  const n = state.nodes.length || 1;
  state.pos = {};
  state.nodes.forEach((node, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    state.pos[node.id] = { x: cx + radius * Math.cos(a), y: cy + radius * Math.sin(a) };
  });
}

function draw() {
  const r = canvas.getBoundingClientRect();
  ctx.clearRect(0, 0, r.width, r.height);
  layout();

  // edges
  const ids = state.nodes.map((n) => n.id);
  for (let i = 0; i < ids.length; i++) {
    for (let j = i + 1; j < ids.length; j++) {
      const p = state.pos[ids[i]], q = state.pos[ids[j]];
      if (!p || !q) continue;
      const split = state.partitions && partitionGroupOf(ids[i]) !== partitionGroupOf(ids[j]);
      ctx.beginPath();
      ctx.moveTo(p.x, p.y); ctx.lineTo(q.x, q.y);
      ctx.strokeStyle = split ? "rgba(255,95,95,0.10)" : "rgba(95,255,135,0.10)";
      ctx.setLineDash(split ? [4, 6] : []);
      ctx.lineWidth = 1;
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  // rpc dots
  const now = performance.now();
  state.rpcs = state.rpcs.filter((m) => now - m.t0 < RPC_MS);
  for (const m of state.rpcs) {
    const p = state.pos[m.from], q = state.pos[m.to];
    if (!p || !q) continue;
    let t = (now - m.t0) / RPC_MS;
    if (m.dropped) t = Math.min(t, 0.45); // dropped dots stop partway
    const x = p.x + (q.x - p.x) * t, y = p.y + (q.y - p.y) * t;
    const color = m.dropped ? "#ff5f5f" : (RPC_COLORS[m.kind] || RPC_COLORS.other);
    const alpha = m.dropped ? Math.max(0, 1 - t * 2) : 1;
    ctx.globalAlpha = alpha;
    ctx.beginPath();
    ctx.arc(x, y, 3.2, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.shadowColor = color; ctx.shadowBlur = 8;
    ctx.fill();
    ctx.shadowBlur = 0;
    ctx.globalAlpha = 1;
  }

  // nodes
  for (const node of state.nodes) {
    const p = state.pos[node.id];
    if (!p) continue;
    const isLeader = node.up && node.role === "Leader";
    let color = "#2f7a45";
    if (!node.up) color = "#3a3a3a";
    else if (node.role === "Leader") color = "#5fff87";
    else if (node.role === "Candidate") color = "#ffcf5f";

    ctx.beginPath();
    ctx.arc(p.x, p.y, 20, 0, 2 * Math.PI);
    ctx.fillStyle = "#0a140d";
    ctx.fill();
    ctx.lineWidth = isLeader ? 3 : 1.5;
    ctx.strokeStyle = color;
    if (isLeader) { ctx.shadowColor = color; ctx.shadowBlur = 16; }
    ctx.stroke();
    ctx.shadowBlur = 0;

    ctx.fillStyle = color;
    ctx.font = "bold 13px ui-monospace, monospace";
    ctx.textAlign = "center"; ctx.textBaseline = "middle";
    ctx.fillText(node.up ? node.id : node.id + "✕", p.x, p.y - 2);
    ctx.font = "10px ui-monospace, monospace";
    ctx.fillStyle = "#5c7a64";
    ctx.fillText("t" + node.term, p.x, p.y + 10);

    if (isLeader) {
      ctx.fillStyle = "#5fff87";
      ctx.font = "9px ui-monospace, monospace";
      ctx.fillText("LEADER", p.x, p.y - 30);
    }
  }

  requestAnimationFrame(draw);
}

resizeCanvas();
requestAnimationFrame(draw);
connect();
