"use strict";

/**
 * VPS AI Brain Gateway Adapter for Claw3D
 * Integrates PostgreSQL ai_brain (Dual-Brain VPS) with Claw3D Virtual Office.
 * Provides real-time state visualization of the 5 AI Agent Crews:
 * 1. Hermes (Executive Orchestrator & Fahru Co-Pilot)
 * 2. DoniesDaily Squad (Doni, Luna, Milo, Cleo)
 * 3. Juragan Bawang Squad (Reyhan Mkt, Didi Ops)
 * 4. SubTracker & Detimon (SaaS Sentinel)
 * 5. Memo (Gro Vault Librarian)
 */

const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");
const { Pool } = require("pg");

function loadDotenvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const content = fs.readFileSync(filePath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
    if (!match) continue;
    const [, key, rawValue] = match;
    if (process.env[key] !== undefined) continue;
    let value = rawValue.trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

function loadRuntimeEnv() {
  const cwd = process.cwd();
  loadDotenvFile(path.join(cwd, ".env.local"));
  loadDotenvFile(path.join(cwd, ".env"));
}

loadRuntimeEnv();

const HERMES_API_URL = (process.env.HERMES_API_URL || "http://127.0.0.1:8317").replace(/\/$/, "");
const HERMES_API_KEY = process.env.HERMES_API_KEY || "";
const ADAPTER_PORT = parseInt(process.env.HERMES_ADAPTER_PORT || "18789", 10);
const HERMES_MODEL = process.env.HERMES_MODEL || "gemini-3.8-flash-high";
const DATABASE_URL = process.env.DATABASE_URL || "postgresql://postgres:Hernansakti2004@172.18.0.10:5432/postgres";
const HOME = process.env.HOME || "/home/ubuntu";

const MAIN_AGENT_ID = "hermes";
const MAIN_KEY = "main";
const MAIN_SESSION_KEY = `agent:${MAIN_AGENT_ID}:${MAIN_KEY}`;

// Database connection pool
const dbPool = new Pool({
  connectionString: DATABASE_URL,
  max: 3,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

dbPool.on("error", (err) => {
  console.error("[vps-brain-adapter] Unexpected DB error on idle client:", err.message);
});

// ---------------------------------------------------------------------------
// 5 AI Agent Crews Definition
// ---------------------------------------------------------------------------

const DEFAULT_AGENTS = [
  {
    id: "hermes",
    name: "Hermes (Lemonaru)",
    role: "Chief AI Orchestrator & Co-Pilot",
    workspace: "/home/ubuntu",
    identity: { name: "Hermes (Lemonaru)", emoji: "🦅" },
    systemPrompt: `You are Hermes Agent, operating as the daily wingman, personal confidant, and technical co-pilot for Fahrurosadi Hernan (Fahru).
You have full executive visibility across the holding businesses: Juragan Bawang, SubTracker, DoniesDaily, and Obsidian Memory.
Speak in smart, casual Indonesian tongkrongan style with dry wit. Signal over noise. Zero bullshit.`,
    settings: { wipe: false, continuity: true, model: HERMES_MODEL },
  },
  {
    id: "doniesdaily",
    name: "Doni (DoniesDaily)",
    role: "Autonomous YouTube Shorts Producer",
    workspace: "/home/ubuntu/apps/ugc-lemonaru",
    identity: { name: "Doni (DoniesDaily)", emoji: "🐱" },
    systemPrompt: `You are Doni, Chief Producer of DoniesDaily YouTube Shorts channel.
You manage the content pipeline: Luna (Scriptwriter), Milo (Visual Director), and Cleo (Quality Control).
Current target: 100 subscribers sprint. Channel ID: UCNPtjF2aB0wvWHI0q7hhcCg.
Voice: ElevenLabs v4 Vivan. Format: Strict Fahru cat clips + Flow renders.`,
    settings: { wipe: false, continuity: true, model: HERMES_MODEL },
  },
  {
    id: "juragan-bawang",
    name: "Juragan Bawang (Reyhan & Didi)",
    role: "B2B Sales Outreach & Warehouse Ops",
    workspace: "/home/ubuntu/apps/juragan-dashboard",
    identity: { name: "Juragan Bawang (Reyhan & Didi)", emoji: "🧅" },
    systemPrompt: `You are the Juragan Bawang operations agent.
You coordinate B2B restaurant outreach (Reyhan / @paramoreyy) and warehouse inventory/packaging (Didi).
Capacity: 4-5 tons/week, MOQ 5kg. P-IRT: 2093309010181-28. Central DW synced at dw_juragan_bawang.`,
    settings: { wipe: false, continuity: true, model: HERMES_MODEL },
  },
  {
    id: "subtracker",
    name: "SubTracker (Detimon)",
    role: "SaaS Multi-Seat & Billing Sentinel",
    workspace: "/home/ubuntu/apps/subtracker",
    identity: { name: "SubTracker (Detimon)", emoji: "💳" },
    systemPrompt: `You are SubTracker Sentinel.
You manage multi-seat reseller pools, member expirations, and subscription renewals.
7 internal accounts active. Detimon pool master operational.`,
    settings: { wipe: false, continuity: true, model: HERMES_MODEL },
  },
  {
    id: "gro-librarian",
    name: "Memo (Gro Vault Librarian)",
    role: "Autonomous Obsidian Memory Curator",
    workspace: "/home/ubuntu/vault",
    identity: { name: "Memo (Gro Vault Librarian)", emoji: "🧠" },
    systemPrompt: `You are Memo, autonomous Obsidian Vault curator using GRO v3.0 (Golden Rules Obsidian).
You maintain Triad Compaction (Devlog, ActiveContext.md, Sub-MOC Index) and ensure zero split-brain between PostgreSQL ai_brain and Obsidian notes.`,
    settings: { wipe: false, continuity: true, model: HERMES_MODEL },
  },
];

const agentRegistry = new Map();
for (const agent of DEFAULT_AGENTS) {
  agentRegistry.set(agent.id, agent);
}

const conversationHistory = new Map();
const sessionSettings = new Map();
const agentFiles = new Map();
const activeRuns = new Map();
const cronJobs = new Map();
const activeSendEventFns = new Set();
let globalSeqCounter = 1;

// Agent live state tracking ("working" | "idle" | "error")
const agentLiveStates = new Map([
  ["hermes", "idle"],
  ["doniesdaily", "idle"],
  ["juragan-bawang", "idle"],
  ["subtracker", "idle"],
  ["gro-librarian", "idle"],
]);

function randomId() {
  return crypto.randomUUID().replace(/-/g, "");
}

function sessionKeyFor(agentId) {
  return `agent:${agentId}:${MAIN_KEY}`;
}

function getHistory(sessionKey) {
  if (!conversationHistory.has(sessionKey)) {
    conversationHistory.set(sessionKey, []);
  }
  return conversationHistory.get(sessionKey);
}

function setInitialAgentSnippet(agentId, snippetText) {
  const sessionKey = sessionKeyFor(agentId);
  if (!conversationHistory.has(sessionKey) || conversationHistory.get(sessionKey).length === 0) {
    conversationHistory.set(sessionKey, [
      { role: "assistant", content: snippetText, timestamp: Date.now() },
    ]);
  }
}

// Initialize default status snippets
setInitialAgentSnippet("hermes", "Executive Co-Pilot standby. Dual-Brain holding operations nominal.");
setInitialAgentSnippet("doniesdaily", "YouTube Shorts squad active. Latest post: KENAPA KUCING SUKA BERKELAKUAN DI LUAR NALAR? 😹🐾 (yh-RSziA_0E).");
setInitialAgentSnippet("juragan-bawang", "B2B leads engine ready. Warehouse sync running.");
setInitialAgentSnippet("subtracker", "7 internal accounts healthy. Member retention audit active.");
setInitialAgentSnippet("gro-librarian", "GRO v3.0 Triad Compaction verified. 107 vault notes indexed.");

function resOk(id, payload) {
  return { type: "res", id, ok: true, payload: payload ?? {} };
}

function resErr(id, code, message) {
  return { type: "res", id, ok: false, error: { code, message } };
}

function broadcastEvent(frame) {
  for (const send of activeSendEventFns) {
    try {
      send(frame);
    } catch {}
  }
}

// ---------------------------------------------------------------------------
// Database Synchronization with ai_brain
// ---------------------------------------------------------------------------

async function syncAiBrainState() {
  try {
    // 1. Fetch cadence tasks
    const tasksRes = await dbPool.query(
      `SELECT id, task_key, title, domain, business_unit, cron_expr, 
              last_run_at, next_run_at, status 
       FROM ai_brain.cadence_tasks 
       ORDER BY task_key;`
    );

    for (const row of tasksRes.rows) {
      cronJobs.set(row.task_key, {
        id: row.id,
        name: row.title,
        agentId: mapTaskToAgent(row.task_key, row.domain, row.business_unit),
        sessionKey: sessionKeyFor(mapTaskToAgent(row.task_key, row.domain, row.business_unit)),
        description: `${row.domain}/${row.business_unit} | Status: ${row.status}`,
        enabled: row.status === "active",
        schedule: { kind: "cron", expr: row.cron_expr || "0 0 * * *" },
        updatedAtMs: Date.now(),
        lastRunAt: row.last_run_at,
        nextRunAt: row.next_run_at,
      });
    }

    // 2. Fetch recent execution logs (last 5 minutes window for active work)
    const logsRes = await dbPool.query(
      `SELECT task_key, fired_at, status 
       FROM ai_brain.cadence_logs 
       ORDER BY fired_at DESC 
       LIMIT 10;`
    );

    const now = Date.now();
    const activeWindowMs = 3 * 60 * 1000; // 3 minutes

    const recentFiredAgents = new Set();
    for (const log of logsRes.rows) {
      const firedTime = new Date(log.fired_at).getTime();
      if (now - firedTime < activeWindowMs) {
        const agentId = mapTaskToAgent(log.task_key);
        recentFiredAgents.add(agentId);
      }
    }

    // 3. Fetch DoniesDaily production queue
    const doniQueueRes = await dbPool.query(
      `SELECT topic_key, title, current_stage, updated_at 
       FROM ai_brain.doniesdaily_production_queue 
       ORDER BY updated_at DESC 
       LIMIT 1;`
    );

    let doniIsRendering = false;
    if (doniQueueRes.rows.length > 0) {
      const q = doniQueueRes.rows[0];
      const stage = q.current_stage;
      if (stage !== "PUBLISHED" && stage !== "FAILED" && stage !== "ARCHIVED") {
        doniIsRendering = true;
      }
    }

    // 4. Update agent live states
    let stateChanged = false;

    // DoniesDaily
    const nextDoniState = doniIsRendering || recentFiredAgents.has("doniesdaily") ? "working" : "idle";
    if (agentLiveStates.get("doniesdaily") !== nextDoniState) {
      agentLiveStates.set("doniesdaily", nextDoniState);
      stateChanged = true;
    }

    // Juragan Bawang
    const nextJbState = recentFiredAgents.has("juragan-bawang") ? "working" : "idle";
    if (agentLiveStates.get("juragan-bawang") !== nextJbState) {
      agentLiveStates.set("juragan-bawang", nextJbState);
      stateChanged = true;
    }

    // SubTracker
    const nextSubState = recentFiredAgents.has("subtracker") ? "working" : "idle";
    if (agentLiveStates.get("subtracker") !== nextSubState) {
      agentLiveStates.set("subtracker", nextSubState);
      stateChanged = true;
    }

    // Gro Librarian
    const nextGroState = recentFiredAgents.has("gro-librarian") ? "working" : "idle";
    if (agentLiveStates.get("gro-librarian") !== nextGroState) {
      agentLiveStates.set("gro-librarian", nextGroState);
      stateChanged = true;
    }

    // Hermes Orchestrator (active if any chat run is in progress or recent task)
    const nextHermesState = activeRuns.size > 0 || recentFiredAgents.has("hermes") ? "working" : "idle";
    if (agentLiveStates.get("hermes") !== nextHermesState) {
      agentLiveStates.set("hermes", nextHermesState);
      stateChanged = true;
    }

    // 5. Broadcast presence event if state changed or heartbeat
    broadcastPresenceEvent();
  } catch (err) {
    console.warn("[vps-brain-adapter] Error syncing with ai_brain:", err.message);
  }
}

function mapTaskToAgent(taskKey, domain, businessUnit) {
  if (taskKey.includes("doni") || businessUnit === "doniesdaily") return "doniesdaily";
  if (taskKey.includes("reyhan") || taskKey.includes("juragan") || businessUnit === "juragan_bawang") return "juragan-bawang";
  if (taskKey.includes("subtracker") || businessUnit === "subtracker") return "subtracker";
  if (taskKey.includes("gro") || taskKey.includes("vault")) return "gro-librarian";
  return "hermes";
}

function broadcastPresenceEvent() {
  const recentSessions = [];
  const byAgent = [];

  for (const [agentId, state] of agentLiveStates.entries()) {
    const sessionKey = sessionKeyFor(agentId);
    const updatedAt = state === "working" ? Date.now() : Date.now() - 60000;
    recentSessions.push({ key: sessionKey, updatedAt });
    byAgent.push({
      agentId,
      recent: [{ key: sessionKey, updatedAt }],
      state,
    });
  }

  broadcastEvent({
    type: "event",
    event: "presence",
    seq: globalSeqCounter++,
    payload: {
      sessions: {
        recent: recentSessions,
        byAgent,
      },
    },
  });
}

// ---------------------------------------------------------------------------
// LLM Completion helper (Talk to CLIProxyAPI Antigravity on :8317)
// ---------------------------------------------------------------------------

function hermesPost(pathName, body) {
  return new Promise((resolve, reject) => {
    const urlStr = HERMES_API_URL + pathName;
    let url;
    try {
      url = new URL(urlStr);
    } catch {
      reject(new Error(`Invalid URL: ${urlStr}`));
      return;
    }
    const transport = url.protocol === "https:" ? https : http;
    const bodyStr = JSON.stringify(body);
    const headers = {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(bodyStr),
    };
    if (HERMES_API_KEY) headers["Authorization"] = `Bearer ${HERMES_API_KEY}`;
    const req = transport.request(
      {
        hostname: url.hostname,
        port: url.port ? parseInt(url.port, 10) : url.protocol === "https:" ? 443 : 80,
        path: url.pathname + (url.search || ""),
        method: "POST",
        headers,
      },
      resolve
    );
    req.on("error", reject);
    req.write(bodyStr);
    req.end();
  });
}

async function streamChatCompletion({ messages, model, emitDelta, abortCheck }) {
  const body = {
    model: model || HERMES_MODEL,
    messages,
    stream: true,
  };

  const res = await hermesPost("/v1/chat/completions", body);
  if (res.statusCode >= 400) {
    res.resume();
    throw new Error(`Hermes LLM API HTTP ${res.statusCode}`);
  }

  let textContent = "";
  let buffer = "";

  await new Promise((resolve, reject) => {
    res.on("data", (chunk) => {
      if (abortCheck && abortCheck()) {
        res.destroy();
        return;
      }
      buffer += chunk.toString("utf8");
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed === "data: [DONE]") continue;
        if (!trimmed.startsWith("data: ")) continue;
        try {
          const data = JSON.parse(trimmed.slice(6));
          const choice = data?.choices?.[0];
          const delta = choice?.delta?.content;
          if (typeof delta === "string" && delta) {
            textContent += delta;
            if (emitDelta) emitDelta(textContent);
          }
        } catch {}
      }
    });
    res.on("end", resolve);
    res.on("error", reject);
  });

  return textContent;
}

// ---------------------------------------------------------------------------
// WebSocket RPC Dispatcher
// ---------------------------------------------------------------------------

async function handleMethod(method, params, id, sendEvent) {
  const p = params || {};

  switch (method) {
    case "agents.list": {
      const allAgents = [...agentRegistry.values()].map((agent) => ({
        id: agent.id,
        name: agent.name,
        workspace: agent.workspace,
        identity: agent.identity || { name: agent.name, emoji: "🤖" },
        role: agent.role,
      }));
      return resOk(id, { defaultId: MAIN_AGENT_ID, mainKey: MAIN_KEY, agents: allAgents });
    }

    case "sessions.list": {
      const sessions = [...agentRegistry.values()].map((agent) => {
        const sessionKey = sessionKeyFor(agent.id);
        const history = getHistory(sessionKey);
        const settings = sessionSettings.get(sessionKey) || {};
        return {
          key: sessionKey,
          agentId: agent.id,
          updatedAt: history.length > 0 ? Date.now() : null,
          displayName: "Main",
          origin: { label: agent.name, provider: "vps-brain" },
          model: settings.model || HERMES_MODEL,
          modelProvider: "vps-brain",
        };
      });
      return resOk(id, { sessions });
    }

    case "sessions.preview": {
      const keys = Array.isArray(p.keys) ? p.keys : [];
      const limit = typeof p.limit === "number" ? p.limit : 8;
      const maxChars = typeof p.maxChars === "number" ? p.maxChars : 240;
      const previews = keys.map((key) => {
        const history = getHistory(key);
        if (history.length === 0) return { key, status: "empty", items: [] };
        const items = history.slice(-limit).map((msg) => ({
          role: msg.role === "assistant" ? "assistant" : "user",
          text: String(msg.content || "").slice(0, maxChars),
          timestamp: msg.timestamp || Date.now(),
        }));
        return { key, status: "ok", items };
      });
      return resOk(id, { ts: Date.now(), previews });
    }

    case "sessions.patch": {
      const key = typeof p.key === "string" ? p.key : MAIN_SESSION_KEY;
      const current = sessionSettings.get(key) || {};
      const next = { ...current };
      if (p.model !== undefined) next.model = p.model;
      sessionSettings.set(key, next);
      return resOk(id, {
        ok: true,
        key,
        resolved: { model: next.model || HERMES_MODEL, modelProvider: "vps-brain" },
      });
    }

    case "sessions.reset": {
      const key = typeof p.key === "string" ? p.key : MAIN_SESSION_KEY;
      conversationHistory.delete(key);
      return resOk(id, { ok: true });
    }

    case "models.list": {
      return resOk(id, {
        models: [
          { id: "gemini-3.8-flash-high", name: "Gemini 3.8 Flash High (Default VPS)" },
          { id: "claude-sonnet-4-6", name: "Claude Sonnet 4.6" },
          { id: "claude-opus-5-5-high", name: "Claude Opus 5.5 High" },
        ],
      });
    }

    case "config.get":
      return resOk(id, {
        config: { gateway: { reload: { mode: "hot" } } },
        hash: "vps-brain",
        exists: true,
        path: "/home/ubuntu/apps/claw3d/config.json",
      });

    case "cron.list": {
      const jobs = [...cronJobs.values()];
      return resOk(id, { jobs });
    }

    case "skills.status":
      return resOk(id, { skills: [] });

    case "tasks.list":
      return resOk(id, { tasks: [] });

    case "exec.approvals.get":
      return resOk(id, {
        path: "",
        exists: true,
        hash: "vps-approvals",
        file: { version: 1, defaults: { security: "full", ask: "off", autoAllowSkills: true }, agents: {} },
      });

    case "exec.approvals.set":
      return resOk(id, { hash: "vps-approvals" });

    case "exec.approval.resolve":
      return resOk(id, { ok: true });

    case "chat.send": {
      const sessionKey = typeof p.sessionKey === "string" ? p.sessionKey : MAIN_SESSION_KEY;
      const userMessage = typeof p.message === "string" ? p.message.trim() : String(p.message || "").trim();
      const runId = typeof p.idempotencyKey === "string" && p.idempotencyKey ? p.idempotencyKey : randomId();

      if (!userMessage) return resOk(id, { status: "no-op", runId });

      const sessionAgentId = sessionKey.startsWith("agent:") ? sessionKey.split(":")[1] : MAIN_AGENT_ID;
      const agent = agentRegistry.get(sessionAgentId) || agentRegistry.get(MAIN_AGENT_ID);

      const history = getHistory(sessionKey);
      history.push({ role: "user", content: userMessage, timestamp: Date.now() });

      let aborted = false;
      activeRuns.set(runId, {
        runId,
        sessionKey,
        agentId: sessionAgentId,
        abort() {
          aborted = true;
        },
      });

      // Update agent state to working
      agentLiveStates.set(sessionAgentId, "working");
      broadcastPresenceEvent();

      setImmediate(async () => {
        let seqCounter = 0;
        const emitChat = (state, extra) => {
          sendEvent({
            type: "event",
            event: "chat",
            seq: seqCounter++,
            payload: { runId, sessionKey, state, ...extra },
          });
        };

        const onTextDelta = (partial) => {
          if (!aborted) emitChat("delta", { message: { role: "assistant", content: partial } });
        };

        try {
          const messages = [
            { role: "system", content: agent.systemPrompt },
            ...history.slice(-10),
          ];

          const finalText = await streamChatCompletion({
            messages,
            model: HERMES_MODEL,
            emitDelta: onTextDelta,
            abortCheck: () => aborted,
          });

          if (aborted) {
            emitChat("aborted", {});
          } else {
            history.push({ role: "assistant", content: finalText, timestamp: Date.now() });
            emitChat("final", {
              stopReason: "end_turn",
              message: { role: "assistant", content: finalText },
            });
          }
        } catch (err) {
          if (!aborted) {
            emitChat("error", { errorMessage: err.message || "Failed to generate reply" });
          }
        } finally {
          activeRuns.delete(runId);
          agentLiveStates.set(sessionAgentId, "idle");
          broadcastPresenceEvent();
        }
      });

      return resOk(id, { status: "started", runId });
    }

    case "chat.abort": {
      const runId = typeof p.runId === "string" ? p.runId.trim() : "";
      if (runId) {
        const handle = activeRuns.get(runId);
        if (handle) {
          handle.abort();
          activeRuns.delete(runId);
        }
      }
      return resOk(id, { ok: true });
    }

    default:
      return resErr(id, "unsupported_method", `Method ${method} not implemented`);
  }
}

// ---------------------------------------------------------------------------
// Server Initialization
// ---------------------------------------------------------------------------

function startServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("VPS AI Brain Gateway Adapter OK");
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server });

  wss.on("connection", (ws) => {
    let connected = false;

    const send = (frame) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(frame));
      }
    };

    activeSendEventFns.add(send);

    send({ type: "event", event: "connect.challenge", payload: { nonce: randomId() } });

    ws.on("message", async (raw) => {
      let frame;
      try {
        frame = JSON.parse(raw.toString("utf8"));
      } catch {
        return;
      }
      if (!frame || typeof frame !== "object" || frame.type !== "req") return;
      const { id, method, params } = frame;
      if (typeof id !== "string" || typeof method !== "string") return;

      if (method === "connect") {
        connected = true;
        const allAgents = [...agentRegistry.values()].map((a) => ({
          agentId: a.id,
          name: a.name,
          isDefault: a.id === MAIN_AGENT_ID,
        }));
        send({
          type: "res",
          id,
          ok: true,
          payload: {
            type: "hello-ok",
            protocol: 3,
            adapterType: "hermes",
            features: {
              methods: [
                "agents.list", "sessions.list", "sessions.preview",
                "sessions.patch", "sessions.reset", "chat.send", "chat.abort",
                "config.get", "cron.list", "models.list", "skills.status",
                "exec.approvals.get", "exec.approvals.set", "exec.approval.resolve"
              ],
              events: ["chat", "presence", "heartbeat", "cron"],
            },
            snapshot: {
              health: { agents: allAgents, defaultAgentId: MAIN_AGENT_ID },
              sessionDefaults: { mainKey: MAIN_KEY },
            },
            auth: { role: "operator", scopes: ["operator.admin"] },
            policy: { tickIntervalMs: 15000 },
          },
        });
        broadcastPresenceEvent();
        return;
      }

      if (!connected) {
        send(resErr(id, "not_connected", "Send connect first."));
        return;
      }

      try {
        const response = await handleMethod(method, params, id, send);
        send(response);
      } catch (err) {
        console.error(`[vps-brain-adapter] Error in ${method}:`, err.message);
        send(resErr(id, "internal_error", err.message || "Internal error"));
      }
    });

    ws.on("close", () => {
      activeSendEventFns.delete(send);
    });

    ws.on("error", () => {
      activeSendEventFns.delete(send);
    });
  });

  server.listen(ADAPTER_PORT, "127.0.0.1", () => {
    console.log(`[vps-brain-adapter] Live on ws://127.0.0.1:${ADAPTER_PORT}`);
    console.log(`[vps-brain-adapter] Monitoring 5 Agent Crews & PostgreSQL ai_brain`);
  });

  // Sync initial state and start 8-second polling cadence
  syncAiBrainState();
  setInterval(syncAiBrainState, 8000);
}

startServer();
