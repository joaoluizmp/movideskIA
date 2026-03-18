import dotenv from "dotenv";
dotenv.config();

import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import OpenAI from "openai";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "10mb" }));

/* =========================
 * CONFIG
 * ========================= */
const PORT = Number(process.env.PORT || 3000);

const MOVIDESK_BASE_URL =
  process.env.MOVIDESK_BASE_URL || "https://api.movidesk.com/public/v1";
const MOVIDESK_TOKEN = process.env.MOVIDESK_TOKEN;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

const TAG_GERAR = process.env.TAG_GERAR || "GERAR_TAREFA_N2";
const TAG_GERADA = process.env.TAG_GERADA || "TAREFA_N2_GERADA";
const TAG_ERRO = process.env.TAG_ERRO || "ERRO_GERAR_TAREFA_N2";

const STORE_DIR = process.env.STORE_DIR || path.join(__dirname, "data");
const PROCESSED_EVENTS_FILE =
  process.env.PROCESSED_EVENTS_FILE ||
  path.join(STORE_DIR, "processed-events.json");

const MAX_HISTORY_ACTIONS = Number(process.env.MAX_HISTORY_ACTIONS || 30);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);

/* =========================
 * VALIDACOES INICIAIS
 * ========================= */
if (!MOVIDESK_TOKEN) {
  throw new Error("Variável MOVIDESK_TOKEN não configurada.");
}

if (!OPENAI_API_KEY) {
  throw new Error("Variável OPENAI_API_KEY não configurada.");
}

if (!fs.existsSync(STORE_DIR)) {
  fs.mkdirSync(STORE_DIR, { recursive: true });
}

const openai = new OpenAI({
  apiKey: OPENAI_API_KEY,
});

/* =========================
 * LOGGER
 * ========================= */
function log(level, message, meta = {}) {
  const ts = new Date().toISOString();
  const payload = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
  console.log(`[${ts}] [${level}] ${message}${payload}`);
}

function info(message, meta) {
  log("INFO", message, meta);
}

function warn(message, meta) {
  log("WARN", message, meta);
}

function error(message, meta) {
  log("ERROR", message, meta);
}

/* =========================
 * STORE DE EVENTOS PROCESSADOS
 * ========================= */
function loadProcessedEvents() {
  try {
    if (!fs.existsSync(PROCESSED_EVENTS_FILE)) {
      fs.writeFileSync(PROCESSED_EVENTS_FILE, JSON.stringify({}, null, 2), "utf8");
      return {};
    }

    const raw = fs.readFileSync(PROCESSED_EVENTS_FILE, "utf8");
    return raw ? JSON.parse(raw) : {};
  } catch (err) {
    error("Falha ao carregar processed-events.json", { error: err.message });
    return {};
  }
}

function saveProcessedEvents(store) {
  try {
    fs.writeFileSync(PROCESSED_EVENTS_FILE, JSON.stringify(store, null, 2), "utf8");
  } catch (err) {
    error("Falha ao salvar processed-events.json", { error: err.message });
  }
}

const processedEvents = loadProcessedEvents();

function hasProcessedEvent(processingKey) {
  return Boolean(processedEvents[processingKey]);
}

function markProcessedEvent(processingKey, ticketId, actionKey) {
  processedEvents[processingKey] = {
    ticketId,
    actionKey,
    processedAt: new Date().toISOString(),
  };
  saveProcessedEvents(processedEvents);
}

/* =========================
 * FILA DE PROCESSAMENTO
 * ========================= */
const queue = [];
const activeProcessingKeys = new Set();
let queueRunning = false;

function enqueueJob(job) {
  queue.push(job);
  processQueue().catch((err) => {
    error("Erro no loop principal da fila", { error: err.message });
  });
}

async function processQueue() {
  if (queueRunning) return;
  queueRunning = true;

  while (queue.length > 0) {
    const job = queue.shift();

    try {
      await processTicketJob(job);
    } catch (err) {
      error("Falha ao processar job da fila", {
        ticketId: job.ticketId,
        error: err.message,
      });
    }
  }

  queueRunning = false;
}

/* =========================
 * HELPERS GERAIS
 * ========================= */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retry(fn, label, retries = MAX_RETRIES) {
  let attempt = 0;
  let lastError;

  while (attempt < retries) {
    attempt += 1;
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      warn(`${label} falhou`, {
        tentativa: attempt,
        maxTentativas: retries,
        error: err.message,
      });

      if (attempt < retries) {
        const waitMs = attempt * 2000;
        await sleep(waitMs);
      }
    }
  }

  throw lastError;
}

function sha1(input) {
  return crypto.createHash("sha1").update(String(input)).digest("hex");
}

function normalizeText(value) {
  if (value == null) return "";
  return String(value)
    .replace(/\r/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function sanitizeHtml(html = "") {
  return String(html)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<li>/gi, "• ")
    .replace(/<\/li>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .trim();
}

function escapeHtml(text = "") {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function n2MarkdownToHtml(text = "") {
  const lines = String(text).replace(/\r/g, "").split("\n");
  const htmlLines = [];

  for (let line of lines) {
    line = escapeHtml(line);

    line = line.replace(/\*\*(.*?)\*\*/g, "<b>$1</b>");

    if (/^\s*•\s+/.test(line)) {
      line = line.replace(/^\s*•\s+/, "&bull; ");
    }

    if (/^\s*◦\s+/.test(line)) {
      line = line.replace(/^\s*◦\s+/, "&nbsp;&nbsp;&nbsp;&nbsp;&#9702; ");
    }

    htmlLines.push(line || "&nbsp;");
  }

  return htmlLines.join("<br>");
}

function buildUrl(endpoint, query = {}) {
  const url = new URL(`${MOVIDESK_BASE_URL}${endpoint}`);
  url.searchParams.set("token", MOVIDESK_TOKEN);

  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  }

  return url.toString();
}

async function movideskRequest(endpoint, options = {}, query = {}) {
  const url = buildUrl(endpoint, query);

  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  const text = await response.text();
  let data;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    throw new Error(
      `Movidesk ${response.status} ${response.statusText} - ${
        typeof data === "string" ? data : JSON.stringify(data)
      }`
    );
  }

  return data;
}

/* =========================
 * MOVIDESK
 * ========================= */
async function getTicketById(ticketId) {
  return movideskRequest(
    `/tickets`,
    { method: "GET" },
    {
      id: ticketId,
      $expand: "actions,owner,createdBy",
    }
  );
}

async function createInternalAction(ticketId, descriptionHtml) {
  return movideskRequest(
    `/tickets`,
    {
      method: "PATCH",
      body: JSON.stringify({
        actions: [
          {
            description: descriptionHtml,
            type: 1,
          },
        ],
      }),
    },
    {
      id: ticketId,
    }
  );
}

async function updateTicketTags(ticketId, tags) {
  const uniqueTags = [...new Set((tags || []).map((t) => String(t).trim()).filter(Boolean))];

  return movideskRequest(
    `/tickets`,
    {
      method: "PATCH",
      body: JSON.stringify({
        tags: uniqueTags,
      }),
    },
    {
      id: ticketId,
    }
  );
}

/* =========================
 * EXTRAÇÃO DE TAGS / AÇÕES
 * ========================= */
function extractTicketIdFromWebhook(body) {
  const candidates = [
    body?.ticket?.id,
    body?.ticket?.Id,
    body?.ticketId,
    body?.TicketId,
    body?.id,
    body?.Id,
    body?.data?.ticket?.id,
    body?.data?.ticket?.Id,
    body?.data?.id,
    body?.data?.Id,
    body?.object?.id,
    body?.object?.Id,
  ];

  const found = candidates.find((value) => value !== undefined && value !== null && value !== "");

  return found ?? null;
}

function extractTags(ticket) {
  const rawTags = ticket?.tags;

  if (!rawTags) return [];

  if (Array.isArray(rawTags)) {
    return rawTags
      .map((tag) => {
        if (typeof tag === "string") return tag.trim();
        if (tag && typeof tag === "object") {
          return String(tag.name || tag.tag || tag.value || "").trim();
        }
        return "";
      })
      .filter(Boolean);
  }

  if (typeof rawTags === "string") {
    return rawTags
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
  }

  return [];
}

function addTag(tags, tagToAdd) {
  const set = new Set((tags || []).map((t) => String(t).trim()).filter(Boolean));
  set.add(tagToAdd);
  return [...set];
}

function removeTag(tags, tagToRemove) {
  return (tags || []).filter((t) => String(t).trim() !== tagToRemove);
}

function extractActions(ticket) {
  const actions = Array.isArray(ticket?.actions) ? ticket.actions : [];

  return actions
    .map((action, index) => {
      const text = normalizeText(
        action?.description || sanitizeHtml(action?.htmlDescription || "")
      );

      return {
        raw: action,
        id: action?.id || null,
        index,
        type: action?.type ?? null,
        createdDate: action?.createdDate || null,
        createdBy:
          action?.createdBy?.businessName ||
          action?.createdBy?.name ||
          action?.owner?.businessName ||
          "",
        description: text,
      };
    })
    .sort((a, b) => {
      const da = new Date(a.createdDate || 0).getTime();
      const db = new Date(b.createdDate || 0).getTime();
      return da - db;
    });
}

function isGeneratedN2Text(text = "") {
  const t = String(text).toLowerCase();
  return (
    t.includes("1. ocorrência") &&
    t.includes("2. verificação realizada") &&
    t.includes("3. expectativa") &&
    t.includes("4. impacto")
  );
}

function findTriggerAction(ticket) {
  const actions = extractActions(ticket);

  const filtered = actions.filter((a) => {
    if (!a.description) return false;

    const text = a.description.toLowerCase();

    if (isGeneratedN2Text(text)) return false;
    if (text.includes(TAG_GERAR.toLowerCase())) return false;
    if (text.includes(TAG_GERADA.toLowerCase())) return false;
    if (text.includes(TAG_ERRO.toLowerCase())) return false;

    return true;
  });

  if (!filtered.length) return null;

  return filtered[filtered.length - 1];
}

function buildActionKey(triggerAction) {
  if (!triggerAction) return null;

  if (triggerAction.id) return String(triggerAction.id);

  const fallback = `${triggerAction.createdDate || ""}|${triggerAction.description || ""}`;
  return sha1(fallback);
}

function buildProcessingKey(ticketId, actionKey) {
  return `${ticketId}:${actionKey}`;
}

function buildHistoryForPrompt(ticket) {
  const actions = extractActions(ticket)
    .filter((a) => a.description)
    .slice(-MAX_HISTORY_ACTIONS);

  if (!actions.length) {
    return "Nenhuma ação encontrada no histórico do ticket.";
  }

  return actions
    .map((a, idx) => {
      return [
        `Ação ${idx + 1}:`,
        `Data: ${a.createdDate || "N/A"}`,
        `Autor: ${a.createdBy || "N/A"}`,
        `Tipo: ${a.type ?? "N/A"}`,
        `Texto:`,
        a.description,
      ].join("\n");
    })
    .join("\n\n------------------------------\n\n");
}

/* =========================
 * OPENAI
 * ========================= */
function buildN2Prompt(ticket) {
  const history = buildHistoryForPrompt(ticket);

  return `
Você é um especialista em transformar histórico de tickets do Movidesk em tarefas técnicas N2.

Gere a tarefa EXATAMENTE no padrão abaixo.

Regras obrigatórias:
1. As seções devem ser numeradas e em negrito:
**1. OCORRÊNCIA**
**2. VERIFICAÇÃO REALIZADA**
**3. EXPECTATIVA**
**4. IMPACTO**

2. As seções devem ficar alinhadas à margem esquerda.
3. Dentro de cada seção, os itens devem iniciar com marcador •.
4. Dentro de EXPECTATIVA, após **O que precisa ser feito:**, devem existir os subitens:
   ◦ **Verificação da Causa Raiz:**
   ◦ **Correção do Caso Evidenciado:**
   ◦ **Investigação e Correção de Novos Casos:**
5. Todos os títulos dos campos devem estar em negrito e seguidos de dois pontos, com o conteúdo na mesma linha.
6. Não invente informações. Use apenas o que estiver no histórico.
7. Se alguma informação estiver ausente, preencha de forma profissional e neutra, sem inventar dados específicos.
8. O impacto deve ser coerente com o relato.
9. Retorne SOMENTE a tarefa final pronta para copiar e colar.

Modelo obrigatório:

**1. OCORRÊNCIA**
• **Quem solicita:** ...
• **Motivação:** ...
• **Recurso do sistema:** ...

**2. VERIFICAÇÃO REALIZADA**
• **Análise do caso:** ...
• **Motivo do encaminhamento:** ...

**3. EXPECTATIVA**
• **O que precisa ser feito:**
  ◦ **Verificação da Causa Raiz:** ...
  ◦ **Correção do Caso Evidenciado:** ...
  ◦ **Investigação e Correção de Novos Casos:** ...
• **Detalhes importantes:** ...

**4. IMPACTO**
• **Classificação do impacto:** ...
• **Descrição do impacto:** ...

Dados do ticket:
- ID: ${ticket?.id ?? "N/A"}
- Assunto: ${ticket?.subject ?? "N/A"}
- Categoria: ${ticket?.category ?? "N/A"}
- Status: ${ticket?.status ?? "N/A"}

Histórico de ações:
${history}
`.trim();
}

async function generateN2Task(ticket) {
  const prompt = buildN2Prompt(ticket);

  const response = await openai.responses.create({
    model: OPENAI_MODEL,
    input: [
      {
        role: "system",
        content: "Você gera tarefas N2 em português do Brasil, seguindo layout rígido.",
      },
      {
        role: "user",
        content: prompt,
      },
    ],
  });

  return normalizeText(response.output_text || "");
}

/* =========================
 * VALIDACAO DA SAIDA
 * ========================= */
function validateN2Output(text) {
  const checks = [
    "**1. OCORRÊNCIA**",
    "**2. VERIFICAÇÃO REALIZADA**",
    "**3. EXPECTATIVA**",
    "**4. IMPACTO**",
    "**Quem solicita:**",
    "**Motivação:**",
    "**Recurso do sistema:**",
    "**Análise do caso:**",
    "**Motivo do encaminhamento:**",
    "**O que precisa ser feito:**",
    "**Verificação da Causa Raiz:**",
    "**Correção do Caso Evidenciado:**",
    "**Investigação e Correção de Novos Casos:**",
    "**Detalhes importantes:**",
    "**Classificação do impacto:**",
    "**Descrição do impacto:**",
  ];

  const missing = checks.filter((item) => !text.includes(item));

  return {
    isValid: missing.length === 0,
    missing,
  };
}

/* =========================
 * PROCESSAMENTO PRINCIPAL
 * ========================= */
async function processTicketJob(job) {
  const { ticketId, source = "webhook" } = job;

  info("Iniciando processamento do ticket", { ticketId, source });

  const ticket = await retry(
    () => getTicketById(ticketId),
    "Falha ao consultar ticket"
  );

  const tags = extractTags(ticket);

  info("Tags lidas do ticket", {
    ticketId,
    rawTags: ticket?.tags,
    normalizedTags: tags,
    tagEsperada: TAG_GERAR,
  });

  const hasTriggerTag = tags.some(
    (tag) => String(tag).trim().toLowerCase() === TAG_GERAR.trim().toLowerCase()
  );

  if (!hasTriggerTag) {
    warn("Webhook ignorado", {
      ticketId,
      motivo: `Tag ${TAG_GERAR} não está presente`,
    });
    return;
  }

  const triggerAction = findTriggerAction(ticket);

  if (!triggerAction) {
    warn("Webhook ignorado", {
      ticketId,
      motivo: "Nenhuma ação gatilho encontrada",
    });
    return;
  }

  const actionKey = buildActionKey(triggerAction);
  const processingKey = buildProcessingKey(ticketId, actionKey);

  if (activeProcessingKeys.has(processingKey)) {
    warn("Webhook ignorado", {
      ticketId,
      motivo: "Essa solicitação já está em processamento",
      processingKey,
    });
    return;
  }

  if (hasProcessedEvent(processingKey)) {
    warn("Webhook ignorado", {
      ticketId,
      motivo: "Essa mesma solicitação já foi processada",
      processingKey,
    });
    return;
  }

  activeProcessingKeys.add(processingKey);

  try {
    info("Solicitação elegível para geração", {
      ticketId,
      actionKey,
      processingKey,
      triggerActionId: triggerAction.id,
      triggerActionDate: triggerAction.createdDate,
    });

    const n2Text = await retry(
      () => generateN2Task(ticket),
      "Falha ao gerar tarefa N2 na OpenAI"
    );

    const validation = validateN2Output(n2Text);

    if (!validation.isValid) {
      throw new Error(
        `Saída da IA inválida. Campos ausentes: ${validation.missing.join(", ")}`
      );
    }

    const n2Html = n2MarkdownToHtml(n2Text);

    await retry(
      () => createInternalAction(ticketId, n2Html),
      "Falha ao gravar ação interna no Movidesk"
    );

    const updatedTags = addTag(removeTag(tags, TAG_GERAR), TAG_GERADA)
      .filter((t) => t !== TAG_ERRO);

    await retry(
      () => updateTicketTags(ticketId, updatedTags),
      "Falha ao atualizar tags do ticket"
    );

    markProcessedEvent(processingKey, ticketId, actionKey);

    info("Processamento finalizado com sucesso", {
      ticketId,
      processingKey,
    });
  } catch (err) {
    error("Falha no processamento do ticket", {
      ticketId,
      processingKey,
      error: err.message,
    });

    try {
      const ticketAtualizado = await getTicketById(ticketId);
      const tagsAtuais = extractTags(ticketAtualizado);
      const fallbackTags = addTag(tagsAtuais, TAG_ERRO);

      await updateTicketTags(ticketId, fallbackTags);
    } catch (tagErr) {
      error("Falha ao adicionar tag de erro", {
        ticketId,
        error: tagErr.message,
      });
    }
  } finally {
    activeProcessingKeys.delete(processingKey);
  }
}

/* =========================
 * ROTAS
 * ========================= */
app.get("/health", (_, res) => {
  res.status(200).json({
    ok: true,
    service: "movidesk-openai-n2",
    time: new Date().toISOString(),
    queueSize: queue.length,
    activeProcessing: activeProcessingKeys.size,
  });
});

app.post("/process/:ticketId", async (req, res) => {
  const ticketId = Number(req.params.ticketId);

  if (!ticketId) {
    return res.status(400).json({ ok: false, error: "ticketId inválido" });
  }

  enqueueJob({
    ticketId,
    source: "manual",
  });

  return res.status(202).json({
    ok: true,
    message: "Ticket enviado para fila de processamento",
    ticketId,
  });
});

app.post("/webhook", async (req, res) => {
  const body = req.body;
  const ticketId = extractTicketIdFromWebhook(body);

  info("Webhook recebido", {
    ticketId,
    bodyKeys: body ? Object.keys(body) : [],
    bodyPreview: {
      id: body?.id,
      Id: body?.Id,
      ticketId: body?.ticketId,
      TicketId: body?.TicketId,
      ticket_id: body?.ticket_id,
      ticket: body?.ticket ? Object.keys(body.ticket) : null,
      data: body?.data ? Object.keys(body.data) : null,
      object: body?.object ? Object.keys(body.object) : null,
    },
  });

  if (!ticketId) {
    warn("Webhook ignorado", {
      motivo: "Não foi possível identificar o ticketId no payload",
    });

    return res.status(200).json({
      ok: true,
      ignored: true,
      reason: "ticketId não encontrado",
    });
  }

  enqueueJob({
    ticketId: Number(ticketId),
    source: "webhook",
  });

  return res.status(200).json({
    ok: true,
    queued: true,
    ticketId: Number(ticketId),
  });
});

app.listen(PORT, () => {
  info("Servidor iniciado", {
    port: PORT,
    movideskBaseUrl: MOVIDESK_BASE_URL,
    tagGerar: TAG_GERAR,
    tagGerada: TAG_GERADA,
    tagErro: TAG_ERRO,
    processedEventsFile: PROCESSED_EVENTS_FILE,
  });
});