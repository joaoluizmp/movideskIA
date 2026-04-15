// server.js
import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "5mb" }));

// =====================================================================================
// CONFIG
// =====================================================================================

const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

const MOVIDESK_TOKEN = process.env.MOVIDESK_TOKEN;
const MOVIDESK_BASE_URL =
  process.env.MOVIDESK_BASE_URL || "https://api.movidesk.com/public/v1";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

const TAG_TRIGGER = "GERAR_TAREFA_N2";
const TAG_SUCCESS = "TAREFA_N2_GERADA";
const TAG_ERROR = "ERRO_GERAR_TAREFA_N2";

const AUTOMATION_AUTHOR_NAMES = [
  "OpenAI",
  "Automação N2",
  "Automacao N2",
  "Webhook N2",
  "Integração N2",
  "Integracao N2",
  "IA N2",
];

const DATA_DIR = path.join(__dirname, "data");
const PROCESSED_FILE = path.join(DATA_DIR, "processed-events.json");

const activeProcessingKeys = new Set();

ensureDataFiles();

// =====================================================================================
// START
// =====================================================================================

app.listen(PORT, HOST, () => {
  console.log(`[START] Servidor rodando em http://${HOST}:${PORT}`);
});

// =====================================================================================
// ROUTES
// =====================================================================================

app.get("/health", async (_req, res) => {
  res.json({
    ok: true,
    service: "movidesk-ia-n2",
    time: new Date().toISOString(),
    model: OPENAI_MODEL,
    movideskBaseUrl: MOVIDESK_BASE_URL,
  });
});

app.post("/webhook", async (req, res) => {
  try {
    const body = req.body || {};
    console.log("[WEBHOOK] Payload recebido:", JSON.stringify(body, null, 2));

    const ticketId = extractTicketId(body);

    if (!ticketId) {
      console.warn("[WEBHOOK] Não foi possível identificar o ticketId.");
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "ticketId não encontrado no payload",
      });
    }

    const payloadText = safeLower(JSON.stringify(body));
    const hasTriggerTagInPayload = payloadText.includes(safeLower(TAG_TRIGGER));

    // Se o payload não trouxer tags claramente, ainda deixamos seguir:
    // vamos validar no ticket depois.
    const referenceDate = extractEventDate(body) || new Date();

    const processingKey = buildProcessingKey({
      source: "webhook",
      ticketId,
      referenceDate,
      payload: body,
    });

    if (isProcessingOrProcessed(processingKey)) {
      console.log("[WEBHOOK] Evento duplicado ignorado:", processingKey);
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "evento duplicado",
      });
    }

    markProcessing(processingKey);

    processTicketFlow({
      ticketId,
      referenceDate,
      processingKey,
      triggerValidationMode: hasTriggerTagInPayload ? "soft-confirmed" : "ticket-confirm",
    })
      .catch((err) => {
        console.error("[WEBHOOK] Erro assíncrono no processamento:", err);
      });

    return res.status(202).json({
      ok: true,
      accepted: true,
      ticketId,
      processingKey,
    });
  } catch (error) {
    console.error("[WEBHOOK] Erro:", error);
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.post("/process/:ticketId", async (req, res) => {
  try {
    const ticketId = Number(req.params.ticketId);
    if (!ticketId) {
      return res.status(400).json({ ok: false, error: "ticketId inválido" });
    }

    const referenceDate = new Date();
    const processingKey = buildProcessingKey({
      source: "manual",
      ticketId,
      referenceDate,
      payload: req.body || {},
    });

    if (isProcessingOrProcessed(processingKey)) {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "processamento já realizado",
        processingKey,
      });
    }

    markProcessing(processingKey);

    const result = await processTicketFlow({
      ticketId,
      referenceDate,
      processingKey,
      triggerValidationMode: "manual",
    });

    return res.json({
      ok: true,
      result,
    });
  } catch (error) {
    console.error("[MANUAL PROCESS] Erro:", error);
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

// =====================================================================================
// MAIN FLOW
// =====================================================================================

async function processTicketFlow({
  ticketId,
  referenceDate = new Date(),
  processingKey,
  triggerValidationMode = "ticket-confirm",
}) {
  console.log(`[FLOW] Iniciando processamento do ticket ${ticketId}`);

  try {
    const ticket = await getTicketById(ticketId);

    if (!ticket) {
      throw new Error(`Ticket ${ticketId} não encontrado no Movidesk.`);
    }

    const currentTags = getTicketTags(ticket);

    if (triggerValidationMode !== "manual") {
      if (!hasTag(currentTags, TAG_TRIGGER)) {
        console.log(
          `[FLOW] Ticket ${ticketId} não possui a tag ${TAG_TRIGGER}. Ignorado.`
        );
        markProcessed(processingKey, {
          status: "ignored",
          reason: "tag trigger ausente no ticket",
          ticketId,
        });
        unmarkProcessing(processingKey);
        return {
          ignored: true,
          reason: `tag ${TAG_TRIGGER} ausente`,
          ticketId,
        };
      }
    }

    const actions = Array.isArray(ticket.actions) ? ticket.actions : [];

    const {
      selectedAction,
      selectedText,
      candidates,
    } = selectBestN2SourceAction(actions, {
      automationAuthorNames: AUTOMATION_AUTHOR_NAMES,
      referenceDate,
      debug: true,
    });

    if (!selectedAction || !selectedText) {
      throw new Error(
        "Nenhuma ação interna válida foi encontrada para gerar a tarefa N2."
      );
    }

    const sourceFingerprint = buildSourceFingerprint(ticketId, selectedAction, selectedText);

    if (hasProcessedSourceFingerprint(sourceFingerprint)) {
      console.log(
        `[FLOW] Ticket ${ticketId} já teve essa mesma ação processada. Ignorado.`
      );
      markProcessed(processingKey, {
        status: "ignored",
        reason: "mesma ação já processada",
        ticketId,
        sourceFingerprint,
      });
      unmarkProcessing(processingKey);
      return {
        ignored: true,
        reason: "mesma ação já processada",
        ticketId,
        sourceFingerprint,
      };
    }

    console.log("[FLOW] Ação escolhida para geração:", {
      author: getActionAuthorName(selectedAction),
      date: getActionDate(selectedAction)?.toISOString?.() || null,
      preview: selectedText.slice(0, 300),
    });

    const prompt = buildN2Prompt(selectedText);

    const gptText = await generateN2Task(prompt);

    if (!gptText || !gptText.trim()) {
      throw new Error("A OpenAI retornou conteúdo vazio.");
    }

    const htmlForMovidesk = markdownLikeToHtml(gptText);

    await addInternalActionToTicket(ticketId, htmlForMovidesk);

    const updatedTags = mergeTags(currentTags, {
      remove: [TAG_TRIGGER, TAG_ERROR],
      add: [TAG_SUCCESS],
    });

    await updateTicketTags(ticketId, updatedTags);

    markProcessed(processingKey, {
      status: "success",
      ticketId,
      sourceFingerprint,
      selectedActionDate: getActionDate(selectedAction)?.toISOString?.() || null,
      selectedActionAuthor: getActionAuthorName(selectedAction),
      preview: selectedText.slice(0, 200),
    });

    unmarkProcessing(processingKey);

    console.log(`[FLOW] Ticket ${ticketId} processado com sucesso.`);

    return {
      success: true,
      ticketId,
      selectedActionAuthor: getActionAuthorName(selectedAction),
      selectedActionDate: getActionDate(selectedAction)?.toISOString?.() || null,
      candidatesCount: candidates.length,
      sourceFingerprint,
    };
  } catch (error) {
    console.error(`[FLOW] Erro ao processar ticket ${ticketId}:`, error);

    try {
      const ticket = await safeGetTicketById(ticketId);
      if (ticket) {
        const currentTags = getTicketTags(ticket);
        const updatedTags = mergeTags(currentTags, {
          remove: [],
          add: [TAG_ERROR],
        });
        await updateTicketTags(ticketId, updatedTags);
      }
    } catch (tagError) {
      console.error("[FLOW] Falha ao aplicar tag de erro:", tagError);
    }

    markProcessed(processingKey, {
      status: "error",
      ticketId,
      error: error.message,
    });

    unmarkProcessing(processingKey);
    throw error;
  }
}

// =====================================================================================
// MOVIDESK
// =====================================================================================

async function getTicketById(ticketId) {
  if (!MOVIDESK_TOKEN) {
    throw new Error("MOVIDESK_TOKEN não configurado.");
  }

  const url = new URL(`${MOVIDESK_BASE_URL}/tickets`);
  url.searchParams.set("token", MOVIDESK_TOKEN);
  url.searchParams.set("id", String(ticketId));
  url.searchParams.set("$expand", "actions,owner,createdBy");

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Erro ao buscar ticket no Movidesk. Status ${response.status}. Body: ${text}`
    );
  }

  const data = parseJsonSafe(text);

  if (Array.isArray(data)) {
    return data[0] || null;
  }

  return data || null;
}

async function safeGetTicketById(ticketId) {
  try {
    return await getTicketById(ticketId);
  } catch {
    return null;
  }
}

async function addInternalActionToTicket(ticketId, htmlText) {
  if (!MOVIDESK_TOKEN) {
    throw new Error("MOVIDESK_TOKEN não configurado.");
  }

  const url = new URL(`${MOVIDESK_BASE_URL}/tickets`);
  url.searchParams.set("token", MOVIDESK_TOKEN);
  url.searchParams.set("id", String(ticketId));

  const body = {
    actions: [
      {
        description: htmlText,
        type: 1, // ação interna
      },
    ],
  };

  const response = await fetch(url.toString(), {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Erro ao adicionar ação interna no ticket ${ticketId}. Status ${response.status}. Body: ${text}`
    );
  }

  return parseJsonSafe(text);
}

async function updateTicketTags(ticketId, tags) {
  if (!MOVIDESK_TOKEN) {
    throw new Error("MOVIDESK_TOKEN não configurado.");
  }

  const url = new URL(`${MOVIDESK_BASE_URL}/tickets`);
  url.searchParams.set("token", MOVIDESK_TOKEN);
  url.searchParams.set("id", String(ticketId));

  const body = {
    tags,
  };

  const response = await fetch(url.toString(), {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Erro ao atualizar tags do ticket ${ticketId}. Status ${response.status}. Body: ${text}`
    );
  }

  return parseJsonSafe(text);
}

// =====================================================================================
// OPENAI
// =====================================================================================

async function generateN2Task(prompt) {
  if (!OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY não configurada.");
  }

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text: [
                "Você é um especialista em suporte técnico e redação de tarefas N2.",
                "Sua função é transformar uma análise de suporte em uma tarefa N2 clara, objetiva e padronizada.",
                "Nunca misture assuntos diferentes.",
                "Use somente o conteúdo fornecido pelo usuário.",
                "Não invente informações.",
                "Se faltar algum dado, mantenha a redação neutra sem criar detalhes falsos.",
                "Responda exatamente no formato solicitado.",
              ].join(" "),
            },
          ],
        },
        {
          role: "user",
          content: [
            {
              type: "input_text",
              text: prompt,
            },
          ],
        },
      ],
    }),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Erro na OpenAI. Status ${response.status}. Body: ${text}`
    );
  }

  const data = parseJsonSafe(text);
  const outputText = extractResponseOutputText(data);

  return outputText?.trim();
}

function buildN2Prompt(sourceText) {
  return `
Você é responsável por transformar uma análise de suporte em uma tarefa N2.

REGRAS IMPORTANTES:
- Use SOMENTE o conteúdo fornecido abaixo.
- NÃO invente informações.
- NÃO misture com outros possíveis assuntos do ticket.
- Se algum dado estiver ausente, mantenha a redação neutra sem criar detalhes.
- Preserve o assunto principal exatamente como descrito.
- Escreva em português do Brasil.
- Retorne exatamente no layout abaixo.

TEXTO BASE:
"""
${sourceText}
"""

FORMATO OBRIGATÓRIO:

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
`.trim();
}

function extractResponseOutputText(data) {
  if (!data) return "";

  if (typeof data.output_text === "string" && data.output_text.trim()) {
    return data.output_text;
  }

  if (Array.isArray(data.output)) {
    const parts = [];

    for (const item of data.output) {
      if (!Array.isArray(item.content)) continue;

      for (const content of item.content) {
        if (
          (content.type === "output_text" || content.type === "text") &&
          typeof content.text === "string"
        ) {
          parts.push(content.text);
        }
      }
    }

    return parts.join("\n").trim();
  }

  return "";
}

// =====================================================================================
// INTELIGÊNCIA DE SELEÇÃO DA AÇÃO
// =====================================================================================

function normalizeText(text = "") {
  return String(text)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<li>/gi, "• ")
    .replace(/<\/li>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\r/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function removeAccents(text = "") {
  return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function safeLower(text = "") {
  return removeAccents(String(text).toLowerCase());
}

function isInternalAction(action) {
  return action?.type === 1;
}

function getActionDate(action) {
  const raw =
    action?.createdDate ||
    action?.date ||
    action?.created_at ||
    action?.originDate ||
    null;

  const dt = raw ? new Date(raw) : null;
  return dt && !Number.isNaN(dt.getTime()) ? dt : null;
}

function getActionAuthorName(action) {
  return (
    action?.createdBy?.businessName ||
    action?.createdBy?.name ||
    action?.owner?.businessName ||
    action?.owner?.name ||
    action?.createdByName ||
    ""
  );
}

function isAutomationGeneratedText(text = "") {
  const t = safeLower(text);

  const automationPatterns = [
    "1. ocorrencia",
    "2. verificacao realizada",
    "3. expectativa",
    "4. impacto",
    "**1. ocorrencia**",
    "**2. verificacao realizada**",
    "**3. expectativa**",
    "**4. impacto**",
    "gerar_tarefa_n2",
    "tarefa_n2_gerada",
    "erro_gerar_tarefa_n2",
  ];

  return automationPatterns.some((p) => t.includes(p));
}

function looksLikeSystemNoise(text = "") {
  const t = safeLower(text);

  const noisePatterns = [
    "ticket encaminhado",
    "sla alterado",
    "status alterado",
    "responsavel alterado",
    "responsável alterado",
    "categoria alterada",
    "urgencia alterada",
    "urgência alterada",
    "prioridade alterada",
    "acao automatica",
    "ação automática",
    "movidesk",
    "email recebido",
    "email enviado",
    "chat encerrado",
    "ticket encerrado",
  ];

  return noisePatterns.some((p) => t.includes(p));
}

function countKeywordMatches(text = "", keywords = []) {
  const t = safeLower(text);
  return keywords.reduce((acc, keyword) => {
    return acc + (t.includes(safeLower(keyword)) ? 1 : 0);
  }, 0);
}

function hasN2Marker(text = "") {
  const t = safeLower(text);
  return t.includes("[n2]") || t.includes("inicio_tarefa_n2") || t.includes("#n2");
}

function calculateActionScore(action, referenceDate = null) {
  const rawText = action?.description || action?.text || "";
  const text = normalizeText(rawText);
  const date = getActionDate(action);

  let score = 0;

  if (hasN2Marker(text)) score += 100;

  const length = text.length;
  if (length >= 300) score += 30;
  else if (length >= 180) score += 20;
  else if (length >= 120) score += 12;
  else score -= 20;

  const strongKeywords = [
    "verificado",
    "verifiquei",
    "analise",
    "análise",
    "nao foi possivel",
    "não foi possível",
    "necessario",
    "necessário",
    "encaminhamento",
    "causa raiz",
    "correcao",
    "correção",
    "acerto de base",
    "impacto",
    "cliente",
    "contrato",
    "parcela",
    "erro",
    "critica",
    "crítica",
    "reproduzido",
    "evidenciado",
    "expectativa",
    "ocorrencia",
    "ocorrência",
    "n2",
  ];

  const weakKeywords = [
    "cpf",
    "cnpj",
    "titulo",
    "título",
    "boleto",
    "sms",
    "whatsapp",
    "negociacao",
    "negociação",
    "integracao",
    "integração",
    "sienge",
    "omie",
    "serasa",
    "spc",
    "pix",
    "stage",
    "api",
  ];

  score += countKeywordMatches(text, strongKeywords) * 8;
  score += countKeywordMatches(text, weakKeywords) * 3;

  if (safeLower(text).includes("motivo do encaminhamento")) score += 20;
  if (safeLower(text).includes("o n2 deve")) score += 25;
  if (safeLower(text).includes("impacto")) score += 10;
  if (safeLower(text).includes("via tela")) score += 10;
  if (safeLower(text).includes("ao tentar")) score += 8;
  if (safeLower(text).includes("foi identificado")) score += 8;
  if (safeLower(text).includes("foi verificado")) score += 10;

  if (isAutomationGeneratedText(text)) score -= 200;
  if (looksLikeSystemNoise(text)) score -= 40;

  if (referenceDate && date) {
    const diffMs = Math.abs(referenceDate.getTime() - date.getTime());
    const diffMinutes = diffMs / (1000 * 60);

    if (diffMinutes <= 2) score += 35;
    else if (diffMinutes <= 5) score += 25;
    else if (diffMinutes <= 15) score += 15;
    else if (diffMinutes <= 60) score += 5;
  }

  return score;
}

function isValidCandidateAction(action, options = {}) {
  const { automationAuthorNames = [], referenceDate = null } = options;

  if (!action) return false;
  if (!isInternalAction(action)) return false;

  const rawText = action?.description || action?.text || "";
  const text = normalizeText(rawText);

  if (!text) return false;

  // Regra mais segura:
  // texto curto só entra se vier com marcador [N2]
  if (text.length < 120 && !hasN2Marker(text)) return false;

  const authorName = safeLower(getActionAuthorName(action));
  const automationAuthorsNormalized = automationAuthorNames.map(safeLower);

  if (
    authorName &&
    automationAuthorsNormalized.some(
      (name) => name && authorName.includes(name)
    )
  ) {
    return false;
  }

  if (isAutomationGeneratedText(text)) return false;

  const actionDate = getActionDate(action);
  if (referenceDate && actionDate && actionDate > referenceDate) {
    return false;
  }

  return true;
}

function selectBestN2SourceAction(actions = [], options = {}) {
  const { automationAuthorNames = [], referenceDate = null, debug = false } = options;

  const candidates = actions
    .filter((action) =>
      isValidCandidateAction(action, {
        automationAuthorNames,
        referenceDate,
      })
    )
    .map((action) => {
      const text = normalizeText(action?.description || action?.text || "");
      const score = calculateActionScore(action, referenceDate);

      return {
        action,
        text,
        score,
        author: getActionAuthorName(action),
        date: getActionDate(action),
      };
    })
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;

      const dateA = a.date ? a.date.getTime() : 0;
      const dateB = b.date ? b.date.getTime() : 0;
      return dateB - dateA;
    });

  if (debug) {
    console.log("===== CANDIDATAS N2 =====");
    for (const item of candidates.slice(0, 10)) {
      console.log({
        score: item.score,
        author: item.author,
        date: item.date?.toISOString?.() || null,
        preview: item.text.slice(0, 180),
      });
    }
  }

  if (!candidates.length) {
    return {
      selectedAction: null,
      selectedText: null,
      candidates: [],
    };
  }

  return {
    selectedAction: candidates[0].action,
    selectedText: candidates[0].text,
    candidates,
  };
}

// =====================================================================================
// TAGS
// =====================================================================================

function getTicketTags(ticket) {
  const raw = ticket?.tags;

  if (!raw) return [];

  if (Array.isArray(raw)) {
    return raw
      .map((tag) => String(tag).trim())
      .filter(Boolean);
  }

  if (typeof raw === "string") {
    return raw
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);
  }

  return [];
}

function hasTag(tags = [], expectedTag = "") {
  return tags.some((tag) => safeLower(tag) === safeLower(expectedTag));
}

function mergeTags(existingTags = [], { add = [], remove = [] } = {}) {
  const normalizedRemove = remove.map(safeLower);

  const kept = existingTags.filter(
    (tag) => !normalizedRemove.includes(safeLower(tag))
  );

  const result = [...kept];

  for (const tag of add) {
    if (!result.some((existing) => safeLower(existing) === safeLower(tag))) {
      result.push(tag);
    }
  }

  return result;
}

// =====================================================================================
// DEDUP / PROCESS CONTROL
// =====================================================================================

function ensureDataFiles() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  if (!fs.existsSync(PROCESSED_FILE)) {
    fs.writeFileSync(
      PROCESSED_FILE,
      JSON.stringify(
        {
          processedKeys: {},
          processedSourceFingerprints: {},
        },
        null,
        2
      ),
      "utf-8"
    );
  }
}

function loadProcessedState() {
  try {
    const raw = fs.readFileSync(PROCESSED_FILE, "utf-8");
    const data = JSON.parse(raw);

    return {
      processedKeys: data?.processedKeys || {},
      processedSourceFingerprints: data?.processedSourceFingerprints || {},
    };
  } catch {
    return {
      processedKeys: {},
      processedSourceFingerprints: {},
    };
  }
}

function saveProcessedState(state) {
  fs.writeFileSync(PROCESSED_FILE, JSON.stringify(state, null, 2), "utf-8");
}

function buildProcessingKey({ source, ticketId, referenceDate, payload }) {
  const hash = crypto
    .createHash("sha256")
    .update(
      JSON.stringify({
        source,
        ticketId,
        referenceDate: referenceDate?.toISOString?.() || null,
        payload,
      })
    )
    .digest("hex");

  return `${source}:${ticketId}:${hash}`;
}

function buildSourceFingerprint(ticketId, action, text) {
  const hash = crypto
    .createHash("sha256")
    .update(
      JSON.stringify({
        ticketId,
        actionDate: getActionDate(action)?.toISOString?.() || null,
        author: getActionAuthorName(action),
        text,
      })
    )
    .digest("hex");

  return `source:${ticketId}:${hash}`;
}

function isProcessingOrProcessed(processingKey) {
  if (activeProcessingKeys.has(processingKey)) return true;

  const state = loadProcessedState();
  return Boolean(state.processedKeys[processingKey]);
}

function markProcessing(processingKey) {
  activeProcessingKeys.add(processingKey);
}

function unmarkProcessing(processingKey) {
  activeProcessingKeys.delete(processingKey);
}

function markProcessed(processingKey, metadata = {}) {
  const state = loadProcessedState();
  state.processedKeys[processingKey] = {
    processedAt: new Date().toISOString(),
    ...metadata,
  };

  if (metadata?.sourceFingerprint) {
    state.processedSourceFingerprints[metadata.sourceFingerprint] = {
      processedAt: new Date().toISOString(),
      ticketId: metadata.ticketId,
      status: metadata.status,
    };
  }

  saveProcessedState(state);
}

function hasProcessedSourceFingerprint(sourceFingerprint) {
  const state = loadProcessedState();
  return Boolean(state.processedSourceFingerprints[sourceFingerprint]);
}

// =====================================================================================
// HELPERS
// =====================================================================================

function extractTicketId(body = {}) {
  const candidates = [
    body?.ticket?.id,
    body?.ticket?.Id,
    body?.ticketId,
    body?.TicketId,
    body?.id,
    body?.Id,
    body?.data?.ticket?.id,
    body?.data?.ticketId,
    body?.object?.id,
    body?.entity?.id,
  ];

  for (const value of candidates) {
    const num = Number(value);
    if (num) return num;
  }

  return null;
}

function extractEventDate(body = {}) {
  const candidates = [
    body?.eventDate,
    body?.date,
    body?.createdDate,
    body?.triggerDate,
    body?.updatedDate,
    body?.data?.eventDate,
    body?.data?.date,
  ];

  for (const value of candidates) {
    const dt = value ? new Date(value) : null;
    if (dt && !Number.isNaN(dt.getTime())) return dt;
  }

  return null;
}

function parseJsonSafe(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function markdownLikeToHtml(text = "") {
  let html = String(text || "").trim();

  html = html
    .replace(/\r/g, "")
    .replace(/\n/g, "<br>")
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");

  return html;
}