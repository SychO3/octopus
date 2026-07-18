#!/usr/bin/env node

import crypto from "node:crypto";
import { execFileSync } from "node:child_process";

const baseURL = process.env.OCTOPUS_TEST_URL || "http://127.0.0.1:18081";
const dbPath = process.env.OCTOPUS_TEST_DB || "/root/octopus-app/data/data.db";
const groupID = Number(process.env.OCTOPUS_TEST_GROUP_ID || 19);
const sourceGroupID = Number(process.env.OCTOPUS_TEST_SOURCE_GROUP_ID || 0);
const requestedModel = process.env.OCTOPUS_TEST_MODEL || "protocol-cross-test";
const onlyChannel = Number(process.env.OCTOPUS_TEST_CHANNEL_ID || 0);
const casePattern = process.env.OCTOPUS_TEST_CASE ? new RegExp(process.env.OCTOPUS_TEST_CASE) : null;

function sql(query) {
  const out = execFileSync("sqlite3", ["-json", dbPath, query], { encoding: "utf8" });
  return out.trim() ? JSON.parse(out) : [];
}

function base64url(value) {
  return Buffer.from(value).toString("base64url");
}

function adminToken() {
  const [{ value: secret }] = sql("SELECT value FROM settings WHERE key='jwt_secret'");
  const now = Math.floor(Date.now() / 1000);
  const header = base64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = base64url(JSON.stringify({ iss: "octopus", iat: now, nbf: now, exp: now + 3600 }));
  const signature = crypto.createHmac("sha256", secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

function loadFixture() {
  const items = sql(`
    SELECT gi.id, gi.channel_id, c.name AS channel_name, c.type, gi.model_name,
           gi.priority, gi.weight
      FROM group_items gi JOIN channels c ON c.id=gi.channel_id
     WHERE gi.group_id=${groupID}
     ORDER BY gi.priority, gi.id
  `);
  const keys = sql(`SELECT api_key FROM api_keys WHERE name='protocol-cross-test' AND enabled=1 ORDER BY id DESC LIMIT 1`);
  if (!items.length) throw new Error(`group ${groupID} has no items`);
  if (!keys.length) throw new Error("protocol-cross-test API key not found");
  return { items, apiKey: keys[0].api_key };
}

async function syncFixture(token) {
  if (!sourceGroupID) return;
  const sourceItems = sql(`SELECT channel_id, model_name, priority, weight FROM group_items WHERE group_id=${sourceGroupID} ORDER BY priority, id`);
  const currentItems = sql(`SELECT id FROM group_items WHERE group_id=${groupID}`);
  if (!sourceItems.length) throw new Error(`source group ${sourceGroupID} has no items`);
  const response = await fetch(`${baseURL}/api/v1/group/update`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({
      id: groupID,
      items_to_delete: currentItems.map((item) => item.id),
      items_to_add: sourceItems,
    }),
  });
  if (!response.ok) throw new Error(`fixture sync failed: ${response.status} ${await response.text()}`);
}

async function updatePriorities(items, targetChannelID, token) {
  const body = {
    id: groupID,
    items_to_update: items.map((item) => ({
      id: item.id,
      priority: targetChannelID ? (item.channel_id === targetChannelID ? 1 : 100 + item.priority) : item.priority,
      weight: item.weight,
    })),
  };
  const response = await fetch(`${baseURL}/api/v1/group/update`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`group update failed: ${response.status} ${await response.text()}`);
}

const tool = {
  type: "function",
  function: {
    name: "lookup_temperature",
    description: "Look up a temperature",
    parameters: { type: "object", properties: { city: { type: "string" } }, required: ["city"] },
  },
};
const anthropicTool = {
  name: "lookup_temperature",
  description: "Look up a temperature",
  input_schema: { type: "object", properties: { city: { type: "string" } }, required: ["city"] },
};
const responsesTool = {
  type: "function",
  name: "lookup_temperature",
  description: "Look up a temperature",
  parameters: { type: "object", properties: { city: { type: "string" } }, required: ["city"] },
};

function cases() {
  const text = (id) => `Reply with exactly: ${id} OK`;
  const toolPrompt = (id) => `Use lookup_temperature for Paris. Case marker: ${id}`;
  const resultPrompt = (id) => `The tool returned 21 C. Reply with exactly: ${id} RESULT_OK`;
  return [
    ...[false, true].flatMap((stream) => [
      { name: `chat_text_${stream ? "stream" : "json"}`, protocol: "chat", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 100, messages: [{ role: "user", content: text(id) }] }), expect: stream ? ["data:", "[DONE]"] : ["choices"] },
      { name: `chat_tool_call_${stream ? "stream" : "json"}`, protocol: "chat", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 160, messages: [{ role: "user", content: toolPrompt(id) }], tools: [tool], tool_choice: { type: "function", function: { name: "lookup_temperature" } } }), expect: ["tool_calls"] },
      { name: `chat_tool_result_${stream ? "stream" : "json"}`, protocol: "chat", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 100, messages: [{ role: "user", content: toolPrompt(id) }, { role: "assistant", content: null, tool_calls: [{ id: `call_${id}`, type: "function", function: { name: "lookup_temperature", arguments: '{"city":"Paris"}' } }] }, { role: "tool", tool_call_id: `call_${id}`, content: resultPrompt(id) }] }), expect: [stream ? "[DONE]" : "RESULT_OK"] },
    ]),
    ...[false, true].flatMap((stream) => [
      { name: `responses_text_${stream ? "stream" : "json"}`, protocol: "responses", stream, body: (id) => ({ model: requestedModel, stream, max_output_tokens: 100, input: text(id) }), expect: stream ? ["response.completed"] : ["output"] },
      { name: `responses_tool_call_${stream ? "stream" : "json"}`, protocol: "responses", stream, body: (id) => ({ model: requestedModel, stream, max_output_tokens: 160, input: toolPrompt(id), tools: [responsesTool], tool_choice: "required" }), expect: ["function_call"] },
      { name: `responses_tool_result_${stream ? "stream" : "json"}`, protocol: "responses", stream, body: (id) => ({ model: requestedModel, stream, max_output_tokens: 100, input: [{ type: "message", role: "user", content: [{ type: "input_text", text: toolPrompt(id) }] }, { type: "function_call", call_id: `call_${id}`, name: "lookup_temperature", arguments: '{"city":"Paris"}' }, { type: "function_call_output", call_id: `call_${id}`, output: resultPrompt(id) }] }), expect: ["RESULT_OK"] },
    ]),
    ...[false, true].flatMap((stream) => [
      { name: `anthropic_text_${stream ? "stream" : "json"}`, protocol: "anthropic", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 100, messages: [{ role: "user", content: text(id) }] }), expect: stream ? ["message_start", "message_stop"] : ["content"] },
      { name: `anthropic_tool_call_${stream ? "stream" : "json"}`, protocol: "anthropic", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 160, messages: [{ role: "user", content: toolPrompt(id) }], tools: [anthropicTool], tool_choice: { type: "tool", name: "lookup_temperature" } }), expect: ["tool_use"] },
      { name: `anthropic_tool_result_${stream ? "stream" : "json"}`, protocol: "anthropic", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 100, messages: [{ role: "user", content: toolPrompt(id) }, { role: "assistant", content: [{ type: "tool_use", id: `toolu_${id}`, name: "lookup_temperature", input: { city: "Paris" } }] }, { role: "user", content: [{ type: "tool_result", tool_use_id: `toolu_${id}`, content: resultPrompt(id) }] }] }), expect: [stream ? "message_stop" : "RESULT_OK"] },
      { name: `anthropic_thinking_${stream ? "stream" : "json"}`, protocol: "anthropic", stream, body: (id) => ({ model: requestedModel, stream, max_tokens: 1200, thinking: { type: "enabled", budget_tokens: 1024 }, messages: [{ role: "user", content: `Calculate 137*149 carefully, then reply ${id} OK` }] }), expect: stream ? ["thinking_delta", "signature_delta"] : ["thinking", "signature"] },
    ]),
  ];
}

function endpoint(test) {
  return test.protocol === "chat" ? "/v1/chat/completions" : test.protocol === "responses" ? "/v1/responses" : "/v1/messages";
}

function responseShapeOK(test, status, responseText, caseID) {
  if (status !== 200 || !test.expect.every((marker) => responseText.includes(marker))) return false;
  if (test.stream && test.protocol === "chat" && !responseText.includes("[DONE]")) return false;
  if (test.protocol === "anthropic") {
    if (!test.stream) {
      try {
        const response = JSON.parse(responseText);
        return response.type === "message" && Array.isArray(response.content) &&
          Number.isFinite(response.usage?.input_tokens) && Number.isFinite(response.usage?.output_tokens);
      } catch {
        return false;
      }
    }
    const events = responseText.split("\n").filter((line) => line.startsWith("data:")).map((line) => {
      try { return JSON.parse(line.slice(5).trim()); } catch { return null; }
    }).filter(Boolean);
    const start = events.find((event) => event.type === "message_start");
    const delta = events.find((event) => event.type === "message_delta");
    const messageEvents = events.filter((event) => event.type !== "ping");
    const startIndex = messageEvents.findIndex((event) => event.type === "message_start");
    const stopIndex = messageEvents.findIndex((event) => event.type === "message_stop");
    const startUsage = start?.message?.usage;
    const effectiveInput = (startUsage?.input_tokens || 0) + (startUsage?.cache_read_input_tokens || 0) + (startUsage?.cache_creation_input_tokens || 0);
    if (!start || !delta || startIndex !== 0 || stopIndex <= startIndex || !(effectiveInput > 0)) return false;
    if (!Number.isFinite(delta.usage?.output_tokens)) return false;
    if (test.name.includes("tool_call") && delta.delta?.stop_reason !== "tool_use") return false;
    if (test.name.includes("thinking") && (!responseText.includes("thinking_delta") || !responseText.includes("signature_delta"))) return false;
  }
  if (!test.stream || test.name.includes("tool_call")) return true;
  const chunks = responseText.split("\n").filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim());
  let content = "";
  for (const chunk of chunks) {
    if (chunk === "[DONE]") continue;
    try {
      const event = JSON.parse(chunk);
      content += event.choices?.[0]?.delta?.content || (typeof event.delta === "string" ? event.delta : event.delta?.text) || "";
      content += event.response?.output?.flatMap((item) => item.content || []).map((item) => item.text || "").join("") || "";
    } catch {}
  }
  return test.name.includes("thinking") || content.includes(caseID) || responseText.includes(caseID);
}

async function findLog(afterID, caseID) {
  for (let attempt = 0; attempt < 12; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, attempt ? 500 : 1100));
    const rows = sql(`SELECT id,success,channel_id,channel_name,error,attempts,total_attempts,substr(request_content,1,4000) AS request_content FROM relay_logs WHERE id>${afterID} ORDER BY id DESC LIMIT 300`);
    const found = rows.find((row) => row.request_content?.includes(caseID));
    if (found) return found;
  }
  return null;
}

async function runCase(test, channel, apiKey) {
  const caseID = `pcx-${channel.channel_id}-${test.name}-${Date.now().toString(36)}`;
  const [{ id: afterID }] = sql("SELECT CAST(COALESCE(MAX(id),0) AS TEXT) AS id FROM relay_logs");
  const headers = { "content-type": "application/json" };
  if (test.protocol === "anthropic") {
    headers["x-api-key"] = apiKey;
    headers["anthropic-version"] = "2023-06-01";
  } else {
    headers.authorization = `Bearer ${apiKey}`;
  }
  const started = Date.now();
  let status = 0;
  let responseText = "";
  let requestError = "";
  try {
    const response = await fetch(`${baseURL}${endpoint(test)}`, { method: "POST", headers, body: JSON.stringify(test.body(caseID)) });
    status = response.status;
    responseText = await response.text();
  } catch (error) {
    requestError = String(error);
  }
  const log = await findLog(afterID, caseID);
  const attempts = log?.attempts ? JSON.parse(log.attempts) : [];
  const targetAttempt = attempts.find((item) => item.channel_id === channel.channel_id);
  const shapeOK = responseShapeOK(test, status, responseText, caseID);
  const targetOK = targetAttempt?.status === "success";
  return {
    case_id: caseID,
    channel_id: channel.channel_id,
    channel_name: channel.channel_name,
    upstream_model: channel.model_name,
    case: test.name,
    protocol: test.protocol,
    stream: test.stream,
    http_status: status,
    duration_ms: Date.now() - started,
    response_shape_ok: shapeOK,
    target_status: targetAttempt?.status || "missing",
    target_error: targetAttempt?.msg || requestError,
    fallback: attempts.some((item) => item.channel_id !== channel.channel_id && item.status === "success"),
    final_log_success: Boolean(log?.success),
    passed: shapeOK && targetOK,
    response_preview: responseText.slice(0, 500),
    response_tail: responseText.slice(-500),
  };
}

async function main() {
  const token = adminToken();
  await syncFixture(token);
  const { items, apiKey } = loadFixture();
  const selected = onlyChannel ? items.filter((item) => item.channel_id === onlyChannel) : items;
  if (!selected.length) throw new Error(`channel ${onlyChannel} is not in group ${groupID}`);
  const results = [];
  try {
    for (const channel of selected) {
      await updatePriorities(items, channel.channel_id, token);
      for (const test of cases().filter((item) => !casePattern || casePattern.test(item.name))) {
        const result = await runCase(test, channel, apiKey);
        results.push(result);
        process.stdout.write(`${JSON.stringify(result)}\n`);
      }
    }
  } finally {
    await updatePriorities(items, 0, token);
  }
  const passed = results.filter((item) => item.passed).length;
  process.stderr.write(`completed ${results.length} cases: ${passed} passed, ${results.length - passed} failed\n`);
  if (passed !== results.length) process.exitCode = 1;
}

await main();
