# 🧪 Memento Prompt-Based Tests

Copy-paste these prompts into any MCP-compatible AI agent (Antigravity, Gemini CLI, etc.)
to validate Memento's capabilities through natural language.

Each test has an **input prompt** and **expected behavior**.

---

## Test 1: Store and Recall Memory

**Prompt:**
> Store a memory with key `bayesian_test` containing "Bayesian inference updates prior
> beliefs into posterior distributions using observed evidence from user interactions"
> with tags `ai,math,bayesian`

**Expected:** Success confirmation with key `bayesian_test`.

**Follow-up:**
> Retrieve the memory with key `bayesian_test`

**Expected:** Full content returned, matching exactly what was stored. Tags should be `ai,math,bayesian`.

---

## Test 2: Search by Keyword

**Prompt:**
> Search all memories for "Bayesian"

**Expected:** Returns at least 1 result. The `bayesian_test` entry should appear with a snippet.

---

## Test 3: List All Memories

**Prompt:**
> List all stored memories

**Expected:** Returns a complete index including `bayesian_test`. Each entry shows key, title (first 80 chars), tags, character count, and last updated timestamp.

---

## Test 4: Update (Upsert) Existing Memory

**Prompt:**
> Store a memory with key `bayesian_test` containing "UPDATED: Bayesian teaching trains
> LLMs to mimic optimal Bayesian assistants, enabling cross-domain probabilistic reasoning"
> with tags `ai,bayesian,teaching,updated`

**Expected:** Success — the existing entry is **updated**, not duplicated.

**Follow-up:**
> Retrieve the memory with key `bayesian_test`

**Expected:** Content should show `UPDATED:...` version. Tags should be `ai,bayesian,teaching,updated`.

---

## Test 5: Delete Memory

**Prompt:**
> Delete the memory with key `bayesian_test`

**Expected:** Success with action `deleted`.

**Follow-up:**
> Retrieve the memory with key `bayesian_test`

**Expected:** Status `not_found`.

---

## Test 6: App Registry (requires app databases running)

**Prompt:**
> List all registered ImagineOS apps

**Expected:** Returns a list of apps (may include Movilo, Vetra, Latinos, etc.) with their slugs, names, descriptions, and key tables.

---

## Test 7: Cross-App Query (requires Movilo DB running)

**Prompt:**
> Query the Movilo database with: `SELECT COUNT(*) as total FROM providers`

**Expected:** Returns an integer count of providers. If DB is not connected, should return an error about the app not being found or connection failure.

---

## Test 8: Error Handling

**Prompt:**
> Retrieve the memory with key `this_key_absolutely_does_not_exist_12345`

**Expected:** Status `not_found` with a clear error message.

---

## Test 9: Large Content Storage

**Prompt:**
> Store a memory with key `large_content_test` containing a summary of the
> entire ImagineOS architecture, including Hera (AI engine), Memento (memory),
> Sentinel (proxy), Imaginclaw (messaging), Argus (orchestration), and all
> applications (Movilo, Vetra, Latinos, Garcero). Include details about the
> UDS IPC protocol, shared memory architecture, and sovereign-first design.
> Tag it with `architecture,os,comprehensive`.

**Expected:** Success. The content should be stored regardless of length (up to ~32KB).

**Follow-up:**
> Retrieve the memory with key `large_content_test` and tell me the character count

**Expected:** Returns the full content with accurate `char_count`.

**Cleanup:**
> Delete the memory with key `large_content_test`

---

## Test 10: Chat Memory (via direct IPC, not MCP)

This test validates the chat memory subsystem. Use a tool that can send raw IPC
messages to `/tmp/memento.sock`, or use `socat`:

```bash
echo '{"action":"save_memory","payload":{"chat_id":"prompt-test","role":"user","content":"Testing from prompts"}}' \
  | socat - UNIX-CONNECT:/tmp/memento.sock
```

**Expected:** `{"status":"success"}`

```bash
echo '{"action":"get_context","payload":{"chat_id":"prompt-test","limit":5}}' \
  | socat - UNIX-CONNECT:/tmp/memento.sock
```

**Expected:** Returns the saved message in the `messages` array.
