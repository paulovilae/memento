# Memento Palace Memory

`Memento` now supports a verbatim memory layout inspired by the useful retrieval patterns in MemPalace, without copying its benchmark claims or storage stack.

## Goal

Keep raw interaction memory findable.

Instead of reducing every event into a fragile summary, `Memento` can store full records with stable scope metadata:

- `wing`: app or broad domain
- `hall`: subsystem or workflow class
- `room`: concrete case, thread, negotiation, or incident

## Why This Exists

This helps with:

- project-specific recall
- contract or policy negotiations
- debugging histories
- user support threads
- reconstructing a session timeline

## IPC Actions

### `save_memory_record`

Stores one scoped memory record.

Useful optional fields:

- `wing`
- `hall`
- `room`
- `entry_title`
- `tags`
- `confidence`
- `content_json`

If `entry_title` is omitted, `Memento` derives one from the first line of `content`.

### `query_memory_records`

Returns raw records using exact filters such as:

- `user_id`
- `tenant_id`
- `app_id`
- `session_id`
- `scope`
- `wing`
- `hall`
- `room`
- `memory_type`

### `search_memory_records`

Runs lightweight scoring over:

- `entry_title`
- `content`
- `memory_type`
- `wing`
- `hall`
- `room`
- `tags`

This is intentionally deterministic and local. It does not call Hera and does not require embeddings.

### `get_memory_timeline`

Returns chronological entries for a filtered room/session so callers can rebuild the verbatim chain of events.

## Design Notes

- Storage stays in `scoped_memory`; no new service was introduced.
- Global reads remain forbidden. Callers must provide at least one scoping filter.
- This is a retrieval and organization improvement, not a claim that metadata beats semantic search universally.
- For long structured files, continue using `page_tree` document indexes.
