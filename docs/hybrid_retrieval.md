# Memento Hybrid Retrieval

`Memento` now supports a hybrid retrieval model for documents:

- `vector` or semantic retrieval remains appropriate for broad, unstructured recall
- `page_tree` retrieval is now available for long, structured documents

## Why

Traditional chunk-and-vector retrieval degrades on:

- legal and compliance documents
- manuals with deep section structure
- policies with nested headings
- contracts with internal references and appendices

For those cases, `Memento` can now store a **hierarchical document index** and retrieve relevant nodes without relying on embeddings alone.

## Retrieval Strategies

### `page_tree`

Use for:

- policies
- contracts
- reports
- manuals
- compliance packs

Each indexed document stores:

- document metadata
- root node id
- hierarchical nodes
- summaries per node
- source references / page spans
- tags

### `vector`

Keep using for:

- chat history
- messy notes
- broad fuzzy recall
- cross-document semantic lookup across unstructured content

## IPC Actions

### `upsert_document_index`

Stores or replaces a document index.

```json
{
  "action": "upsert_document_index",
  "payload": {
    "document_id": "vetra-policy-001",
    "tenant_id": "tenant-main",
    "app_id": "vetra",
    "owner_scope": "workspace",
    "title": "Remote Work Policy",
    "summary": "Policy covering remote work expectations and approvals.",
    "index_type": "page_tree",
    "source_type": "policy",
    "source_uri": "/docs/policies/remote-work.pdf",
    "root_node_id": "root",
    "status": "active",
    "nodes": [
      {
        "node_id": "root",
        "title": "Remote Work Policy",
        "summary": "Top-level summary",
        "level": 0,
        "node_type": "document",
        "page_from": 1,
        "page_to": 8
      }
    ]
  }
}
```

### `get_document_index`

Returns the full index and all stored nodes.

### `list_document_indexes`

Returns indexed documents filtered by:

- `app_id`
- `tenant_id`
- `index_type`

### `query_document_index`

Runs lightweight symbolic node scoring over `page_tree` indexes.

Current implementation:

- token overlap against title, summary, and tags
- shallow preference for higher-level nodes
- returns evidence-oriented node results

Future implementation:

- Hera-assisted reasoning traversal over the tree
- hybrid multi-document path selection
- section-following via source references and cross-node links

## Recommended Architecture

- `Memento` stores the canonical document index and retrieval metadata
- `Hera` can later use `query_document_index` results for deeper reasoning
- apps choose retrieval strategy per document class instead of forcing one global method

## Current Limitation

`query_document_index` is currently symbolic, not LLM-traversed.

That is intentional:

- `Memento` remains fast and deterministic
- `Hera` can add higher-order reasoning later
- the stored `page_tree` structure is already compatible with that future step
