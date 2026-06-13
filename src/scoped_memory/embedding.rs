//! Embedding helpers for semantic recall: cosine similarity plus the two decode
//! paths for the stored vector (JSON TEXT column and the fast little-endian BYTEA
//! column). Extracted from `mod.rs` to keep that file under the size guard.

/// Cosine similarity between two equal-length f32 vectors. Vectors from Hera's
/// embed action arrive L2-normalized, so this reduces to a dot product, but we
/// compute the full cosine for robustness against unnormalized callers.
pub(super) fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

pub(super) fn parse_embedding(raw: &str) -> Option<Vec<f32>> {
    let parsed: Vec<f32> = serde_json::from_str(raw).ok()?;
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

/// Fast-path decode of the BYTEA embedding column: raw little-endian f32, 4 bytes each.
/// Returns None for empty/misaligned bytes so the caller can fall back to the TEXT column.
pub(super) fn unpack_embedding(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }
    Some(
        bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

#[cfg(test)]
mod embedding_bytea_tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let v = vec![1.0f32, -0.5, 0.25, 0.0, 123.456];
        let bytes = super::super::parsing::pack_embedding(&v);
        assert_eq!(bytes.len(), v.len() * 4);
        assert_eq!(unpack_embedding(&bytes).expect("roundtrip"), v);
    }

    #[test]
    fn unpack_rejects_bad_input() {
        assert!(unpack_embedding(&[]).is_none());
        assert!(unpack_embedding(&[1, 2, 3]).is_none()); // not a multiple of 4
    }
}
