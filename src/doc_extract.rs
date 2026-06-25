//! Conversor de documentos → texto, **propiedad de Memento** (capa de ingesta de conocimiento;
//! ver AVA_BUNDLE_CAPABILITIES_MATRIX: document indexing/retrieval = Memento).
//!
//! REGLA DE PLATAFORMA (verificada 2026-06-25): sobre el IPC se manda la **UBICACIÓN del archivo,
//! no el archivo**. El socket de Memento lee un solo chunk de 32 KB por mensaje, así que mandar los
//! bytes (base64) de un PDF/Word lo truncaría. El llamador deja el archivo en un directorio
//! compartido y manda el `path`; Memento lo lee y extrae el texto.
//!
//! Seguridad: el `path` debe quedar bajo `MEMENTO_EXTRACT_BASE` (default `/tmp/os-doc-extract`) —
//! se canonicaliza y se verifica el prefijo para evitar que un llamador haga leer `/etc/passwd`.

use serde_json::{json, Value};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Directorio raíz permitido para extracción (los apps dejan ahí el archivo subido).
fn extract_base() -> String {
    std::env::var("MEMENTO_EXTRACT_BASE").unwrap_or_else(|_| "/tmp/os-doc-extract".to_string())
}

/// Acción IPC `extract_text`: `{ path }` → `{ ok, text, source_type, char_count }`.
pub async fn extract_text(payload: Value) -> Value {
    let path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if path.is_empty() {
        return json!({ "error": "extract_text: falta 'path'" });
    }

    // Anti path-traversal: canonicalizar y exigir que esté bajo la base permitida.
    let canon = match std::fs::canonicalize(&path) {
        Ok(p) => p,
        Err(e) => return json!({ "error": format!("extract_text: no se pudo abrir '{path}': {e}") }),
    };
    let base = extract_base();
    let base_canon = std::fs::canonicalize(&base).unwrap_or_else(|_| PathBuf::from(&base));
    if !canon.starts_with(&base_canon) {
        return json!({ "error": "extract_text: path fuera del directorio permitido" });
    }

    let ext = canon
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();

    // La extracción es CPU/IO bloqueante → fuera del executor async.
    match tokio::task::spawn_blocking(move || extract_by_ext(&canon, &ext)).await {
        Ok(Ok((text, source_type))) => {
            let char_count = text.chars().count();
            json!({ "ok": true, "text": text, "source_type": source_type, "char_count": char_count })
        }
        Ok(Err(e)) => json!({ "error": format!("extract_text: {e}") }),
        Err(e) => json!({ "error": format!("extract_text join: {e}") }),
    }
}

/// Despacha por extensión al extractor correcto. Devuelve (texto, source_type).
fn extract_by_ext(path: &Path, ext: &str) -> anyhow::Result<(String, String)> {
    match ext {
        "pdf" => Ok((pdf_extract::extract_text(path)?, "pdf".into())),
        "docx" => Ok((extract_docx(path)?, "docx".into())),
        "pptx" => Ok((extract_pptx(path)?, "pptx".into())),
        "xlsx" | "xls" | "xlsm" => Ok((extract_xlsx(path)?, "xlsx".into())),
        "txt" | "md" | "markdown" | "csv" | "json" => {
            Ok((std::fs::read_to_string(path)?, "text".into()))
        }
        other => anyhow::bail!("formato no soportado: .{other}"),
    }
}

/// Word: el texto vive en `word/document.xml` dentro de los nodos `<w:t>` (local name `t`).
fn extract_docx(path: &Path) -> anyhow::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut xml = String::new();
    archive.by_name("word/document.xml")?.read_to_string(&mut xml)?;
    Ok(collect_xml_text(&xml, "t"))
}

/// PowerPoint: cada diapositiva es `ppt/slides/slideN.xml`; el texto está en `<a:t>` (local `t`).
fn extract_pptx(path: &Path) -> anyhow::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut slides: Vec<String> = (0..archive.len())
        .filter_map(|i| archive.by_index(i).ok().map(|f| f.name().to_string()))
        .filter(|n| n.starts_with("ppt/slides/slide") && n.ends_with(".xml"))
        .collect();
    slides.sort();
    let mut text = String::new();
    for name in slides {
        let mut xml = String::new();
        archive.by_name(&name)?.read_to_string(&mut xml)?;
        text.push_str(&collect_xml_text(&xml, "t"));
        text.push('\n');
    }
    Ok(text)
}

/// Excel: todas las celdas de todas las hojas, fila por fila (vía calamine).
fn extract_xlsx(path: &Path) -> anyhow::Result<String> {
    use calamine::{open_workbook_auto, Data, Reader};
    let mut workbook = open_workbook_auto(path)?;
    let mut text = String::new();
    for sheet in workbook.sheet_names().to_owned() {
        if let Ok(range) = workbook.worksheet_range(&sheet) {
            for row in range.rows() {
                for cell in row {
                    match cell {
                        Data::String(s) => {
                            text.push_str(s);
                            text.push(' ');
                        }
                        Data::Float(f) => {
                            text.push_str(&f.to_string());
                            text.push(' ');
                        }
                        Data::Int(i) => {
                            text.push_str(&i.to_string());
                            text.push(' ');
                        }
                        Data::Bool(b) => {
                            text.push_str(&b.to_string());
                            text.push(' ');
                        }
                        Data::DateTime(d) => {
                            text.push_str(&d.to_string());
                            text.push(' ');
                        }
                        _ => {}
                    }
                }
                text.push('\n');
            }
        }
    }
    Ok(text)
}

/// Recorre el XML y concatena el texto de los nodos cuyo nombre local sea `tag` (ej. `t`).
/// roxmltree devuelve el nombre local sin prefijo de namespace, así que sirve para `w:t` y `a:t`.
fn collect_xml_text(xml: &str, tag: &str) -> String {
    let mut out = String::new();
    if let Ok(doc) = roxmltree::Document::parse(xml) {
        for node in doc.descendants() {
            if node.tag_name().name() == tag {
                if let Some(t) = node.text() {
                    out.push_str(t);
                    out.push(' ');
                }
            }
        }
    }
    out
}
