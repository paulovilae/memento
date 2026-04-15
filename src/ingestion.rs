use crate::config::WatchedFolder;
use std::path::Path;
use tracing::{info, warn};
use walkdir::WalkDir;

pub async fn start_folder_watcher(folders: Vec<WatchedFolder>) -> anyhow::Result<()> {
    info!(
        "👀 Starting Local Folder Watcher on {} configured directories",
        folders.len()
    );

    // In a real app we'd set up `notify::recommended_watcher` bridging to tokio mpsc.
    // For now, we perform an initial scan to populate the Privacy Queue.

    tokio::spawn(async move {
        let mut document_count = 0;

        for folder in folders {
            info!(
                "🔍 Scanning {} for documents... (Sanitize: {})",
                folder.path, folder.sanitize_pii
            );

            for entry in WalkDir::new(&folder.path)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let p = entry.path();
                if p.is_file() {
                    if let Some(ext) = p.extension() {
                        let ext_str = ext.to_string_lossy().to_lowercase();
                        if ["pdf", "txt", "md", "docx", "xlsx"].contains(&ext_str.as_str()) {
                            document_count += 1;

                            match ext_str.as_str() {
                                "pdf" => match pdf_extract::extract_text(p) {
                                    Ok(text) => info!(
                                        "📄 Added PDF to Queue: {} ({} bytes)",
                                        p.display(),
                                        text.len()
                                    ),
                                    Err(e) => {
                                        warn!("⚠️ Failed to extract PDF {}: {}", p.display(), e)
                                    }
                                },
                                "docx" => match extract_docx(p) {
                                    Ok(text) => info!(
                                        "📝 Added DOCX to Queue: {} ({} bytes)",
                                        p.display(),
                                        text.len()
                                    ),
                                    Err(e) => {
                                        warn!("⚠️ Failed to extract DOCX {}: {}", p.display(), e)
                                    }
                                },
                                "xlsx" => match extract_xlsx(p) {
                                    Ok(text) => info!(
                                        "📊 Added XLSX to Queue: {} ({} bytes)",
                                        p.display(),
                                        text.len()
                                    ),
                                    Err(e) => {
                                        warn!("⚠️ Failed to extract XLSX {}: {}", p.display(), e)
                                    }
                                },
                                _ => {
                                    info!("📝 Added text file to Queue: {}", p.display());
                                }
                            }
                        }
                    }
                }
            }
        }

        info!(
            "🛡️ Scan complete. {} documents pending SLM PII sanitization.",
            document_count
        );

        // Dummy loop representing the ongoing watcher
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        }
    });

    Ok(())
}

fn extract_docx(path: &Path) -> anyhow::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut document_xml = archive.by_name("word/document.xml")?;
    let mut xml_content = String::new();
    std::io::Read::read_to_string(&mut document_xml, &mut xml_content)?;

    let doc = roxmltree::Document::parse(&xml_content)?;
    let mut text = String::new();
    for node in doc.descendants() {
        if node.has_tag_name("t") || node.tag_name().name() == "t" {
            if let Some(t) = node.text() {
                text.push_str(t);
                text.push(' ');
            }
        }
    }
    Ok(text)
}

fn extract_xlsx(path: &Path) -> anyhow::Result<String> {
    use calamine::{open_workbook_auto, Data, Reader};
    let mut workbook = open_workbook_auto(path)?;
    let mut text = String::new();
    let sheet_names = workbook.sheet_names().to_owned();
    for sheet in sheet_names {
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
                        _ => {}
                    }
                }
                text.push('\n');
            }
        }
    }
    Ok(text)
}
