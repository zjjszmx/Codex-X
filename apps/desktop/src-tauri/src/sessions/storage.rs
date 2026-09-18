use super::global_state::normalize_workspace_path;
use super::index::{load_rollout_index, persist_rollout_index, system_time_ns, RolloutIndexEntry};
use super::types::{
    RolloutScan, SessionFileChange, SessionPage, SessionPageCursor, SessionPreview, SqliteScan,
};
use crate::error::{CodexxError, Result};
use crate::file_io::{
    atomic_write, io_err, json_err, parse_toml_document, read_to_string_if_exists,
};
use crate::paths::home_dir;
use crate::sqlite_utils::{sql_select_column, sqlite_has_table, table_column_set};
use crate::{config_path, string_value};
use rusqlite::{
    types::{Value as SqlValue, ValueRef},
    Connection, OpenFlags, Row,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use toml_edit::DocumentMut;

const ROLLOUT_FULL_READ_LIMIT_BYTES: u64 = 32 * 1024 * 1024;
const ROLLOUT_METADATA_PREFIX_LIMIT_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
struct RolloutHeaderMetadata {
    session_id: String,
    title: Option<String>,
    cwd: Option<String>,
    model_provider: Option<String>,
    is_subagent: bool,
    parent_session_id: Option<String>,
}

fn rollout_header_metadata(path: &Path) -> Result<RolloutHeaderMetadata> {
    let file = fs::File::open(path).map_err(|error| io_err(path, error))?;
    let mut prefix = Vec::new();
    file.take(ROLLOUT_METADATA_PREFIX_LIMIT_BYTES)
        .read_to_end(&mut prefix)
        .map_err(|error| io_err(path, error))?;
    let complete_len = if prefix.last() == Some(&b'\n') {
        prefix.len()
    } else {
        prefix
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or_default()
    };
    for raw_line in prefix[..complete_len].split(|byte| *byte == b'\n') {
        let raw_line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if raw_line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_slice::<Value>(raw_line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        let Some(payload) = record.get("payload").and_then(Value::as_object) else {
            continue;
        };
        let Some(session_id) = payload
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let title = payload
            .get("title")
            .or_else(|| payload.get("first_user_message"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let cwd = payload
            .get("cwd")
            .and_then(Value::as_str)
            .and_then(normalize_workspace_path);
        let model_provider = payload
            .get("model_provider")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let source = payload.get("source");
        let is_subagent = source.is_some_and(source_value_is_subagent);
        let parent_session_id = source
            .and_then(|value| value.pointer("/subagent/thread_spawn/parent_thread_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        return Ok(RolloutHeaderMetadata {
            session_id: session_id.to_string(),
            title,
            cwd,
            model_provider,
            is_subagent,
            parent_session_id,
        });
    }
    Err(CodexxError::Config(format!(
        "Large session file has no readable session_meta in its first {} KiB: {}",
        ROLLOUT_METADATA_PREFIX_LIMIT_BYTES / 1024,
        path.display()
    )))
}

pub(super) fn current_model_provider(codex_dir: &Path, explicit: Option<String>) -> Result<String> {
    if let Some(provider) = explicit
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return Ok(provider);
    }
    let cfg = config_path(codex_dir);
    let text = read_to_string_if_exists(&cfg)?;
    let doc = parse_toml_document(&cfg, &text)?;
    Ok(string_value(&doc, "model_provider").unwrap_or_else(|| "openai".to_string()))
}

fn is_rollout_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
}

pub(super) fn rollout_filename_matches_id(path: &Path, id: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.ends_with(&format!("-{id}.jsonl")) || name.ends_with(&format!("-{id}.jsonl.zst"))
        })
}

pub(super) fn canonical_rollout_storage_roots(codex_dir: &Path) -> Vec<PathBuf> {
    [
        codex_dir.join("sessions"),
        codex_dir.join("archived_sessions"),
    ]
    .into_iter()
    .filter_map(|root| root.canonicalize().ok())
    .collect()
}

pub(super) fn is_canonical_rollout_storage_path(codex_dir: &Path, path: &Path) -> bool {
    canonical_rollout_storage_roots(codex_dir)
        .iter()
        .any(|root| path.starts_with(root))
}

fn referenced_rollout_paths(
    codex_dir: &Path,
    rollout_paths_by_thread_id: &HashMap<String, String>,
    include_archived_storage: bool,
    failures: &mut Vec<String>,
    warnings: &mut Vec<String>,
) -> HashMap<PathBuf, HashSet<String>> {
    let mut referenced = HashMap::<PathBuf, HashSet<String>>::new();
    for (thread_id, value) in rollout_paths_by_thread_id {
        let raw = PathBuf::from(value.trim());
        let path = if raw.is_absolute() {
            raw
        } else {
            codex_dir.join(raw)
        };
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                warnings.push(format!(
                    "已忽略活动 SQLite 中的旧会话引用（文件不存在）: {}",
                    path.display()
                ));
                continue;
            }
            Err(error) => {
                failures.push(format!(
                    "无法检查活动会话文件: {} ({error})",
                    path.display()
                ));
                continue;
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() || !is_rollout_file(&path) {
            failures.push(format!(
                "活动 SQLite 引用了不受支持的会话文件: {}",
                path.display()
            ));
            continue;
        }
        let canonical = match path.canonicalize() {
            Ok(canonical) => canonical,
            Err(error) => {
                failures.push(format!(
                    "无法解析活动会话文件路径: {} ({error})",
                    path.display()
                ));
                continue;
            }
        };
        let within_codex_storage = is_canonical_rollout_storage_path(codex_dir, &canonical);
        let allowed_root = if include_archived_storage {
            within_codex_storage
        } else {
            within_codex_storage
                && codex_dir
                    .join("sessions")
                    .canonicalize()
                    .is_ok_and(|root| canonical.starts_with(root))
        };
        if !allowed_root {
            let reason = if within_codex_storage {
                "活动 SQLite 引用了归档会话文件"
            } else {
                "活动 SQLite 引用的会话文件超出 Codex 会话目录"
            };
            failures.push(format!("{reason}: {}", path.display()));
            continue;
        }
        let expected_thread_ids = referenced.entry(canonical.clone()).or_default();
        expected_thread_ids.insert(thread_id.clone());
        if expected_thread_ids.len() > 1 {
            failures.push(format!(
                "活动 SQLite 的多个线程引用了同一个会话文件: {}",
                canonical.display()
            ));
        }
    }
    referenced
}

fn collect_rollout_paths(root: &Path, out: &mut Vec<PathBuf>, failures: &mut Vec<String>) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                failures.push(format!("无法读取会话目录: {} ({error})", root.display()));
            }
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                failures.push(format!("无法读取会话目录项: {} ({error})", root.display()));
                continue;
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                failures.push(format!(
                    "无法读取会话文件类型: {} ({error})",
                    path.display()
                ));
                continue;
            }
        };
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_rollout_paths(&path, out, failures);
        } else if file_type.is_file() && is_rollout_file(&path) {
            out.push(path);
        }
    }
}

pub(super) fn split_line_ending(segment: &str) -> (&str, &str) {
    if let Some(line) = segment.strip_suffix("\r\n") {
        (line, "\r\n")
    } else if let Some(line) = segment.strip_suffix('\n') {
        (line, "\n")
    } else {
        (segment, "")
    }
}

fn is_locked_io_error(error: &std::io::Error) -> bool {
    matches!(error.kind(), std::io::ErrorKind::PermissionDenied)
        || matches!(error.raw_os_error(), Some(32 | 33))
}

pub(crate) fn scan_rollouts(codex_dir: &Path, target_provider: &str) -> Result<RolloutScan> {
    scan_rollouts_with_thread_filter(codex_dir, target_provider, None, None, true, None, false)
}

pub(super) fn scan_provider_rollouts(
    codex_dir: &Path,
    target_provider: &str,
    excluded_thread_ids: &HashSet<String>,
) -> Result<RolloutScan> {
    scan_rollouts_with_thread_filter(
        codex_dir,
        target_provider,
        None,
        None,
        false,
        Some(excluded_thread_ids),
        true,
    )
}

pub(super) fn scan_rollouts_for_thread_ids(
    codex_dir: &Path,
    target_provider: &str,
    thread_ids: &HashSet<String>,
    rollout_paths_by_thread_id: &HashMap<String, String>,
) -> Result<RolloutScan> {
    scan_rollouts_with_thread_filter(
        codex_dir,
        target_provider,
        Some(thread_ids),
        Some(rollout_paths_by_thread_id),
        false,
        None,
        false,
    )
}

fn scan_rollouts_with_thread_filter(
    codex_dir: &Path,
    target_provider: &str,
    allowed_thread_ids: Option<&HashSet<String>>,
    rollout_paths_by_thread_id: Option<&HashMap<String, String>>,
    include_archived_storage: bool,
    excluded_thread_ids: Option<&HashSet<String>>,
    exclude_source_marked_subagents: bool,
) -> Result<RolloutScan> {
    let mut paths = Vec::new();
    let mut scan = RolloutScan::default();
    let mut referenced = HashMap::new();
    collect_rollout_paths(
        &codex_dir.join("sessions"),
        &mut paths,
        &mut scan.scan_failures,
    );
    if include_archived_storage {
        collect_rollout_paths(
            &codex_dir.join("archived_sessions"),
            &mut paths,
            &mut scan.scan_failures,
        );
    }
    paths.sort();
    let present_index_paths = paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<HashSet<_>>();
    scan.discovered_rollout_files = paths.len();
    if let Some(excluded_thread_ids) = excluded_thread_ids {
        paths.retain(|path| {
            !excluded_thread_ids
                .iter()
                .any(|id| rollout_filename_matches_id(path, id))
        });
    }
    if let Some(allowed_thread_ids) = allowed_thread_ids {
        let empty_rollout_paths = HashMap::new();
        let rollout_paths_by_thread_id = rollout_paths_by_thread_id.unwrap_or(&empty_rollout_paths);
        let explicitly_referenced_thread_ids = rollout_paths_by_thread_id
            .keys()
            .filter(|id| allowed_thread_ids.contains(*id))
            .cloned()
            .collect::<HashSet<_>>();
        referenced = referenced_rollout_paths(
            codex_dir,
            rollout_paths_by_thread_id,
            include_archived_storage,
            &mut scan.scan_failures,
            &mut scan.warnings,
        );
        paths.retain(|path| {
            let is_unreferenced_thread_rollout = allowed_thread_ids
                .iter()
                .filter(|id| !explicitly_referenced_thread_ids.contains(*id))
                .any(|id| rollout_filename_matches_id(path, id));
            is_unreferenced_thread_rollout
                || path
                    .canonicalize()
                    .is_ok_and(|canonical| referenced.contains_key(&canonical))
        });
    }
    scan.rollout_files = paths.len();

    let rollout_index = match load_rollout_index(codex_dir) {
        Ok(index) => index,
        Err(error) => {
            scan.warnings
                .push(format!("Session metadata cache is unavailable: {error}"));
            HashMap::new()
        }
    };
    let mut changed_index_entries = Vec::new();
    let mut metadata_only_files = 0usize;

    for path in paths {
        let expected_thread_ids = path
            .canonicalize()
            .ok()
            .and_then(|canonical| referenced.get(&canonical));
        let path_key = path.display().to_string();
        let file_metadata = fs::metadata(&path).ok();
        let file_mtime_ns = file_metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok())
            .map(system_time_ns)
            .unwrap_or_default();
        let file_size = file_metadata
            .as_ref()
            .and_then(|metadata| i64::try_from(metadata.len()).ok())
            .unwrap_or_default();
        // A cache hit is only safe when the filesystem supplied a complete
        // fingerprint. Falling back to (0, 0) after a metadata error could
        // otherwise make a changed rollout look unchanged forever.
        let has_complete_fingerprint = file_metadata.is_some() && file_mtime_ns > 0;
        if let Some(cached) = rollout_index.get(&path_key).filter(|cached| {
            has_complete_fingerprint
                && cached.file_mtime_ns == file_mtime_ns
                && cached.file_size == file_size
        }) {
            let is_subagent = cached.session_type.as_deref() == Some("subagent");
            if cached.model_provider.as_deref() == Some(target_provider) {
                if (exclude_source_marked_subagents && is_subagent)
                    || excluded_thread_ids
                        .is_some_and(|excluded| excluded.contains(&cached.session_id))
                {
                    continue;
                }
                if let Some(expected_thread_ids) = expected_thread_ids {
                    if expected_thread_ids.len() != 1
                        || !expected_thread_ids.contains(&cached.session_id)
                    {
                        scan.scan_failures.push(format!(
                            "Cached rollout metadata does not match the referenced thread: {}",
                            path.display()
                        ));
                        continue;
                    }
                }
                if allowed_thread_ids.is_some_and(|allowed| !allowed.contains(&cached.session_id)) {
                    continue;
                }
                scan.session_meta_count += 1;
                scan.thread_ids.insert(cached.session_id.clone());
                if let Some(cwd) = cached.cwd.clone() {
                    scan.cwd_by_thread_id.insert(cached.session_id.clone(), cwd);
                }
                continue;
            }
        }
        let is_large_rollout = file_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.len() > ROLLOUT_FULL_READ_LIMIT_BYTES);
        let header_metadata = rollout_header_metadata(&path);
        let header_matches_target = header_metadata
            .as_ref()
            .is_ok_and(|header| header.model_provider.as_deref() == Some(target_provider));
        if header_matches_target {
            let header = header_metadata
                .as_ref()
                .expect("matching rollout header must be readable");
            if (exclude_source_marked_subagents && header.is_subagent)
                || excluded_thread_ids.is_some_and(|excluded| excluded.contains(&header.session_id))
            {
                continue;
            }
            if let Some(expected_thread_ids) = expected_thread_ids {
                if expected_thread_ids.len() != 1
                    || !expected_thread_ids.contains(&header.session_id)
                {
                    scan.scan_failures.push(format!(
                        "Rollout metadata does not match the referenced thread: {}",
                        path.display()
                    ));
                    continue;
                }
            }
            if allowed_thread_ids.is_some_and(|allowed| !allowed.contains(&header.session_id)) {
                continue;
            }
            scan.session_meta_count += 1;
            scan.thread_ids.insert(header.session_id.clone());
            if let Some(cwd) = header.cwd.clone() {
                scan.cwd_by_thread_id.insert(header.session_id.clone(), cwd);
            }
            if has_complete_fingerprint {
                changed_index_entries.push(RolloutIndexEntry {
                    jsonl_path: path_key,
                    file_mtime_ns,
                    file_size,
                    session_id: header.session_id.clone(),
                    title: header.title.clone(),
                    cwd: header.cwd.clone(),
                    model_provider: header.model_provider.clone(),
                    session_type: Some(
                        if header.is_subagent {
                            "subagent"
                        } else {
                            "main"
                        }
                        .to_string(),
                    ),
                    parent_session_id: header.parent_session_id.clone(),
                    updated_at_ms: file_mtime_ns / 1_000_000,
                });
            }
            metadata_only_files += 1;
            continue;
        }
        if is_large_rollout {
            let header = match header_metadata {
                Ok(header) => header,
                Err(error) => {
                    scan.scan_failures.push(error.to_string());
                    continue;
                }
            };
            if (exclude_source_marked_subagents && header.is_subagent)
                || excluded_thread_ids.is_some_and(|excluded| excluded.contains(&header.session_id))
            {
                continue;
            }
            if let Some(expected_thread_ids) = expected_thread_ids {
                if expected_thread_ids.len() != 1
                    || !expected_thread_ids.contains(&header.session_id)
                {
                    scan.scan_failures.push(format!(
                        "Large rollout metadata does not match the referenced thread: {}",
                        path.display()
                    ));
                    continue;
                }
            }
            if allowed_thread_ids.is_some_and(|allowed| !allowed.contains(&header.session_id)) {
                continue;
            }
            scan.session_meta_count += 1;
            scan.thread_ids.insert(header.session_id.clone());
            if let Some(cwd) = header.cwd.clone() {
                scan.cwd_by_thread_id.insert(header.session_id.clone(), cwd);
            }
            scan.mismatched_rollouts += 1;
            scan.mismatched_session_meta += 1;
            scan.mismatched_thread_ids.insert(header.session_id.clone());
            scan.warnings.push(format!(
                "Large session file requires a streaming provider rewrite and was left unchanged: {}",
                path.display()
            ));
            continue;
        }
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => {
                let reason = if is_locked_io_error(&error) {
                    "会话文件被占用或无权限读取"
                } else {
                    "无法读取会话文件"
                };
                scan.scan_failures
                    .push(format!("{reason}: {} ({error})", path.display()));
                continue;
            }
        };
        let mut next_text = String::with_capacity(text.len());
        let mut rewrite_needed = false;
        let mut file_session_meta_count = 0usize;
        let mut file_mismatched_session_meta = 0usize;
        let mut invalid_json_lines = 0usize;
        let mut invalid_session_meta = 0usize;
        let mut thread_id = None;
        let mut cwd = None;
        let mut is_subagent = false;
        let mut title = None;
        let mut observed_provider = None;
        let mut parent_session_id = None;

        for segment in text.split_inclusive('\n') {
            let (line, line_ending) = split_line_ending(segment);
            let mut next_line = line.to_string();
            if !line.trim().is_empty() {
                if let Ok(mut record) = serde_json::from_str::<Value>(line) {
                    if record.get("type").and_then(Value::as_str) == Some("session_meta") {
                        if let Some(payload) =
                            record.get_mut("payload").and_then(Value::as_object_mut)
                        {
                            file_session_meta_count += 1;
                            if thread_id.is_none() {
                                thread_id = payload
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .map(ToString::to_string);
                            }
                            if cwd.is_none() {
                                cwd = payload
                                    .get("cwd")
                                    .and_then(Value::as_str)
                                    .and_then(normalize_workspace_path);
                            }
                            if title.is_none() {
                                title = payload
                                    .get("title")
                                    .or_else(|| payload.get("first_user_message"))
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(ToString::to_string);
                            }
                            if observed_provider.is_none() {
                                observed_provider = payload
                                    .get("model_provider")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(ToString::to_string);
                            }
                            if parent_session_id.is_none() {
                                parent_session_id = payload
                                    .get("source")
                                    .and_then(|source| {
                                        source.pointer("/subagent/thread_spawn/parent_thread_id")
                                    })
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(ToString::to_string);
                            }
                            if payload.get("source").is_some_and(source_value_is_subagent) {
                                is_subagent = true;
                            }
                            if payload.get("model_provider").and_then(Value::as_str)
                                != Some(target_provider)
                            {
                                payload.insert(
                                    "model_provider".to_string(),
                                    Value::String(target_provider.to_string()),
                                );
                                next_line = serde_json::to_string(&record)
                                    .map_err(|error| json_err(&path, error))?;
                                rewrite_needed = true;
                                file_mismatched_session_meta += 1;
                            }
                        } else {
                            invalid_session_meta += 1;
                        }
                    }
                } else {
                    invalid_json_lines += 1;
                }
            }
            next_text.push_str(&next_line);
            next_text.push_str(line_ending);
        }

        if invalid_json_lines > 0 {
            scan.scan_failures.push(format!(
                "会话文件包含 {invalid_json_lines} 行无法解析的 JSON: {}",
                path.display()
            ));
        }
        if invalid_session_meta > 0 {
            scan.scan_failures.push(format!(
                "会话文件包含 {invalid_session_meta} 条无法读取的 session_meta: {}",
                path.display()
            ));
        }

        if file_session_meta_count == 0 {
            if expected_thread_ids.is_some() {
                scan.scan_failures.push(format!(
                    "活动 SQLite 引用的会话文件缺少 session_meta: {}",
                    path.display()
                ));
            }
            continue;
        }
        let Some(thread_id) = thread_id else {
            scan.scan_failures.push(format!(
                "会话文件的 session_meta 缺少 id: {}",
                path.display()
            ));
            continue;
        };
        // Do not cache a partially malformed rollout. Re-reading it on later
        // checks preserves the diagnostics instead of hiding them behind a
        // seemingly valid fingerprint entry.
        if invalid_json_lines == 0 && invalid_session_meta == 0 && has_complete_fingerprint {
            changed_index_entries.push(RolloutIndexEntry {
                jsonl_path: path_key,
                file_mtime_ns,
                file_size,
                session_id: thread_id.clone(),
                title,
                cwd: cwd.clone(),
                model_provider: observed_provider,
                session_type: Some(if is_subagent { "subagent" } else { "main" }.to_string()),
                parent_session_id,
                updated_at_ms: file_mtime_ns / 1_000_000,
            });
        }
        if (exclude_source_marked_subagents && is_subagent)
            || excluded_thread_ids.is_some_and(|excluded| excluded.contains(&thread_id))
        {
            continue;
        }
        if let Some(expected_thread_ids) = expected_thread_ids {
            if expected_thread_ids.len() != 1 || !expected_thread_ids.contains(&thread_id) {
                scan.scan_failures.push(format!(
                    "活动 SQLite 引用的会话文件与线程 ID 不一致: {}",
                    path.display()
                ));
                continue;
            }
        }
        if allowed_thread_ids.is_some_and(|allowed| !allowed.contains(&thread_id)) {
            continue;
        }
        scan.session_meta_count += file_session_meta_count;
        scan.thread_ids.insert(thread_id.clone());
        if let Some(cwd) = cwd {
            scan.cwd_by_thread_id.insert(thread_id.clone(), cwd);
        }
        if rewrite_needed {
            scan.mismatched_rollouts += 1;
            scan.mismatched_session_meta += file_mismatched_session_meta;
            scan.mismatched_thread_ids.insert(thread_id);
            scan.changes.push(SessionFileChange {
                original_mtime: fs::metadata(&path)
                    .and_then(|metadata| metadata.modified())
                    .ok(),
                path,
                original_text: text,
                next_text,
            });
        }
    }
    if metadata_only_files > 0 {
        scan.warnings.push(format!(
            "Indexed {metadata_only_files} session file(s) from bounded metadata without loading chat bodies"
        ));
    }
    if let Err(error) =
        persist_rollout_index(codex_dir, &changed_index_entries, &present_index_paths)
    {
        scan.warnings.push(format!(
            "Session metadata cache could not be updated: {error}"
        ));
    }
    Ok(scan)
}

fn restore_file_mtime(path: &Path, mtime: Option<SystemTime>) {
    let Some(mtime) = mtime else { return };
    let Ok(file) = fs::File::options().write(true).open(path) else {
        return;
    };
    let _ = file.set_times(std::fs::FileTimes::new().set_modified(mtime));
}

#[cfg(all(target_os = "macos", not(test)))]
fn rollout_file_is_open(path: &Path) -> bool {
    std::process::Command::new("/usr/sbin/lsof")
        .args(["-t", "--"])
        .arg(path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(any(not(target_os = "macos"), test))]
fn rollout_file_is_open(_path: &Path) -> bool {
    false
}

pub(crate) fn apply_session_changes(
    changes: &[SessionFileChange],
) -> Result<(Vec<SessionFileChange>, Vec<PathBuf>)> {
    let mut applied = Vec::new();
    let mut skipped = Vec::new();
    for change in changes {
        if rollout_file_is_open(&change.path) {
            skipped.push(change.path.clone());
            continue;
        }
        match fs::read_to_string(&change.path) {
            Ok(current) if current == change.original_text => {}
            Ok(_) => {
                skipped.push(change.path.clone());
                continue;
            }
            Err(error) if is_locked_io_error(&error) => {
                skipped.push(change.path.clone());
                continue;
            }
            Err(error) => {
                let original_error = io_err(&change.path, error);
                return match restore_session_changes(&applied) {
                    Ok(()) => Err(original_error),
                    Err(rollback_error) => Err(CodexxError::Config(format!(
                        "{original_error}；回滚失败：{rollback_error}"
                    ))),
                };
            }
        }
        match atomic_write(&change.path, change.next_text.as_bytes()) {
            Ok(()) => {
                restore_file_mtime(&change.path, change.original_mtime);
                applied.push(change.clone());
            }
            Err(error) => {
                return match restore_session_changes(&applied) {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(CodexxError::Config(format!(
                        "{error}；回滚失败：{rollback_error}"
                    ))),
                };
            }
        }
    }
    Ok((applied, skipped))
}

pub(crate) fn restore_session_changes(changes: &[SessionFileChange]) -> Result<()> {
    let mut failed = 0usize;
    for change in changes {
        if rollout_file_is_open(&change.path) {
            failed += 1;
            continue;
        }
        let unchanged =
            fs::read_to_string(&change.path).is_ok_and(|current| current == change.next_text);
        if !unchanged {
            failed += 1;
            continue;
        }
        if atomic_write(&change.path, change.original_text.as_bytes()).is_err() {
            failed += 1;
            continue;
        }
        restore_file_mtime(&change.path, change.original_mtime);
    }
    if failed > 0 {
        return Err(CodexxError::Config(format!(
            "有 {failed} 个会话文件无法安全回滚；文件正在使用或已发生变化。"
        )));
    }
    Ok(())
}

fn expand_sqlite_home(codex_dir: &Path, value: &str) -> PathBuf {
    let trimmed = value.trim();
    if trimmed == "~" {
        return home_dir().unwrap_or_else(|_| codex_dir.to_path_buf());
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        return home_dir()
            .map(|home| home.join(rest))
            .unwrap_or_else(|_| PathBuf::from(trimmed));
    }
    let path = PathBuf::from(trimmed);
    if path.is_absolute() {
        path
    } else {
        codex_dir.join(path)
    }
}

fn configured_sqlite_home(codex_dir: &Path) -> Option<PathBuf> {
    let config = config_path(codex_dir);
    let configured = fs::read_to_string(&config)
        .ok()
        .and_then(|text| text.parse::<DocumentMut>().ok())
        .and_then(|doc| string_value(&doc, "sqlite_home"));
    #[cfg(test)]
    let environment = None;
    #[cfg(not(test))]
    let environment = std::env::var("CODEX_SQLITE_HOME").ok();
    configured
        .or(environment)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| expand_sqlite_home(codex_dir, &value))
}

#[derive(Debug)]
struct SqliteStorageRoot {
    path: PathBuf,
    is_active: bool,
    active_priority: usize,
    session_priority: usize,
    allow_custom_names: bool,
}

fn sqlite_storage_roots(codex_dir: &Path) -> Vec<SqliteStorageRoot> {
    let mut roots = Vec::new();
    let configured = configured_sqlite_home(codex_dir);
    if let Some(configured) = configured.as_ref() {
        roots.push(SqliteStorageRoot {
            path: configured.clone(),
            is_active: true,
            active_priority: 0,
            session_priority: 0,
            allow_custom_names: true,
        });
    }
    roots.push(SqliteStorageRoot {
        path: codex_dir.to_path_buf(),
        is_active: configured.is_none(),
        active_priority: 1,
        session_priority: 2,
        allow_custom_names: false,
    });
    roots.push(SqliteStorageRoot {
        path: codex_dir.join("sqlite"),
        is_active: false,
        active_priority: 2,
        session_priority: 1,
        allow_custom_names: true,
    });

    let mut seen = HashSet::new();
    roots.retain(|root| seen.insert(root.path.clone()));
    roots
}

const SESSION_TABLES: &[&str] = &["threads", "automation_runs", "inbox_items"];
const RELATED_TABLES: &[&str] = &[
    "threads",
    "thread_dynamic_tools",
    "thread_spawn_edges",
    "agent_job_items",
    "logs",
    "stage1_outputs",
    "thread_goals",
    "thread_turns",
    "thread_items",
    "thread_history_projection_state",
    "local_thread_catalog",
    "automation_runs",
    "inbox_items",
];

#[derive(Debug, Clone, Default)]
pub(super) struct SqliteDiscovery {
    pub(super) active_paths: Vec<PathBuf>,
    pub(super) thread_paths: Vec<PathBuf>,
    pub(super) session_paths: Vec<PathBuf>,
    pub(super) related_paths: Vec<PathBuf>,
    pub(super) unreadable_paths: Vec<PathBuf>,
    pub(super) active_scan_failures: Vec<String>,
}

impl SqliteDiscovery {
    pub(super) fn active_first_thread_paths(&self) -> Vec<PathBuf> {
        primary_paths_first(&self.active_paths, &self.thread_paths)
    }

    pub(super) fn active_first_session_paths(&self) -> Vec<PathBuf> {
        primary_paths_first(&self.active_paths, &self.session_paths)
    }
}

#[derive(Debug)]
struct DiscoveredSqlite {
    path: PathBuf,
    is_active: bool,
    active_priority: usize,
    session_priority: usize,
    state_version: Option<u64>,
    has_threads: bool,
    has_session_tables: bool,
    has_related_tables: bool,
}

fn is_sqlite_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "db" | "sqlite" | "sqlite3"
            )
        })
}

fn is_root_codex_sqlite_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    if name == "codex-dev.db" {
        return true;
    }
    let Some(stem) = name.strip_suffix(".sqlite") else {
        return false;
    };
    let Some((kind, version)) = stem.rsplit_once('_') else {
        return false;
    };
    !version.is_empty()
        && version.chars().all(|ch| ch.is_ascii_digit())
        && matches!(
            kind,
            "state" | "logs" | "memories" | "goals" | "thread_history"
        )
}

fn sqlite_state_version(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|stem| stem.strip_prefix("state_"))
        .and_then(|value| value.parse::<u64>().ok())
}

fn sqlite_table_names(path: &Path, busy_timeout: Option<Duration>) -> Option<HashSet<String>> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    if let Some(timeout) = busy_timeout {
        conn.busy_timeout(timeout).ok()?;
    }
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .ok()?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0)).ok()?;
    let mut tables = HashSet::new();
    for row in rows {
        tables.insert(row.ok()?);
    }
    Some(tables)
}

fn has_sqlite_header(path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut header = [0u8; 16];
    file.read_exact(&mut header).is_ok() && &header == b"SQLite format 3\0"
}

fn primary_paths_first(primary: &[PathBuf], paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut ordered = Vec::with_capacity(paths.len());
    let mut seen = HashSet::new();
    for path in primary.iter().chain(paths) {
        if seen.insert(path.clone()) {
            ordered.push(path.clone());
        }
    }
    ordered
}

fn compare_sqlite_filenames(left: &Path, right: &Path) -> std::cmp::Ordering {
    left.file_name()
        .is_none_or(|name| name != std::ffi::OsStr::new("codex-dev.db"))
        .cmp(
            &right
                .file_name()
                .is_none_or(|name| name != std::ffi::OsStr::new("codex-dev.db")),
        )
        .then_with(|| left.file_name().cmp(&right.file_name()))
}

pub(super) fn ensure_sqlite_discovery_writable(discovery: &SqliteDiscovery) -> Result<()> {
    if discovery.unreadable_paths.is_empty() {
        Ok(())
    } else {
        Err(CodexxError::Config(
            "无法读取会话数据库，请关闭 Codex 后重试。".to_string(),
        ))
    }
}

fn ordered_database_paths(
    databases: &[DiscoveredSqlite],
    include: impl Fn(&DiscoveredSqlite) -> bool,
    priority: impl Fn(&DiscoveredSqlite) -> usize,
) -> Vec<PathBuf> {
    let mut matches = databases
        .iter()
        .filter(|database| include(database))
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        priority(left)
            .cmp(&priority(right))
            .then_with(|| compare_sqlite_filenames(&left.path, &right.path))
    });
    matches
        .into_iter()
        .map(|database| database.path.clone())
        .collect()
}

pub(super) fn discover_sqlite_databases(codex_dir: &Path) -> SqliteDiscovery {
    discover_sqlite_databases_with_busy_timeout(codex_dir, None)
}

fn discover_sqlite_databases_with_busy_timeout(
    codex_dir: &Path,
    busy_timeout: Option<Duration>,
) -> SqliteDiscovery {
    let mut databases = Vec::new();
    let mut seen_paths = HashSet::new();
    let mut unreadable_paths = Vec::new();
    let mut active_scan_failures = Vec::new();

    for root in sqlite_storage_roots(codex_dir) {
        let entries = match fs::read_dir(&root.path) {
            Ok(entries) => entries,
            Err(error) => {
                if root.is_active && error.kind() != std::io::ErrorKind::NotFound {
                    active_scan_failures.push(format!(
                        "无法读取当前会话数据库目录: {} ({error})",
                        root.path.display()
                    ));
                }
                continue;
            }
        };
        let mut paths = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    if root.is_active {
                        active_scan_failures.push(format!(
                            "无法读取当前会话数据库目录项: {} ({error})",
                            root.path.display()
                        ));
                    }
                    continue;
                }
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    if root.is_active {
                        active_scan_failures.push(format!(
                            "无法读取当前会话数据库文件类型: {} ({error})",
                            path.display()
                        ));
                    }
                    continue;
                }
            };
            if file_type.is_file()
                && !file_type.is_symlink()
                && is_sqlite_file(&path)
                && (root.allow_custom_names || is_root_codex_sqlite_file(&path))
            {
                paths.push(path);
            }
        }
        paths.sort();

        for path in paths {
            if !seen_paths.insert(path.clone()) {
                continue;
            }
            let codex_named = is_root_codex_sqlite_file(&path);
            let Some(tables) = sqlite_table_names(&path, busy_timeout) else {
                let header_is_sqlite = has_sqlite_header(&path);
                let read_error = fs::File::open(&path).err();
                if header_is_sqlite || read_error.is_some() {
                    if root.is_active {
                        let detail = read_error
                            .map(|error| format!(" ({error})"))
                            .unwrap_or_default();
                        active_scan_failures.push(format!(
                            "无法读取当前活动会话数据库: {}{detail}",
                            path.display()
                        ));
                    }
                    unreadable_paths.push(path);
                }
                continue;
            };
            let has_threads = tables.contains("threads");
            let has_session_tables = SESSION_TABLES.iter().any(|table| tables.contains(*table));
            let has_related_tables = RELATED_TABLES.iter().any(|table| tables.contains(*table))
                && (has_session_tables || codex_named);
            if !has_session_tables && !has_related_tables {
                continue;
            }
            databases.push(DiscoveredSqlite {
                state_version: sqlite_state_version(&path),
                path,
                is_active: root.is_active,
                active_priority: root.active_priority,
                session_priority: root.session_priority,
                has_threads,
                has_session_tables,
                has_related_tables,
            });
        }
    }

    let active_path = databases
        .iter()
        .filter(|database| database.is_active && database.has_threads)
        .min_by(|left, right| {
            left.active_priority
                .cmp(&right.active_priority)
                .then_with(|| right.state_version.cmp(&left.state_version))
                .then_with(|| compare_sqlite_filenames(&left.path, &right.path))
        })
        .map(|database| database.path.clone());

    SqliteDiscovery {
        active_paths: active_path.into_iter().collect(),
        thread_paths: ordered_database_paths(
            &databases,
            |database| database.has_threads,
            |database| database.session_priority,
        ),
        session_paths: ordered_database_paths(
            &databases,
            |database| database.has_session_tables,
            |database| database.session_priority,
        ),
        related_paths: ordered_database_paths(
            &databases,
            |database| database.has_related_tables,
            |database| database.active_priority,
        ),
        unreadable_paths,
        active_scan_failures,
    }
}

pub(crate) fn sqlite_candidate_paths(codex_dir: &Path) -> Vec<PathBuf> {
    discover_sqlite_databases(codex_dir).active_paths
}

pub(crate) fn sqlite_candidate_paths_with_timeout(
    codex_dir: &Path,
    timeout: Duration,
) -> Vec<PathBuf> {
    discover_sqlite_databases_with_busy_timeout(codex_dir, Some(timeout)).active_paths
}

fn clean_session_title(values: [Option<String>; 3]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

pub(crate) fn session_project_title(cwd: &str) -> Option<String> {
    let path = normalize_workspace_path(cwd)?.replace('\\', "/");
    if path.chars().any(char::is_control) || path.contains("://") {
        return None;
    }
    let parts = path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    // A UNC share root is a storage location, rather than a project directory.
    if path.starts_with("//") && parts.len() <= 2 {
        return None;
    }
    let name = parts.last()?.trim();
    if name.is_empty()
        || matches!(name, "." | ".." | "~")
        || (name.len() == 2 && name.as_bytes()[0].is_ascii_alphabetic() && name.ends_with(':'))
    {
        return None;
    }
    Some(name.to_string())
}

/// Best-effort titles for the small recent-usage list. Read only the requested
/// IDs and re-read metadata on each refresh so renamed chats appear immediately.
pub(crate) fn session_titles_by_id(codex_dir: &Path, ids: &[String]) -> HashMap<String, String> {
    let ids = ids
        .iter()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
        .collect::<HashSet<_>>();
    if ids.is_empty() {
        return HashMap::new();
    }
    let timeout = Duration::from_millis(50);
    let discovery = discover_sqlite_databases_with_busy_timeout(codex_dir, Some(timeout));
    let mut titles = HashMap::new();
    let mut projects = HashMap::new();
    for path in discovery.active_first_session_paths() {
        let Ok(conn) = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) else {
            continue;
        };
        if conn.busy_timeout(timeout).is_err()
            || !sqlite_has_table(&conn, "threads").unwrap_or(false)
        {
            continue;
        }
        let Ok(cols) = table_column_set(&conn, "threads") else {
            continue;
        };
        if !cols.contains("id") {
            continue;
        }
        let unresolved = ids
            .iter()
            .copied()
            .filter(|id| !titles.contains_key(*id))
            .collect::<Vec<_>>();
        if unresolved.is_empty() {
            break;
        }
        let title = sql_select_column(&cols, "title", "NULL");
        let first = sql_select_column(&cols, "first_user_message", "NULL");
        let preview = sql_select_column(&cols, "preview", "NULL");
        let cwd = sql_select_column(&cols, "cwd", "NULL");
        // Keep below SQLite's variable limit even if another caller requests more IDs.
        for chunk in unresolved.chunks(400) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let query = format!("SELECT \"id\", {title}, {first}, {preview}, {cwd} FROM threads WHERE \"id\" IN ({placeholders})");
            let Ok(mut statement) = conn.prepare(&query) else {
                continue;
            };
            let Ok(rows) = statement.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                let id = row.get::<_, String>(0)?;
                let text = |index| row.get::<_, Option<String>>(index).ok().flatten();
                Ok((
                    id,
                    clean_session_title([text(1), text(2), text(3)]),
                    text(4),
                ))
            }) else {
                continue;
            };
            for (id, title, cwd) in rows.flatten() {
                if let Some(title) = title {
                    titles.entry(id).or_insert(title);
                } else if let Some(project) = cwd.as_deref().and_then(session_project_title) {
                    projects.entry(id).or_insert(project);
                }
            }
        }
    }
    // Prefer a real chat title from an older database over a project-only fallback.
    for (id, project) in projects {
        titles.entry(id).or_insert(project);
    }
    titles
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn sqlite_session_db_paths(codex_dir: &Path) -> Vec<PathBuf> {
    discover_sqlite_databases(codex_dir).session_paths
}
pub(super) fn sqlite_subagent_thread_ids(
    conn: &Connection,
    thread_cols: &HashSet<String>,
) -> Result<HashSet<String>> {
    let mut edge_child_ids = HashSet::new();

    if sqlite_has_table(conn, "thread_spawn_edges")? {
        let edge_cols = table_column_set(conn, "thread_spawn_edges")?;
        if edge_cols.contains("child_thread_id") {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT e.child_thread_id
                     FROM thread_spawn_edges e
                     INNER JOIN threads t ON t.id = e.child_thread_id",
                )
                .map_err(|e| CodexxError::Database(e.to_string()))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| CodexxError::Database(e.to_string()))?;
            for row in rows {
                edge_child_ids.insert(row.map_err(|e| CodexxError::Database(e.to_string()))?);
            }
        }
    }

    let mut ids = edge_child_ids;
    if thread_cols.contains("thread_source") || thread_cols.contains("source") {
        let thread_source_col = sql_select_column(thread_cols, "thread_source", "NULL");
        let source_col = sql_select_column(thread_cols, "source", "NULL");
        let query = format!("SELECT \"id\", {thread_source_col}, {source_col} FROM threads");
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| CodexxError::Database(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|e| CodexxError::Database(e.to_string()))?;
        for row in rows {
            let (id, thread_source, source) =
                row.map_err(|e| CodexxError::Database(e.to_string()))?;
            if let Some(thread_source) = thread_source
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if thread_source.eq_ignore_ascii_case("subagent") {
                    ids.insert(id);
                } else {
                    ids.remove(&id);
                }
                continue;
            }

            if let Some(source) = source
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if source_text_is_subagent(source) {
                    ids.insert(id);
                } else {
                    ids.remove(&id);
                }
            }
        }
    }

    Ok(ids)
}

pub(crate) fn source_value_is_subagent(source: &Value) -> bool {
    match source {
        Value::String(source) => source.trim().eq_ignore_ascii_case("subagent"),
        Value::Object(source) => source.contains_key("subagent"),
        _ => false,
    }
}

fn source_text_is_subagent(source: &str) -> bool {
    let source = source.trim();
    source.eq_ignore_ascii_case("subagent")
        || serde_json::from_str::<Value>(source)
            .ok()
            .as_ref()
            .is_some_and(source_value_is_subagent)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsageThreadIdentity {
    pub(crate) is_subagent: bool,
    pub(crate) classification_known: bool,
    pub(crate) parent_id: Option<String>,
    pub(crate) parent_conflict: bool,
}

fn usage_identities_on_connection(
    conn: &Connection,
) -> Result<HashMap<String, (bool, bool, HashSet<String>)>> {
    if !sqlite_has_table(conn, "threads")? {
        return Ok(HashMap::new());
    }
    let cols = table_column_set(conn, "threads")?;
    if !cols.contains("id") {
        return Ok(HashMap::new());
    }
    let subagents = sqlite_subagent_thread_ids(conn, &cols)?;
    let source = sql_select_column(&cols, "source", "NULL");
    let thread_source = sql_select_column(&cols, "thread_source", "NULL");
    let mut statement = conn
        .prepare(&format!(
            "SELECT id, {source}, {thread_source} FROM threads"
        ))
        .map_err(|error| CodexxError::Database(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1).ok().flatten(),
                row.get::<_, Option<String>>(2).ok().flatten(),
            ))
        })
        .map_err(|error| CodexxError::Database(error.to_string()))?;
    let mut identities = HashMap::new();
    for row in rows {
        let (id, source, thread_source) =
            row.map_err(|error| CodexxError::Database(error.to_string()))?;
        if id.trim().is_empty() {
            continue;
        }
        let is_subagent = subagents.contains(&id);
        let classification_known = is_subagent
            || thread_source
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            || source
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
        let mut parents = HashSet::new();
        if is_subagent {
            if let Some(parent) = source
                .as_deref()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .and_then(|value| {
                    value
                        .pointer("/subagent/thread_spawn/parent_thread_id")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            {
                parents.insert(parent);
            }
        }
        identities.insert(id, (is_subagent, classification_known, parents));
    }
    if sqlite_has_table(conn, "thread_spawn_edges")? {
        let cols = table_column_set(conn, "thread_spawn_edges")?;
        if cols.contains("child_thread_id") && cols.contains("parent_thread_id") {
            let mut statement = conn
                .prepare("SELECT child_thread_id, parent_thread_id FROM thread_spawn_edges")
                .map_err(|error| CodexxError::Database(error.to_string()))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1).ok().flatten(),
                    ))
                })
                .map_err(|error| CodexxError::Database(error.to_string()))?;
            for row in rows {
                let (child, parent) =
                    row.map_err(|error| CodexxError::Database(error.to_string()))?;
                if let Some((true, _, parents)) = identities.get_mut(&child) {
                    if let Some(parent) = parent
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty())
                    {
                        parents.insert(parent);
                    }
                }
            }
        }
    }
    Ok(identities)
}

/// Reuse session-management classification without reading any rollout content.
/// Current database records override stale legacy classification and parentage.
pub(crate) fn usage_thread_identities(codex_dir: &Path) -> HashMap<String, UsageThreadIdentity> {
    let timeout = Duration::from_millis(50);
    let discovery = discover_sqlite_databases_with_busy_timeout(codex_dir, Some(timeout));
    let mut active_ids = HashSet::new();
    let mut combined = HashMap::<String, (bool, bool, HashSet<String>)>::new();
    for path in discovery.active_first_session_paths() {
        let Ok(conn) = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) else {
            continue;
        };
        if conn.busy_timeout(timeout).is_err() {
            continue;
        }
        let Ok(identities) = usage_identities_on_connection(&conn) else {
            continue;
        };
        let is_active = discovery.active_paths.contains(&path);
        for (id, (is_subagent, classification_known, parents)) in identities {
            if is_active {
                active_ids.insert(id.clone());
                combined.insert(id, (is_subagent, classification_known, parents));
            } else if !active_ids.contains(&id) {
                let existing = combined
                    .entry(id)
                    .or_insert_with(|| (false, false, HashSet::new()));
                existing.0 |= is_subagent;
                existing.1 |= classification_known;
                existing.2.extend(parents);
            }
        }
    }
    combined
        .into_iter()
        .map(|(id, (is_subagent, classification_known, parents))| {
            let parent_conflict = is_subagent && parents.len() > 1;
            let parent_id =
                (is_subagent && parents.len() == 1).then(|| parents.into_iter().next().unwrap());
            (
                id,
                UsageThreadIdentity {
                    is_subagent,
                    classification_known,
                    parent_id,
                    parent_conflict,
                },
            )
        })
        .collect()
}

pub(super) struct SqliteThreadIndexState<'a> {
    pub(super) thread_id: &'a str,
    pub(super) provider: Option<&'a str>,
    pub(super) cwd: Option<&'a str>,
    pub(super) cwd_column: bool,
    pub(super) archived: bool,
}

pub(super) fn sqlite_thread_needs_alignment(
    rollouts: &RolloutScan,
    target_provider: &str,
    state: &SqliteThreadIndexState<'_>,
) -> bool {
    if state.archived {
        return false;
    }
    if state.provider.map(str::trim).unwrap_or_default() != target_provider {
        return true;
    }
    if state.cwd_column {
        if let Some(expected_cwd) = rollouts.cwd_by_thread_id.get(state.thread_id) {
            if state.cwd.and_then(normalize_workspace_path).as_deref()
                != Some(expected_cwd.as_str())
            {
                return true;
            }
        }
    }
    false
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn scan_sqlite(
    codex_dir: &Path,
    rollouts: &RolloutScan,
    target_provider: &str,
) -> Result<SqliteScan> {
    let discovery = discover_sqlite_databases(codex_dir);
    scan_sqlite_with_paths(&discovery.session_paths, rollouts, target_provider)
}

pub(super) fn scan_sqlite_with_paths(
    sqlite_paths: &[PathBuf],
    rollouts: &RolloutScan,
    target_provider: &str,
) -> Result<SqliteScan> {
    let mut scan = SqliteScan::default();
    let mut thread_ids = HashSet::new();
    let mut syncable_thread_ids = HashSet::new();
    let mut archived_thread_ids = HashSet::new();
    let mut rollout_paths_by_thread_id = HashMap::new();
    let mut subagent_ids = HashSet::new();
    let mut mismatched_ids = HashSet::new();
    for path in sqlite_paths {
        let conn = match Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(conn) => conn,
            Err(e) => {
                scan.scan_failures
                    .push(format!("无法读取当前活动 SQLite: {} ({e})", path.display()));
                continue;
            }
        };
        if !sqlite_has_table(&conn, "threads")? {
            continue;
        }
        let cols = table_column_set(&conn, "threads")?;
        if !cols.contains("id") || !cols.contains("model_provider") {
            scan.scan_failures.push(format!(
                "当前活动 SQLite 的 threads 表缺少 id 或 model_provider 字段: {}",
                path.display()
            ));
            continue;
        }
        scan.sqlite_dbs += 1;
        let cwd_col = sql_select_column(&cols, "cwd", "NULL");
        let rollout_col = sql_select_column(&cols, "rollout_path", "NULL");
        let archived_col = sql_select_column(&cols, "archived", "0");
        let query = format!(
            "SELECT \"id\", \"model_provider\", {cwd_col}, {rollout_col}, {archived_col} FROM threads"
        );
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| CodexxError::Database(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(|e| CodexxError::Database(e.to_string()))?;
        for row in rows {
            let (id, provider, cwd, rollout_path, archived) =
                row.map_err(|e| CodexxError::Database(e.to_string()))?;
            thread_ids.insert(id.clone());
            let archived = archived != 0;
            if archived {
                archived_thread_ids.insert(id.clone());
            } else {
                syncable_thread_ids.insert(id.clone());
                if let Some(rollout_path) = rollout_path
                    .map(|path| path.trim().to_string())
                    .filter(|path| !path.is_empty())
                {
                    rollout_paths_by_thread_id.insert(id.clone(), rollout_path);
                }
            }
            if sqlite_thread_needs_alignment(
                rollouts,
                target_provider,
                &SqliteThreadIndexState {
                    thread_id: &id,
                    provider: provider.as_deref(),
                    cwd: cwd.as_deref(),
                    cwd_column: cols.contains("cwd"),
                    archived,
                },
            ) {
                mismatched_ids.insert(id);
            }
        }
        subagent_ids.extend(sqlite_subagent_thread_ids(&conn, &cols)?);
    }
    subagent_ids.retain(|id| thread_ids.contains(id));
    syncable_thread_ids.retain(|id| !subagent_ids.contains(id));
    rollout_paths_by_thread_id.retain(|id, _| !subagent_ids.contains(id));
    mismatched_ids.retain(|id| !subagent_ids.contains(id));
    scan.sqlite_threads = thread_ids.len();
    scan.subagent_threads = subagent_ids.len();
    scan.top_level_threads = thread_ids.len().saturating_sub(subagent_ids.len());
    scan.mismatched_threads = mismatched_ids.len();
    scan.thread_ids = thread_ids;
    scan.syncable_thread_ids = syncable_thread_ids;
    scan.archived_thread_ids = archived_thread_ids;
    scan.subagent_thread_ids = subagent_ids;
    scan.rollout_paths_by_thread_id = rollout_paths_by_thread_id;
    scan.mismatched_thread_ids = mismatched_ids;
    Ok(scan)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn list_session_previews(
    codex_dir: &Path,
    rollouts: &RolloutScan,
    target_provider: &str,
    limit: usize,
) -> Result<(Vec<SessionPreview>, Vec<String>)> {
    let discovery = discover_sqlite_databases(codex_dir);
    list_session_previews_with_paths(
        &discovery.active_first_session_paths(),
        rollouts,
        target_provider,
        limit,
    )
}

pub(super) fn list_session_previews_with_paths(
    sqlite_paths: &[PathBuf],
    rollouts: &RolloutScan,
    target_provider: &str,
    limit: usize,
) -> Result<(Vec<SessionPreview>, Vec<String>)> {
    let mut candidates = Vec::new();
    let mut warnings = Vec::new();

    for path in sqlite_paths {
        let conn = match Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(conn) => conn,
            Err(e) => {
                warnings.push(format!("无法读取会话数据库: {} ({e})", path.display()));
                continue;
            }
        };
        if !sqlite_has_table(&conn, "threads")? {
            continue;
        }
        let cols = table_column_set(&conn, "threads")?;
        if !cols.contains("id") {
            continue;
        }
        let subagent_thread_ids = sqlite_subagent_thread_ids(&conn, &cols)?;

        let title_col = sql_select_column(&cols, "title", "NULL");
        let first_message_col = sql_select_column(&cols, "first_user_message", "NULL");
        let preview_col = sql_select_column(&cols, "preview", "NULL");
        let provider_col = sql_select_column(&cols, "model_provider", "NULL");
        let model_col = sql_select_column(&cols, "model", "NULL");
        let cwd_col = sql_select_column(&cols, "cwd", "NULL");
        let rollout_col = sql_select_column(&cols, "rollout_path", "NULL");
        let updated_ms_col = sql_select_column(&cols, "updated_at_ms", "NULL");
        let updated_col = sql_select_column(&cols, "updated_at", "NULL");
        let archived_col = sql_select_column(&cols, "archived", "0");
        let has_user_event_col = sql_select_column(&cols, "has_user_event", "0");
        let order_col = if cols.contains("recency_at_ms") {
            "\"recency_at_ms\""
        } else if cols.contains("updated_at_ms") {
            "\"updated_at_ms\""
        } else if cols.contains("updated_at") {
            "\"updated_at\""
        } else {
            "\"id\""
        };

        let query = format!(
            "SELECT \"id\", {title_col}, {first_message_col}, {preview_col}, {provider_col}, {model_col}, {cwd_col}, {rollout_col}, {updated_ms_col}, {updated_col}, {archived_col}, {has_user_event_col} FROM threads ORDER BY {order_col} DESC"
        );
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| CodexxError::Database(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let title: Option<String> = row.get(1)?;
                let first_message: Option<String> = row.get(2)?;
                let preview: Option<String> = row.get(3)?;
                let model_provider: Option<String> = row.get(4)?;
                let model: Option<String> = row.get(5)?;
                let cwd: Option<String> = row.get(6)?;
                let rollout_path: Option<String> = row.get(7)?;
                let updated_at_ms: Option<i64> = row.get(8)?;
                let updated_at: Option<i64> = row.get(9)?;
                let archived: i64 = row.get(10)?;
                let has_user_event: i64 = row.get(11)?;
                let clean_title = clean_session_title([title, first_message, preview])
                    .unwrap_or_else(|| format!("会话 {}", id.chars().take(8).collect::<String>()));
                let normalized_provider = model_provider
                    .as_ref()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty());
                let normalized_cwd = cwd.as_deref().and_then(normalize_workspace_path);
                let normalized_rollout_path = rollout_path
                    .as_ref()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty());
                let is_archived = archived != 0;
                let is_subagent = subagent_thread_ids.contains(&id);
                let needs_sync = !is_archived
                    && !is_subagent
                    && (rollouts.mismatched_thread_ids.contains(&id)
                        || sqlite_thread_needs_alignment(
                            rollouts,
                            target_provider,
                            &SqliteThreadIndexState {
                                thread_id: &id,
                                provider: normalized_provider.as_deref(),
                                cwd: normalized_cwd.as_deref(),
                                cwd_column: cols.contains("cwd"),
                                archived: is_archived,
                            },
                        ));
                Ok(SessionPreview {
                    id,
                    title: clean_title,
                    model_provider: normalized_provider.clone(),
                    model: model.and_then(|v| {
                        let v = v.trim().to_string();
                        (!v.is_empty()).then_some(v)
                    }),
                    cwd: normalized_cwd,
                    rollout_path: normalized_rollout_path,
                    updated_at_ms: updated_at_ms.or_else(|| updated_at.map(|v| v * 1000)),
                    archived: is_archived,
                    has_user_event: has_user_event != 0,
                    is_subagent,
                    needs_sync,
                })
            })
            .map_err(|e| CodexxError::Database(e.to_string()))?;

        for row in rows {
            let session = row.map_err(|e| CodexxError::Database(e.to_string()))?;
            candidates.push(session);
        }
    }

    candidates.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then_with(|| a.id.cmp(&b.id))
    });
    let mut seen = HashSet::new();
    let sessions = candidates
        .into_iter()
        .filter(|session| seen.insert(session.id.clone()))
        .take(limit.max(1))
        .collect();
    Ok((sessions, warnings))
}

const SESSION_PAGE_DEFAULT_LIMIT: usize = 100;
const SESSION_PAGE_MAX_LIMIT: usize = 100;

#[derive(Debug)]
struct SessionPageRow {
    preview: SessionPreview,
    updated_at_ms: i64,
}

fn session_page_column(columns: &HashSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!("t.\"{name}\"")
    } else {
        fallback.to_string()
    }
}

fn session_page_row_text(row: &Row<'_>, index: usize) -> Option<String> {
    match row.get_ref(index).ok()? {
        ValueRef::Null => None,
        ValueRef::Integer(value) => Some(value.to_string()),
        ValueRef::Real(value) => Some(value.to_string()),
        ValueRef::Text(value) => String::from_utf8(value.to_vec()).ok(),
        ValueRef::Blob(_) => None,
    }
}

fn session_page_source_is_subagent(expression: &str) -> String {
    format!(
        "(LOWER(TRIM(CAST({expression} AS TEXT))) = 'subagent' \
         OR INSTR(LOWER(CAST({expression} AS TEXT)), '\"subagent\"') > 0)"
    )
}

fn session_page_internal_sql(
    conn: &Connection,
    columns: &HashSet<String>,
) -> Result<(String, String)> {
    let (edge_join, edge_expression) = if sqlite_has_table(conn, "thread_spawn_edges")? {
        let edge_columns = table_column_set(conn, "thread_spawn_edges")?;
        if edge_columns.contains("child_thread_id") {
            (
                " LEFT JOIN (SELECT DISTINCT \"child_thread_id\" \
                 FROM \"thread_spawn_edges\") AS session_edges \
                 ON session_edges.\"child_thread_id\" = t.\"id\""
                    .to_string(),
                "session_edges.\"child_thread_id\" IS NOT NULL".to_string(),
            )
        } else {
            (String::new(), "0".to_string())
        }
    } else {
        (String::new(), "0".to_string())
    };
    let source_expression = if columns.contains("source") {
        session_page_source_is_subagent("t.\"source\"")
    } else {
        "0".to_string()
    };
    let thread_source_expression = if columns.contains("thread_source") {
        session_page_source_is_subagent("t.\"thread_source\"")
    } else {
        "0".to_string()
    };
    let source_fallback = if columns.contains("source") {
        format!(
            "CASE WHEN NULLIF(TRIM(CAST(t.\"source\" AS TEXT)), '') IS NOT NULL \
             THEN {source_expression} ELSE {edge_expression} END"
        )
    } else {
        edge_expression
    };
    let expression = if columns.contains("thread_source") {
        format!(
            "CASE WHEN NULLIF(TRIM(CAST(t.\"thread_source\" AS TEXT)), '') IS NOT NULL \
             THEN {thread_source_expression} ELSE {source_fallback} END"
        )
    } else {
        source_fallback
    };
    Ok((edge_join, expression))
}

fn session_page_search_pattern(search: &str) -> String {
    let search = search.to_ascii_lowercase();
    let mut escaped = String::with_capacity(search.len());
    for ch in search.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '%' => escaped.push_str("\\%"),
            '_' => escaped.push_str("\\_"),
            _ => escaped.push(ch),
        }
    }
    format!("%{escaped}%")
}

fn session_page_search_clause(
    columns: &HashSet<String>,
    search_pattern: Option<&str>,
) -> (String, Vec<SqlValue>) {
    let Some(search_pattern) = search_pattern else {
        return (String::new(), Vec::new());
    };
    let fields = [
        "title",
        "first_user_message",
        "preview",
        "cwd",
        "model_provider",
        "model",
        "id",
    ];
    let fields = fields
        .into_iter()
        .filter(|field| columns.contains(*field))
        .map(|field| format!("LOWER(COALESCE(CAST(t.\"{field}\" AS TEXT), '')) LIKE ? ESCAPE '\\'"))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return (" AND 0".to_string(), Vec::new());
    }
    let values = std::iter::repeat_with(|| SqlValue::Text(search_pattern.to_string()))
        .take(fields.len())
        .collect();
    (format!(" AND ({})", fields.join(" OR ")), values)
}

fn session_page_updated_expression(columns: &HashSet<String>) -> String {
    let updated_ms = session_page_column(columns, "updated_at_ms", "NULL");
    let updated = session_page_column(columns, "updated_at", "NULL");
    format!("CAST(COALESCE({updated_ms}, ({updated}) * 1000, 0) AS INTEGER)")
}

fn session_page_row_from_sql(
    row: &Row<'_>,
    updated_has_column: bool,
) -> rusqlite::Result<SessionPageRow> {
    let id = session_page_row_text(row, 0).unwrap_or_default();
    let title = clean_session_title([
        session_page_row_text(row, 1),
        session_page_row_text(row, 2),
        session_page_row_text(row, 3),
    ])
    .unwrap_or_else(|| format!("会话 {}", id.chars().take(8).collect::<String>()));
    let model_provider = session_page_row_text(row, 4).and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    });
    let model = session_page_row_text(row, 5).and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    });
    let cwd = session_page_row_text(row, 6).and_then(|value| normalize_workspace_path(&value));
    let rollout_path = session_page_row_text(row, 7).and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    });
    let updated_at_ms = row.get::<_, i64>(8)?;
    let archived = row.get::<_, i64>(9)? != 0;
    let has_user_event = row.get::<_, i64>(10)? != 0;
    let is_subagent = row.get::<_, i64>(11)? != 0;
    Ok(SessionPageRow {
        preview: SessionPreview {
            id,
            title,
            model_provider,
            model,
            cwd,
            rollout_path,
            updated_at_ms: updated_has_column.then_some(updated_at_ms),
            archived,
            has_user_event,
            is_subagent,
            needs_sync: false,
        },
        updated_at_ms,
    })
}

fn session_page_cursor_clause(
    updated_expression: &str,
    cursor: Option<&SessionPageCursor>,
) -> (String, Vec<SqlValue>) {
    let Some(cursor) = cursor else {
        return (String::new(), Vec::new());
    };
    (
        format!(
            " AND (({updated_expression}) < ? OR \
                   (({updated_expression}) = ? AND t.\"id\" > ?))"
        ),
        vec![
            SqlValue::Integer(cursor.updated_at_ms),
            SqlValue::Integer(cursor.updated_at_ms),
            SqlValue::Text(cursor.id.clone()),
        ],
    )
}

fn session_page_count_rows(
    conn: &Connection,
    columns: &HashSet<String>,
    internal_join: &str,
    internal_expression: &str,
    search_pattern: Option<&str>,
) -> Result<(usize, usize)> {
    let (search_clause, search_values) = session_page_search_clause(columns, search_pattern);
    let sql = format!(
        "SELECT COALESCE(SUM(CASE WHEN ({internal_expression}) THEN 0 ELSE 1 END), 0), \
                COALESCE(SUM(CASE WHEN ({internal_expression}) THEN 1 ELSE 0 END), 0) \
         FROM threads AS t{internal_join} WHERE 1 = 1{search_clause}"
    );
    conn.query_row(&sql, rusqlite::params_from_iter(search_values), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    })
    .map(|(top_level, subagent)| (top_level.max(0) as usize, subagent.max(0) as usize))
    .map_err(|error| CodexxError::Database(error.to_string()))
}

/// Return one bounded, cursor-addressable page directly from the SQLite thread metadata.
/// This path intentionally never opens or scans rollout JSONL files.
pub(crate) fn get_session_page(
    codex_dir: &Path,
    cursor_updated_at_ms: Option<i64>,
    cursor_id: Option<String>,
    limit: Option<usize>,
    search: Option<String>,
    include_internal: Option<bool>,
) -> Result<SessionPage> {
    let limit = limit
        .unwrap_or(SESSION_PAGE_DEFAULT_LIMIT)
        .clamp(1, SESSION_PAGE_MAX_LIMIT);
    let cursor = cursor_id
        .filter(|value| !value.trim().is_empty())
        .map(|id| SessionPageCursor {
            updated_at_ms: cursor_updated_at_ms.unwrap_or_default(),
            id,
        });
    let include_internal = include_internal.unwrap_or(false);
    let search_pattern = search
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| session_page_search_pattern(&value));

    let discovery = discover_sqlite_databases(codex_dir);
    let paths = if discovery.active_paths.is_empty() {
        discovery.active_first_session_paths()
    } else {
        discovery.active_paths.clone()
    };
    let mut warnings = discovery.active_scan_failures.clone();
    warnings.extend(
        discovery
            .unreadable_paths
            .iter()
            .map(|path| format!("无法读取会话数据库: {}", path.display())),
    );
    let mut top_level = 0usize;
    let mut subagent = 0usize;
    let mut page_rows = Vec::<SessionPageRow>::new();
    let mut database_has_more = false;
    let fetch_limit = (limit + 1).saturating_add(1);

    for path in paths {
        let conn = match Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(conn) => conn,
            Err(error) => {
                warnings.push(format!("无法读取会话数据库: {} ({error})", path.display()));
                continue;
            }
        };
        if !sqlite_has_table(&conn, "threads")? {
            continue;
        }
        let columns = table_column_set(&conn, "threads")?;
        if !columns.contains("id") {
            warnings.push(format!("会话数据库缺少 threads.id: {}", path.display()));
            continue;
        }
        let (internal_join, internal_expression) = session_page_internal_sql(&conn, &columns)?;
        match session_page_count_rows(
            &conn,
            &columns,
            &internal_join,
            &internal_expression,
            search_pattern.as_deref(),
        ) {
            Ok((database_top_level, database_subagent)) => {
                top_level = top_level.saturating_add(database_top_level);
                subagent = subagent.saturating_add(database_subagent);
            }
            Err(error) => {
                warnings.push(format!("无法统计会话数据库: {} ({error})", path.display()))
            }
        }

        let updated_expression = session_page_updated_expression(&columns);
        let id_expression = session_page_column(&columns, "id", "NULL");
        let title_expression = session_page_column(&columns, "title", "NULL");
        let first_message_expression = session_page_column(&columns, "first_user_message", "NULL");
        let preview_expression = session_page_column(&columns, "preview", "NULL");
        let provider_expression = session_page_column(&columns, "model_provider", "NULL");
        let model_expression = session_page_column(&columns, "model", "NULL");
        let cwd_expression = session_page_column(&columns, "cwd", "NULL");
        let rollout_expression = session_page_column(&columns, "rollout_path", "NULL");
        let archived_expression = session_page_column(&columns, "archived", "0");
        let has_user_event_expression = session_page_column(&columns, "has_user_event", "0");
        let (search_clause, search_values) =
            session_page_search_clause(&columns, search_pattern.as_deref());
        let (cursor_clause, cursor_values) =
            session_page_cursor_clause(&updated_expression, cursor.as_ref());
        let internal_clause = if include_internal {
            String::new()
        } else {
            format!(" AND NOT ({internal_expression})")
        };
        let sql = format!(
            "SELECT {id_expression}, {title_expression}, {first_message_expression}, \
                    {preview_expression}, {provider_expression}, {model_expression}, \
                    {cwd_expression}, {rollout_expression}, {updated_expression} AS \
                    __codexx_updated_at_ms, CAST(COALESCE({archived_expression}, 0) AS INTEGER), \
                    CAST(COALESCE({has_user_event_expression}, 0) AS INTEGER), \
                    CAST(({internal_expression}) AS INTEGER) \
             FROM threads AS t{internal_join} \
             WHERE 1 = 1{search_clause}{cursor_clause}{internal_clause} \
             ORDER BY __codexx_updated_at_ms DESC, t.\"id\" ASC LIMIT ?"
        );
        let mut params = Vec::<SqlValue>::new();
        params.extend(search_values);
        params.extend(cursor_values);
        params.push(SqlValue::Integer(fetch_limit as i64));
        let mut statement = match conn.prepare(&sql) {
            Ok(statement) => statement,
            Err(error) => {
                warnings.push(format!("无法查询会话数据库: {} ({error})", path.display()));
                continue;
            }
        };
        let rows = match statement.query_map(rusqlite::params_from_iter(params), |row| {
            session_page_row_from_sql(
                row,
                columns.contains("updated_at_ms") || columns.contains("updated_at"),
            )
        }) {
            Ok(rows) => rows,
            Err(error) => {
                warnings.push(format!("无法读取会话分页: {} ({error})", path.display()));
                continue;
            }
        };
        let mut database_rows = Vec::new();
        for row in rows {
            match row {
                Ok(row) => database_rows.push(row),
                Err(error) => {
                    warnings.push(format!("无法解析会话分页行: {} ({error})", path.display()))
                }
            }
        }
        if database_rows.len() > limit + 1 {
            database_has_more = true;
            database_rows.truncate(limit + 1);
        }
        page_rows.extend(database_rows);
    }

    page_rows.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.preview.id.cmp(&right.preview.id))
    });
    let mut seen = HashSet::new();
    let mut sessions = page_rows
        .into_iter()
        .filter(|row| seen.insert(row.preview.id.clone()))
        .map(|row| row.preview)
        .take(limit + 1)
        .collect::<Vec<_>>();
    let page_has_extra = sessions.len() > limit;
    if page_has_extra {
        sessions.truncate(limit);
    }
    let total = if include_internal {
        top_level + subagent
    } else {
        top_level
    };
    let has_more = page_has_extra || database_has_more;
    let next_cursor =
        has_more
            .then(|| sessions.last())
            .flatten()
            .map(|session| SessionPageCursor {
                updated_at_ms: session.updated_at_ms.unwrap_or_default(),
                id: session.id.clone(),
            });
    Ok(SessionPage {
        sessions,
        total,
        top_level,
        subagent,
        has_more,
        next_cursor,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_codex_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "codex-x-storage-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create test codex dir");
        path
    }

    #[test]
    fn missing_sqlite_rollout_reference_is_a_non_blocking_warning() {
        let codex_dir = temp_codex_dir("missing-rollout-reference");
        fs::create_dir_all(codex_dir.join("sessions")).unwrap();
        let thread_id = "019dd389-96fc-70b1-ba6c-388ca8a96665";
        let thread_ids = HashSet::from([thread_id.to_string()]);
        let missing_path = codex_dir
            .join("sessions/2026/04/28")
            .join(format!("rollout-2026-04-28T17-59-31-{thread_id}.jsonl"));
        let rollout_paths =
            HashMap::from([(thread_id.to_string(), missing_path.display().to_string())]);

        let scan = scan_rollouts_for_thread_ids(&codex_dir, "openai", &thread_ids, &rollout_paths)
            .unwrap();

        assert!(scan.scan_failures.is_empty());
        assert_eq!(scan.warnings.len(), 1);
        assert!(scan.warnings[0].contains("旧会话引用"));
        fs::remove_dir_all(codex_dir).unwrap();
    }

    fn create_thread_database(path: &Path, id: &str, provider: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create sqlite parent");
        }
        let conn = Connection::open(path).expect("create thread database");
        conn.execute_batch(
            "CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                model_provider TEXT NOT NULL,
                title TEXT,
                updated_at_ms INTEGER
             );",
        )
        .expect("create threads table");
        conn.execute(
            "INSERT INTO threads (id, model_provider, title, updated_at_ms)
             VALUES (?1, ?2, 'test session', 1)",
            (id, provider),
        )
        .expect("insert thread");
    }

    fn create_title_database(path: &Path) -> Connection {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE threads (id TEXT PRIMARY KEY, title, first_user_message, preview, cwd TEXT);").unwrap();
        conn
    }

    fn create_page_database(path: &Path, count: usize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                title TEXT,
                first_user_message TEXT,
                preview TEXT,
                model_provider TEXT,
                model TEXT,
                cwd TEXT,
                rollout_path TEXT,
                updated_at_ms INTEGER,
                archived INTEGER NOT NULL DEFAULT 0,
                has_user_event INTEGER NOT NULL DEFAULT 1,
                source TEXT,
                thread_source TEXT
             );
             CREATE TABLE thread_spawn_edges (
                parent_thread_id TEXT,
                child_thread_id TEXT
             );",
        )
        .unwrap();
        let transaction = conn.transaction().unwrap();
        for index in 0..count {
            let id = format!("thread-{index:05}");
            let source = (index % 10 == 0).then_some("subagent");
            transaction
                .execute(
                    "INSERT INTO threads
                     (id, title, first_user_message, preview, model_provider, model, cwd,
                      updated_at_ms, source)
                     VALUES (?1, ?2, ?3, ?4, 'openai', 'gpt-test', '/workspace/project', ?5, ?6)",
                    (
                        &id,
                        format!("Session {index}"),
                        format!("Message {index}"),
                        if index % 25 == 0 {
                            "needle preview"
                        } else {
                            "ordinary preview"
                        },
                        (count - index) as i64,
                        source,
                    ),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    #[test]
    fn bounded_rollout_header_reads_metadata_without_chat_body() {
        let codex_dir = temp_codex_dir("bounded-rollout-header");
        let path = codex_dir.join("rollout.jsonl");
        let session_meta = serde_json::json!({
            "type": "session_meta",
            "payload": {
                "id": "session-large",
                "title": "Large session",
                "cwd": "C:/workspace/project",
                "model_provider": "openai",
                "source": {
                    "subagent": {
                        "thread_spawn": { "parent_thread_id": "session-parent" }
                    }
                }
            }
        });
        fs::write(
            &path,
            format!("{session_meta}\n{{\"type\":\"event_msg\",\"payload\":{{}}}}\n"),
        )
        .expect("write rollout fixture");

        let metadata = rollout_header_metadata(&path).expect("read bounded rollout metadata");
        assert_eq!(metadata.session_id, "session-large");
        assert_eq!(metadata.title.as_deref(), Some("Large session"));
        assert_eq!(metadata.cwd.as_deref(), Some("C:/workspace/project"));
        assert_eq!(metadata.model_provider.as_deref(), Some("openai"));
        assert!(metadata.is_subagent);
        assert_eq!(
            metadata.parent_session_id.as_deref(),
            Some("session-parent")
        );

        let _ = fs::remove_dir_all(codex_dir);
    }

    fn create_identity_database(path: &Path) -> Connection {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE threads (id TEXT PRIMARY KEY, thread_source TEXT, source TEXT);
            CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn usage_thread_identity_reuses_edges_source_and_authoritative_thread_source() {
        let dir = temp_codex_dir("usage-identities-source");
        let path = dir.join("state_10.sqlite");
        let conn = create_identity_database(&path);
        conn.execute_batch(r#"INSERT INTO threads VALUES
            ('parent', 'user', NULL),
            ('edge', NULL, NULL),
            ('source', NULL, '{"subagent":{"thread_spawn":{"parent_thread_id":"parent"}}}'),
            ('thread-source', 'subagent', NULL),
            ('source-only', NULL, 'subagent'),
            ('user-override', 'user', '{"subagent":{"thread_spawn":{"parent_thread_id":"parent"}}}');
            INSERT INTO thread_spawn_edges VALUES ('parent', 'edge'), ('parent', 'user-override');"#).unwrap();
        drop(conn);
        let before = fs::read(&path).unwrap();
        let identities = usage_thread_identities(&dir);
        assert_eq!(
            identities["edge"],
            UsageThreadIdentity {
                classification_known: true,
                is_subagent: true,
                parent_id: Some("parent".into()),
                parent_conflict: false,
            }
        );
        assert_eq!(identities["source"], identities["edge"]);
        assert_eq!(
            identities["thread-source"],
            UsageThreadIdentity {
                classification_known: true,
                is_subagent: true,
                parent_id: None,
                parent_conflict: false,
            }
        );
        assert_eq!(identities["source-only"], identities["thread-source"]);
        assert_eq!(
            identities["user-override"],
            UsageThreadIdentity {
                classification_known: true,
                is_subagent: false,
                parent_id: None,
                parent_conflict: false,
            }
        );
        assert_eq!(identities["parent"], identities["user-override"]);
        assert_eq!(fs::read(&path).unwrap(), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn usage_thread_identity_missing_or_blank_source_is_unknown_not_explicit_user() {
        let dir = temp_codex_dir("usage-identities-unknown");
        let active = create_identity_database(&dir.join("state_10.sqlite"));
        active
            .execute_batch(
                "INSERT INTO threads VALUES
            ('null-source', NULL, NULL), ('blank-source', '  ', '  '),
            ('known-user', 'user', NULL), ('source-user', NULL, 'cli');",
            )
            .unwrap();
        let legacy_path = dir.join("sqlite/state_5.sqlite");
        create_thread_database(&legacy_path, "no-source-columns", "openai");
        let identities = usage_thread_identities(&dir);
        for id in ["null-source", "blank-source", "no-source-columns"] {
            assert_eq!(
                identities[id],
                UsageThreadIdentity {
                    is_subagent: false,
                    classification_known: false,
                    parent_id: None,
                    parent_conflict: false,
                }
            );
        }
        for id in ["known-user", "source-user"] {
            assert!(!identities[id].is_subagent);
            assert!(identities[id].classification_known);
        }
        drop(active);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn usage_thread_identity_active_records_override_stale_legacy_subagents_and_parents() {
        let dir = temp_codex_dir("usage-identities-active");
        let active = create_identity_database(&dir.join("state_10.sqlite"));
        let legacy = create_identity_database(&dir.join("sqlite/state_5.sqlite"));
        active
            .execute_batch(
                "INSERT INTO threads VALUES ('user', 'user', NULL), ('child', 'subagent', NULL);
            INSERT INTO thread_spawn_edges VALUES ('current-parent', 'child');",
            )
            .unwrap();
        legacy.execute_batch("INSERT INTO threads VALUES ('user', 'subagent', NULL), ('child', 'subagent', NULL), ('legacy-child', 'subagent', NULL);
            INSERT INTO thread_spawn_edges VALUES ('old-parent', 'user'), ('old-parent', 'child'), ('legacy-parent', 'legacy-child');").unwrap();
        let identities = usage_thread_identities(&dir);
        assert_eq!(
            identities["user"],
            UsageThreadIdentity {
                classification_known: true,
                is_subagent: false,
                parent_id: None,
                parent_conflict: false,
            }
        );
        assert_eq!(
            identities["child"].parent_id.as_deref(),
            Some("current-parent")
        );
        assert_eq!(
            identities["legacy-child"].parent_id.as_deref(),
            Some("legacy-parent")
        );
        drop(active);
        drop(legacy);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn usage_thread_identity_conflicting_parent_edges_or_sources_remain_unassigned() {
        let dir = temp_codex_dir("usage-identities-conflicts");
        let active = create_identity_database(&dir.join("state_10.sqlite"));
        let first = create_identity_database(&dir.join("sqlite/state_5.sqlite"));
        let second = create_identity_database(&dir.join("sqlite/state_6.sqlite"));
        active.execute_batch(r#"INSERT INTO threads VALUES ('conflict', 'subagent', '{"subagent":{"thread_spawn":{"parent_thread_id":"source-parent"}}}');
            INSERT INTO thread_spawn_edges VALUES ('edge-parent', 'conflict');"#).unwrap();
        first.execute_batch("INSERT INTO threads VALUES ('legacy', 'subagent', NULL); INSERT INTO thread_spawn_edges VALUES ('first-parent', 'legacy');").unwrap();
        second.execute_batch("INSERT INTO threads VALUES ('legacy', 'subagent', NULL); INSERT INTO thread_spawn_edges VALUES ('second-parent', 'legacy');").unwrap();
        let identities = usage_thread_identities(&dir);
        assert_eq!(
            identities["conflict"],
            UsageThreadIdentity {
                classification_known: true,
                is_subagent: true,
                parent_id: None,
                parent_conflict: true,
            }
        );
        assert_eq!(identities["legacy"], identities["conflict"]);
        drop(active);
        drop(first);
        drop(second);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn usage_thread_identity_missing_corrupt_and_locked_databases_are_best_effort() {
        let dir = temp_codex_dir("usage-identities-unavailable");
        assert!(usage_thread_identities(&dir.join("missing")).is_empty());
        assert!(!dir.join("missing").exists());
        fs::write(
            dir.join("state_12.sqlite"),
            b"SQLite format 3\0broken fixture",
        )
        .unwrap();
        assert!(usage_thread_identities(&dir).is_empty());
        let locked = create_identity_database(&dir.join("state_10.sqlite"));
        locked
            .execute_batch("INSERT INTO threads VALUES ('user', 'user', NULL); BEGIN EXCLUSIVE;")
            .unwrap();
        let start = std::time::Instant::now();
        assert!(usage_thread_identities(&dir).is_empty());
        assert!(start.elapsed() < Duration::from_secs(2));
        locked.execute_batch("ROLLBACK;").unwrap();
        assert!(!usage_thread_identities(&dir)["user"].is_subagent);
        drop(locked);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn usage_session_titles_follow_chat_title_fallbacks_and_only_requested_ids() {
        let codex_dir = temp_codex_dir("usage-title-fields");
        let conn = create_title_database(&codex_dir.join("state_10.sqlite"));
        conn.execute_batch(
            "INSERT INTO threads VALUES
             ('named', '  真实聊天标题  ', 'First message', 'Preview', '/projects/unused'),
             ('first', '   ', ' 首条用户消息 ', 'Preview', NULL),
             ('preview', NULL, ' ', '  会话摘要  ', NULL),
             ('project', NULL, NULL, NULL, 'C:\\work\\Codex-X\\'),
             ('invalid-title', X'FF', ' 可读取的首条消息 ', NULL, NULL),
             ('invalid-values', 42, 3.14, X'FF', NULL),
             ('root', NULL, NULL, NULL, '/'),
             ('excluded', 'This row was not requested', NULL, NULL, NULL);",
        )
        .unwrap();
        let ids = [
            "named",
            "first",
            "preview",
            "project",
            "invalid-title",
            "invalid-values",
            "root",
            "x' OR 1=1 --",
        ]
        .map(ToString::to_string);
        let titles = session_titles_by_id(&codex_dir, &ids);
        assert_eq!(titles.len(), 5);
        assert_eq!(titles["named"], "真实聊天标题");
        assert_eq!(titles["first"], "首条用户消息");
        assert_eq!(titles["preview"], "会话摘要");
        assert_eq!(titles["project"], "Codex-X");
        assert_eq!(titles["invalid-title"], "可读取的首条消息");
        assert!(!titles.contains_key("excluded"));
        assert!(!titles.contains_key("invalid-values"));
        assert!(!titles.contains_key("root"));
        drop(conn);
        fs::remove_dir_all(codex_dir).unwrap();
    }

    #[test]
    fn usage_session_titles_prefer_active_database_and_reread_renames() {
        let codex_dir = temp_codex_dir("usage-title-priority");
        let active = create_title_database(&codex_dir.join("state_10.sqlite"));
        let legacy = create_title_database(&codex_dir.join("sqlite/state_5.sqlite"));
        active
            .execute_batch(
                "INSERT INTO threads VALUES
            ('shared', 'Current title', NULL, NULL, NULL),
            ('fallback', ' ', NULL, NULL, '/projects/active-project'),
            ('project', NULL, NULL, NULL, '/projects/active-project');",
            )
            .unwrap();
        legacy
            .execute_batch(
                "INSERT INTO threads VALUES
            ('shared', 'Old title', NULL, NULL, NULL),
            ('fallback', 'Recovered chat title', NULL, NULL, '/projects/old-project'),
            ('project', NULL, NULL, NULL, '/projects/old-project');",
            )
            .unwrap();
        let ids = ["shared", "fallback", "project"].map(ToString::to_string);
        let titles = session_titles_by_id(&codex_dir, &ids);
        assert_eq!(titles["shared"], "Current title");
        assert_eq!(titles["fallback"], "Recovered chat title");
        assert_eq!(titles["project"], "active-project");
        active
            .execute(
                "UPDATE threads SET title = 'Renamed chat title' WHERE id = 'shared'",
                [],
            )
            .unwrap();
        assert_eq!(
            session_titles_by_id(&codex_dir, &ids)["shared"],
            "Renamed chat title"
        );
        drop(active);
        drop(legacy);
        fs::remove_dir_all(codex_dir).unwrap();
    }

    #[test]
    fn usage_session_titles_are_read_only_and_tolerate_missing_corrupt_or_locked_databases() {
        let codex_dir = temp_codex_dir("usage-title-read-only");
        let missing = codex_dir.join("missing");
        let ids = ["session".to_string()];
        assert!(session_titles_by_id(&missing, &ids).is_empty());
        assert!(!missing.exists());
        let path = codex_dir.join("state_10.sqlite");
        create_thread_database(&path, "session", "openai");
        fs::write(
            codex_dir.join("state_12.sqlite"),
            b"SQLite format 3\0broken fixture",
        )
        .unwrap();
        let before = fs::read(&path).unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        let original_permissions = fs::metadata(&path).unwrap().permissions();
        let mut permissions = original_permissions.clone();
        permissions.set_readonly(true);
        fs::set_permissions(&path, permissions.clone()).unwrap();
        assert_eq!(
            session_titles_by_id(&codex_dir, &ids)["session"],
            "test session"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        assert!(!codex_dir.join("state_10.sqlite-journal").exists());
        fs::set_permissions(&path, original_permissions).unwrap();
        let locked = Connection::open(&path).unwrap();
        locked.execute_batch("BEGIN EXCLUSIVE;").unwrap();
        let start = std::time::Instant::now();
        assert!(session_titles_by_id(&codex_dir, &ids).is_empty());
        assert!(start.elapsed() < Duration::from_secs(2));
        locked.execute_batch("ROLLBACK;").unwrap();
        drop(locked);
        fs::remove_dir_all(codex_dir).unwrap();
    }

    #[test]
    fn session_project_titles_support_platform_paths_and_reject_roots() {
        for (path, expected) in [
            ("/Users/test/projects/Codex-X/", Some("Codex-X")),
            (r"C:\work\Codex-X\", Some("Codex-X")),
            (r"\\?\C:\work\Codex-X", Some("Codex-X")),
            (r"\\server\share\project", Some("project")),
            ("relative/project", Some("project")),
            ("", None),
            ("  ", None),
            ("/", None),
            (r"C:\", None),
            ("C:", None),
            (r"\\server\share\", None),
            (".", None),
            ("..", None),
        ] {
            assert_eq!(session_project_title(path).as_deref(), expected, "{path}");
        }
    }

    #[test]
    fn root_state_10_honors_an_explicit_provider_target() {
        let codex_dir = temp_codex_dir("root-state-10");
        let database = codex_dir.join("state_10.sqlite");
        let id = "019f6000-0000-7000-8000-000000000301";
        fs::write(
            codex_dir.join("config.toml"),
            "model_provider = \"custom\"\n",
        )
        .expect("write shared provider config");
        create_thread_database(&database, id, "openai");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![database.clone()]);
        assert_eq!(discovery.thread_paths, vec![database.clone()]);
        let (sessions, warnings) = list_session_previews_with_paths(
            &discovery.session_paths,
            &RolloutScan::default(),
            "custom",
            50,
        )
        .expect("list state_10 session");
        assert!(warnings.is_empty());
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, id);

        let status = crate::sessions::sync::session_sync_status_inner(
            Some(codex_dir.display().to_string()),
            Some("openai".to_string()),
        )
        .expect("read explicit provider status");
        assert_eq!(status.target_provider, "openai");
        assert!(!status.needs_sync);

        let result = crate::sessions::sync::sync_sessions_provider_inner(
            Some(codex_dir.display().to_string()),
            Some("openai".to_string()),
        )
        .expect("sync root state_10");
        assert_eq!(result.status.target_provider, "openai");
        assert_eq!(result.updated_threads, 0);
        let provider: String = Connection::open(&database)
            .expect("reopen state_10")
            .query_row(
                "SELECT model_provider FROM threads WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("read updated provider");
        assert_eq!(provider, "openai");

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn root_codex_dev_database_is_active_without_state_database() {
        let codex_dir = temp_codex_dir("root-codex-dev");
        let database = codex_dir.join("codex-dev.db");
        create_thread_database(&database, "019f6000-0000-7000-8000-000000000302", "openai");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![database.clone()]);
        assert_eq!(discovery.thread_paths, vec![database]);

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn configured_sqlite_home_custom_database_is_active() {
        let codex_dir = temp_codex_dir("configured-custom-active");
        fs::write(
            codex_dir.join("config.toml"),
            "sqlite_home = \"session-store\"\n",
        )
        .expect("write sqlite_home config");
        let database = codex_dir.join("session-store/custom-name.db");
        let later_database = codex_dir.join("session-store/later-name.sqlite3");
        create_thread_database(&database, "019f6000-0000-7000-8000-000000000303", "openai");
        create_thread_database(
            &later_database,
            "019f6000-0000-7000-8000-000000000304",
            "openai",
        );

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![database.clone()]);
        assert_eq!(discovery.thread_paths, vec![database, later_database]);

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn configured_sqlite_home_prefers_codex_dev_before_custom_database() {
        let codex_dir = temp_codex_dir("configured-codex-dev-precedence");
        fs::write(
            codex_dir.join("config.toml"),
            "sqlite_home = \"session-store\"\n",
        )
        .expect("write sqlite_home config");
        let storage = codex_dir.join("session-store");
        let codex_dev = storage.join("codex-dev.db");
        let custom = storage.join("custom-name.db");
        create_thread_database(&codex_dev, "019f6000-0000-7000-8000-000000000305", "openai");
        create_thread_database(&custom, "019f6000-0000-7000-8000-000000000306", "openai");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![codex_dev]);

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn configured_sqlite_home_prefers_latest_state_database() {
        let codex_dir = temp_codex_dir("configured-state-precedence");
        fs::write(
            codex_dir.join("config.toml"),
            "sqlite_home = \"session-store\"\n",
        )
        .expect("write sqlite_home config");
        let storage = codex_dir.join("session-store");
        let custom = storage.join("custom-name.db");
        let codex_dev = storage.join("codex-dev.db");
        let state_5 = storage.join("state_5.sqlite");
        let state_10 = storage.join("state_10.sqlite");
        for (database, id) in [
            (&custom, "019f6000-0000-7000-8000-000000000307"),
            (&codex_dev, "019f6000-0000-7000-8000-000000000308"),
            (&state_5, "019f6000-0000-7000-8000-000000000309"),
            (&state_10, "019f6000-0000-7000-8000-000000000310"),
        ] {
            create_thread_database(database, id, "openai");
        }

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![state_10]);

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn custom_db_and_sqlite3_share_the_same_discovery() {
        let codex_dir = temp_codex_dir("custom-extensions");
        let custom_db = codex_dir.join("sqlite/custom.db");
        let custom_sqlite3 = codex_dir.join("sqlite/custom.sqlite3");
        create_thread_database(&custom_db, "019f6000-0000-7000-8000-000000000311", "openai");
        create_thread_database(
            &custom_sqlite3,
            "019f6000-0000-7000-8000-000000000312",
            "openai",
        );

        let discovery = discover_sqlite_databases(&codex_dir);
        let expected = HashSet::from([custom_db, custom_sqlite3]);
        assert_eq!(
            discovery.thread_paths.into_iter().collect::<HashSet<_>>(),
            expected
        );
        assert_eq!(
            discovery.session_paths.into_iter().collect::<HashSet<_>>(),
            expected
        );
        assert_eq!(
            discovery.related_paths.into_iter().collect::<HashSet<_>>(),
            expected
        );

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn invalid_state_database_is_ignored() {
        let codex_dir = temp_codex_dir("invalid-state");
        let invalid = codex_dir.join("state_5.sqlite");
        let valid = codex_dir.join("state_10.sqlite");
        fs::write(&invalid, b"not a sqlite database").expect("write invalid sqlite");
        create_thread_database(&valid, "019f6000-0000-7000-8000-000000000321", "openai");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.active_paths, vec![valid.clone()]);
        assert_eq!(discovery.thread_paths, vec![valid]);
        assert!(!discovery.session_paths.contains(&invalid));
        assert!(!discovery.related_paths.contains(&invalid));
        assert!(discovery.unreadable_paths.is_empty());

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn sqlite_header_with_unreadable_schema_blocks_mutation() {
        let codex_dir = temp_codex_dir("unreadable-schema");
        let unreadable = codex_dir.join("state_5.sqlite");
        fs::write(&unreadable, b"SQLite format 3\0").expect("write truncated sqlite");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.unreadable_paths, vec![unreadable]);
        let error = ensure_sqlite_discovery_writable(&discovery)
            .expect_err("unreadable sqlite must block mutation");
        assert_eq!(
            error.to_string(),
            "配置错误: 无法读取会话数据库，请关闭 Codex 后重试。"
        );

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn unrelated_root_sqlite_is_not_classified_as_codex_storage() {
        let codex_dir = temp_codex_dir("unrelated-root");
        let unrelated = codex_dir.join("unrelated.sqlite");
        create_thread_database(&unrelated, "019f6000-0000-7000-8000-000000000341", "openai");

        let discovery = discover_sqlite_databases(&codex_dir);
        assert!(!discovery.related_paths.contains(&unrelated));
        assert!(!discovery.session_paths.contains(&unrelated));
        assert!(!discovery.thread_paths.contains(&unrelated));

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn unrelated_custom_database_with_only_related_table_is_not_cleaned() {
        let codex_dir = temp_codex_dir("unrelated-custom");
        let unrelated = codex_dir.join("sqlite/unrelated.db");
        fs::create_dir_all(unrelated.parent().expect("sqlite parent"))
            .expect("create sqlite directory");
        let conn = Connection::open(&unrelated).expect("create unrelated custom sqlite");
        conn.execute("CREATE TABLE logs (thread_id TEXT)", [])
            .expect("create unrelated logs table");
        drop(conn);

        let discovery = discover_sqlite_databases(&codex_dir);
        assert!(!discovery.related_paths.contains(&unrelated));
        assert!(!discovery.session_paths.contains(&unrelated));

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn duplicate_preview_prefers_active_database_at_same_timestamp() {
        let codex_dir = temp_codex_dir("active-preview-priority");
        let active = codex_dir.join("state_10.sqlite");
        let legacy = codex_dir.join("sqlite/state_5.sqlite");
        let id = "019f6000-0000-7000-8000-000000000351";
        create_thread_database(&active, id, "openai");
        create_thread_database(&legacy, id, "openai");
        for (path, title) in [(&active, "active title"), (&legacy, "legacy title")] {
            Connection::open(path)
                .expect("open duplicate database")
                .execute(
                    "UPDATE threads SET title = ?1, updated_at_ms = 100 WHERE id = ?2",
                    (title, id),
                )
                .expect("update duplicate title");
        }

        let discovery = discover_sqlite_databases(&codex_dir);
        assert_eq!(discovery.session_paths, vec![legacy, active.clone()]);
        let previews = list_session_previews_with_paths(
            &discovery.active_first_session_paths(),
            &RolloutScan::default(),
            "openai",
            50,
        )
        .expect("list duplicate previews")
        .0;
        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0].title, "active title");

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn session_page_limits_pages_and_advances_without_duplicates() {
        for count in [100usize, 1_000, 5_000] {
            let codex_dir = temp_codex_dir(&format!("session-page-{count}"));
            let database = codex_dir.join("state_10.sqlite");
            create_page_database(&database, count);

            let first = get_session_page(&codex_dir, None, None, None, None, Some(true))
                .expect("read first session page");
            assert_eq!(first.sessions.len(), count.min(100));
            assert_eq!(first.total, count);
            assert_eq!(first.top_level, count - count.div_ceil(10));
            assert_eq!(first.subagent, count.div_ceil(10));

            let mut ids = HashSet::new();
            ids.extend(first.sessions.iter().map(|session| session.id.clone()));
            let mut page = first;
            while page.has_more {
                let cursor = page.next_cursor.clone().expect("next page cursor");
                page = get_session_page(
                    &codex_dir,
                    Some(cursor.updated_at_ms),
                    Some(cursor.id),
                    Some(100),
                    None,
                    Some(true),
                )
                .expect("read next session page");
                assert!(page.sessions.len() <= 100);
                for session in &page.sessions {
                    assert!(ids.insert(session.id.clone()), "duplicate {}", session.id);
                }
            }
            assert_eq!(ids.len(), count);
            assert!(!ids.is_empty());
            let _ = fs::remove_dir_all(codex_dir);
        }
    }

    #[test]
    fn session_page_search_uses_metadata_fields() {
        let codex_dir = temp_codex_dir("session-page-search");
        let database = codex_dir.join("state_10.sqlite");
        create_page_database(&database, 100);

        let page = get_session_page(
            &codex_dir,
            None,
            None,
            Some(100),
            Some("needle".to_string()),
            Some(true),
        )
        .expect("search session metadata");
        assert_eq!(page.total, 4);
        assert_eq!(page.sessions.len(), 4);
        assert!(page.sessions.iter().all(|session| {
            [
                "thread-00000",
                "thread-00025",
                "thread-00050",
                "thread-00075",
            ]
            .contains(&session.id.as_str())
        }));

        let _ = fs::remove_dir_all(codex_dir);
    }

    #[test]
    fn session_page_excludes_internal_threads_before_limit() {
        let codex_dir = temp_codex_dir("session-page-internal");
        let database = codex_dir.join("state_10.sqlite");
        create_page_database(&database, 250);
        let conn = Connection::open(&database).expect("open page database for edge fixture");
        conn.execute(
            "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id)
             VALUES ('thread-parent', 'thread-00001')",
            [],
        )
        .expect("insert edge-only subagent");
        drop(conn);

        let top_level = get_session_page(&codex_dir, None, None, Some(100), None, None)
            .expect("read top-level sessions");
        assert_eq!(top_level.sessions.len(), 100);
        assert!(top_level
            .sessions
            .iter()
            .all(|session| !session.is_subagent));
        assert_eq!(top_level.subagent, 26);

        let all = get_session_page(&codex_dir, None, None, Some(100), None, Some(true))
            .expect("read all sessions");
        assert_eq!(all.sessions.len(), 100);
        assert!(all.sessions.iter().any(|session| session.is_subagent));
        assert_eq!(all.total, 250);

        let _ = fs::remove_dir_all(codex_dir);
    }
}
