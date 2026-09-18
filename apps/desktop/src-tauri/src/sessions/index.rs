#[cfg(not(test))]
use crate::app_db;
use crate::error::CodexxError;
use crate::error::Result;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub(super) struct RolloutIndexEntry {
    pub(super) jsonl_path: String,
    pub(super) file_mtime_ns: i64,
    pub(super) file_size: i64,
    pub(super) session_id: String,
    pub(super) title: Option<String>,
    pub(super) cwd: Option<String>,
    pub(super) model_provider: Option<String>,
    pub(super) session_type: Option<String>,
    pub(super) parent_session_id: Option<String>,
    pub(super) updated_at_ms: i64,
}

fn database_error(error: rusqlite::Error) -> CodexxError {
    CodexxError::Database(error.to_string())
}

pub(super) fn system_time_ns(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or_default()
}

fn load_rollout_index_from_connection(
    conn: &Connection,
    codex_dir: &str,
) -> Result<HashMap<String, RolloutIndexEntry>> {
    let mut statement = conn
        .prepare(
            "SELECT jsonl_path, file_mtime_ns, file_size, session_id, title, cwd,
                    model_provider, session_type, parent_session_id, updated_at_ms
             FROM session_rollout_index WHERE codex_dir = ?1",
        )
        .map_err(database_error)?;
    let rows = statement
        .query_map([codex_dir], |row| {
            Ok(RolloutIndexEntry {
                jsonl_path: row.get(0)?,
                file_mtime_ns: row.get(1)?,
                file_size: row.get(2)?,
                session_id: row.get(3)?,
                title: row.get(4)?,
                cwd: row.get(5)?,
                model_provider: row.get(6)?,
                session_type: row.get(7)?,
                parent_session_id: row.get(8)?,
                updated_at_ms: row.get(9)?,
            })
        })
        .map_err(database_error)?;
    let mut entries = HashMap::new();
    for row in rows {
        let entry = row.map_err(database_error)?;
        entries.insert(entry.jsonl_path.clone(), entry);
    }
    Ok(entries)
}

#[cfg(not(test))]
pub(super) fn load_rollout_index(codex_dir: &Path) -> Result<HashMap<String, RolloutIndexEntry>> {
    let conn = app_db::open()?;
    load_rollout_index_from_connection(&conn, &codex_dir.display().to_string())
}

#[cfg(test)]
pub(super) fn load_rollout_index(_codex_dir: &Path) -> Result<HashMap<String, RolloutIndexEntry>> {
    Ok(HashMap::new())
}

fn persist_rollout_index_on_connection(
    conn: &mut Connection,
    codex_dir: &str,
    changed: &[RolloutIndexEntry],
    present_paths: &HashSet<String>,
    indexed_at: i64,
) -> Result<()> {
    let transaction = conn.transaction().map_err(database_error)?;
    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO session_rollout_index (
                    codex_dir, jsonl_path, file_mtime_ns, file_size, session_id,
                    title, cwd, model_provider, session_type, parent_session_id,
                    updated_at_ms, last_indexed_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(codex_dir, jsonl_path) DO UPDATE SET
                    file_mtime_ns = excluded.file_mtime_ns,
                    file_size = excluded.file_size,
                    session_id = excluded.session_id,
                    title = excluded.title,
                    cwd = excluded.cwd,
                    model_provider = excluded.model_provider,
                    session_type = excluded.session_type,
                    parent_session_id = excluded.parent_session_id,
                    updated_at_ms = excluded.updated_at_ms,
                    last_indexed_at = excluded.last_indexed_at",
            )
            .map_err(database_error)?;
        for entry in changed {
            statement
                .execute(params![
                    codex_dir,
                    entry.jsonl_path,
                    entry.file_mtime_ns,
                    entry.file_size,
                    entry.session_id,
                    entry.title,
                    entry.cwd,
                    entry.model_provider,
                    entry.session_type,
                    entry.parent_session_id,
                    entry.updated_at_ms,
                    indexed_at,
                ])
                .map_err(database_error)?;
        }
    }

    let cached_paths = {
        let mut statement = transaction
            .prepare("SELECT jsonl_path FROM session_rollout_index WHERE codex_dir = ?1")
            .map_err(database_error)?;
        let paths = statement
            .query_map([&codex_dir], |row| row.get::<_, String>(0))
            .map_err(database_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(database_error)?;
        paths
    };
    let mut delete = transaction
        .prepare("DELETE FROM session_rollout_index WHERE codex_dir = ?1 AND jsonl_path = ?2")
        .map_err(database_error)?;
    for path in cached_paths {
        if !present_paths.contains(&path) {
            delete
                .execute(params![codex_dir, path])
                .map_err(database_error)?;
        }
    }
    drop(delete);
    transaction.commit().map_err(database_error)
}

#[cfg(not(test))]
pub(super) fn persist_rollout_index(
    codex_dir: &Path,
    changed: &[RolloutIndexEntry],
    present_paths: &HashSet<String>,
) -> Result<()> {
    let mut conn = app_db::open()?;
    let indexed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or_default();
    persist_rollout_index_on_connection(
        &mut conn,
        &codex_dir.display().to_string(),
        changed,
        present_paths,
        indexed_at,
    )
}

#[cfg(test)]
pub(super) fn persist_rollout_index(
    _codex_dir: &Path,
    _changed: &[RolloutIndexEntry],
    _present_paths: &HashSet<String>,
) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("open rollout index test database");
        conn.execute_batch(
            "CREATE TABLE session_rollout_index (
                codex_dir TEXT NOT NULL,
                jsonl_path TEXT NOT NULL,
                file_mtime_ns INTEGER NOT NULL,
                file_size INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                title TEXT,
                cwd TEXT,
                model_provider TEXT,
                session_type TEXT,
                parent_session_id TEXT,
                updated_at_ms INTEGER NOT NULL,
                last_indexed_at INTEGER NOT NULL,
                PRIMARY KEY(codex_dir, jsonl_path)
            );",
        )
        .expect("create rollout index table");
        conn
    }

    fn entry(path: &str, id: &str, size: i64) -> RolloutIndexEntry {
        RolloutIndexEntry {
            jsonl_path: path.to_string(),
            file_mtime_ns: 123,
            file_size: size,
            session_id: id.to_string(),
            title: Some(format!("Title {id}")),
            cwd: Some("C:/workspace".to_string()),
            model_provider: Some("openai".to_string()),
            session_type: Some("main".to_string()),
            parent_session_id: None,
            updated_at_ms: 456,
        }
    }

    #[test]
    fn rollout_index_upserts_loads_and_prunes_missing_paths() {
        let mut conn = test_connection();
        let first = entry("C:/codex/sessions/one.jsonl", "one", 10);
        let second = entry("C:/codex/sessions/two.jsonl", "two", 20);
        let all_paths = HashSet::from([first.jsonl_path.clone(), second.jsonl_path.clone()]);
        persist_rollout_index_on_connection(
            &mut conn,
            "C:/codex",
            &[first.clone(), second.clone()],
            &all_paths,
            1,
        )
        .expect("persist initial rollout index");

        let loaded = load_rollout_index_from_connection(&conn, "C:/codex")
            .expect("load initial rollout index");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&first.jsonl_path].session_id, "one");
        assert_eq!(loaded[&second.jsonl_path].file_size, 20);

        let updated = entry("C:/codex/sessions/one.jsonl", "one", 99);
        let remaining_paths = HashSet::from([updated.jsonl_path.clone()]);
        persist_rollout_index_on_connection(
            &mut conn,
            "C:/codex",
            &[updated.clone()],
            &remaining_paths,
            2,
        )
        .expect("update and prune rollout index");

        let loaded = load_rollout_index_from_connection(&conn, "C:/codex")
            .expect("load updated rollout index");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&updated.jsonl_path].file_size, 99);
        let indexed_at: i64 = conn
            .query_row(
                "SELECT last_indexed_at FROM session_rollout_index WHERE codex_dir = ?1",
                ["C:/codex"],
                |row| row.get(0),
            )
            .expect("read index timestamp");
        assert_eq!(indexed_at, 2);
    }

    #[test]
    fn rollout_index_is_scoped_by_codex_directory() {
        let mut conn = test_connection();
        let shared_path = "C:/shared/session.jsonl";
        let alpha = entry(shared_path, "alpha", 10);
        let beta = entry(shared_path, "beta", 20);
        let present = HashSet::from([shared_path.to_string()]);
        persist_rollout_index_on_connection(&mut conn, "C:/alpha", &[alpha], &present, 1)
            .expect("persist alpha index");
        persist_rollout_index_on_connection(&mut conn, "C:/beta", &[beta], &present, 1)
            .expect("persist beta index");

        let alpha =
            load_rollout_index_from_connection(&conn, "C:/alpha").expect("load alpha index");
        let beta = load_rollout_index_from_connection(&conn, "C:/beta").expect("load beta index");
        assert_eq!(alpha[shared_path].session_id, "alpha");
        assert_eq!(beta[shared_path].session_id, "beta");
    }
}
