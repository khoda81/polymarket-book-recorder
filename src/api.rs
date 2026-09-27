use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{RawQuery, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tower_http::cors::{Any, CorsLayer};

use crate::{
    pressure::PressureFrontierSnapshot,
    store::{RecorderStore, RecorderStoreStats},
};

#[derive(Clone)]
struct AppState {
    store: Arc<RecorderStore>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    watched_tokens: u64,
    completed_tokens: u64,
    hydrated_tokens: u64,
    live_books: u64,
    subscription_connections: u64,
    subscription_batches: u64,
    dirty_tokens: u64,
    pending_pressure_mutations: u64,
    pressure_tokens: u64,
    pressure_log_mutations: u64,
    database_path: String,
    mode: &'static str,
}

impl From<RecorderStoreStats> for HealthResponse {
    fn from(stats: RecorderStoreStats) -> Self {
        Self {
            watched_tokens: stats.watched_tokens,
            completed_tokens: stats.completed_tokens,
            hydrated_tokens: 0,
            live_books: 0,
            subscription_connections: 0,
            subscription_batches: 0,
            dirty_tokens: 0,
            pending_pressure_mutations: 0,
            pressure_tokens: stats.pressure_tokens,
            pressure_log_mutations: stats.pressure_log_mutations,
            database_path: stats.database_path,
            mode: "v5-compatibility-reader",
        }
    }
}

#[derive(Debug, Serialize)]
struct TransportState {
    pressure: PressureFrontierSnapshot,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateResponse {
    recording_since_ms_by_token: BTreeMap<String, i64>,
    states: BTreeMap<String, TransportState>,
    pending_token_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

pub fn router(store: Arc<RecorderStore>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE]);

    Router::new()
        .route("/api/recorder/health", get(health))
        .route("/api/recorder/state", get(state))
        .route("/api/recorder/watch", post(watch_not_yet_ported))
        .layer(cors)
        .with_state(AppState { store })
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    Ok(Json(state.store.stats()?.into()))
}

async fn state(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<StateResponse>, ApiError> {
    let query = StateQuery::parse(raw_query.as_deref());

    let mut recording_since_ms_by_token = BTreeMap::new();
    let mut states = BTreeMap::new();
    let mut pending_token_ids = Vec::new();

    for token_id in query.token_ids {
        match state.store.load(&token_id)? {
            Some(record) => {
                if let (Some(since), Some(pressure)) = (record.recording_since_ms, record.pressure)
                {
                    recording_since_ms_by_token.insert(token_id.clone(), since);
                    if !query.metadata_only {
                        states.insert(token_id, TransportState { pressure });
                    }
                } else if matches!(record.status, crate::store::RecorderTokenStatus::Watched) {
                    pending_token_ids.push(token_id);
                }
            }
            None => {
                // The live recorder will create/watch these once ingestion is
                // ported. Until then, report the same useful frontend state:
                // requested but not hydrated yet.
                pending_token_ids.push(token_id);
            }
        }
    }

    Ok(Json(StateResponse {
        recording_since_ms_by_token,
        states,
        pending_token_ids,
    }))
}

async fn watch_not_yet_ported() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorBody {
            error: "live Polymarket ingestion is the next porting milestone".to_owned(),
        }),
    )
}

#[derive(Debug, Default)]
struct StateQuery {
    token_ids: Vec<String>,
    metadata_only: bool,
}

impl StateQuery {
    fn parse(raw: Option<&str>) -> Self {
        let mut token_ids = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut metadata_only = false;

        for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                "tokenId" => {
                    for token_id in value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        if seen.insert(token_id.to_owned()) {
                            token_ids.push(token_id.to_owned());
                        }
                    }
                }
                "metadataOnly" => metadata_only = value == "1",
                _ => {}
            }
        }

        Self {
            token_ids,
            metadata_only,
        }
    }
}

struct ApiError(anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::error!(error = ?self.0, "recorder request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::StateQuery;

    #[test]
    fn state_query_accepts_repeated_and_comma_separated_tokens() {
        let query = StateQuery::parse(Some("tokenId=a,b&tokenId=b&tokenId=c&metadataOnly=1"));
        assert_eq!(query.token_ids, ["a", "b", "c"]);
        assert!(query.metadata_only);
    }
}
