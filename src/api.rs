use axum::{
    Json, Router,
    extract::{RawQuery, State},
    http::{HeaderValue, Method, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use crate::recorder::{RecorderHandle, RecorderStats};

#[derive(Clone)]
struct AppState {
    recorder: RecorderHandle,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WatchRequest {
    #[serde(default)]
    token_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WatchResponse {
    changed: bool,
    #[serde(flatten)]
    stats: RecorderStats,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

pub fn router(recorder: RecorderHandle) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE]);

    Router::new()
        .route("/api/recorder/health", get(health))
        .route("/api/recorder/state", get(state))
        .route("/api/recorder/watch", post(watch))
        .layer(cors)
        .with_state(AppState { recorder })
}

async fn health(State(state): State<AppState>) -> Result<Json<RecorderStats>, ApiError> {
    Ok(Json(state.recorder.stats().await?))
}

async fn state(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, ApiError> {
    let query = StateQuery::parse(raw_query.as_deref());
    let state = state
        .recorder
        .state(query.token_ids, !query.metadata_only)
        .await?;

    let message = crate::proto::RecorderStateResponse {
        recording_since_ms_by_token: state
            .recording_since_ms_by_token
            .into_iter()
            .collect(),
        states: state
            .states
            .into_iter()
            .map(|(token_id, state)| {
                (
                    token_id,
                    crate::proto::TokenPressureState {
                        pressure: Some(state.pressure.to_proto()),
                    },
                )
            })
            .collect(),
        pending_token_ids: state.pending_token_ids,
    };

    let mut response = message.encode_to_vec().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    Ok(response)
}

async fn watch(
    State(state): State<AppState>,
    Json(body): Json<WatchRequest>,
) -> Result<Json<WatchResponse>, ApiError> {
    let (changed, stats) = state.recorder.watch(body.token_ids).await?;
    Ok(Json(WatchResponse { changed, stats }))
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
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
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
