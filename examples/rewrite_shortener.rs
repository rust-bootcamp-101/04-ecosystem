use anyhow::Result;
use axum::{
    extract::{Json, Path, State},
    http::{header::LOCATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use axum_macros::debug_handler;
use nanoid::nanoid;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgDatabaseError, FromRow, PgPool};
use std::{convert::Infallible, str::FromStr, sync::Arc};
use tokio::net::TcpListener;
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

const LISTEN_ADDR: &str = "0.0.0.0:9876";

#[tokio::main]
async fn main() -> Result<()> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    let postgres_uri = "postgres://liangchuan:postgres@localhost:5432/shortener";
    let state = AppState::try_new(postgres_uri).await?;
    info!("Connected to database: {postgres_uri}");
    let state = Arc::new(state);
    let listener = TcpListener::bind(LISTEN_ADDR).await?;
    info!("Listening on: {}", LISTEN_ADDR);

    let app = Router::new()
        .route("/", post(shorten))
        .route("/:id", get(redirect))
        .with_state(state);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

#[derive(Debug)]
struct AppState {
    db: PgPool,
}

impl AppState {
    async fn try_new(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        // Create table if not exists
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS urls (
                id CHAR(6) PRIMARY KEY,
                url TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&pool)
        .await?;
        Ok(Self { db: pool })
    }

    async fn shorten(&self, url: &str) -> Result<String> {
        let ret = shorten_with_conflict_retry(&self.db, url, 5, None).await?;
        Ok(ret.id)
    }

    async fn get_url(&self, id: &str) -> Result<String> {
        let record: UrlRecord = sqlx::query_as("SELECT url FROM urls WHERE id = $1")
            .bind(id)
            .fetch_one(&self.db)
            .await?;
        Ok(record.url)
    }
}

// =============================================================================
// 如果生成的 id 重复，而产生数据库错误，则重新生成一个 id，直到不再出错
// 增加重试次数
async fn shorten_with_conflict_retry(
    db: &PgPool,
    url: &str,
    retries: usize,
    default_id: Option<String>,
) -> Result<UrlRecord> {
    let mut retries = retries;
    loop {
        let id = match &default_id {
            Some(default_id) => default_id.clone(),
            None => nanoid!(6),
        };
        match sqlx::query_as(
            "INSERT INTO urls (id, url) VALUES ($1, $2) ON CONFLICT(url) DO UPDATE SET url=EXCLUDED.url RETURNING *",
        )
        .bind(id)
        .bind(url)
        .fetch_one(db)
        .await {
            Ok(ret) => return Ok(ret),
            Err(e) =>  {
                let app_error: AppError = e.into();
                match app_error {
                    // 如果是id重复，则重试
                    AppError::ConflictShortenerId(_) => {
                        retries -= 1;
                        if retries == 0 { // 重试次数达到
                            anyhow::bail!("Internal server error")
                        }
                    },
                    _ => anyhow::bail!("{}", app_error),
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ShortenReq {
    url: String,
}

#[derive(Debug, Serialize)]
struct ShortenRes {
    url: String,
}

#[derive(Debug, FromRow)]
struct UrlRecord {
    #[sqlx(default)]
    id: String,
    #[sqlx(default)]
    url: String,
}

// 注意: extract 中，Body只会被消费一次，所以必须放最后面
// 因为Json是实现了FromRequest，而其他的参数如header，method，path则实现了FromRequestPart，
// 它们是可以被取多次的
#[debug_handler] // 使用 debug_handler 检查handler函数的错误
async fn shorten(
    State(state): State<Arc<AppState>>,
    Json(data): Json<ShortenReq>,
) -> Result<Json<ShortenRes>, AppError> {
    let id = state.shorten(&data.url).await.map_err(|e| {
        warn!("Failed to shorten URL: {e}");
        AppError::UnprocessableEntity(data.url)
    })?; // 返回422表示 资源我认识，但我处理不了
    Ok(Json(ShortenRes {
        url: format!("http://{LISTEN_ADDR}/{id}"),
    }))
}

async fn redirect(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let url = state.get_url(&id).await.map_err(|_| AppError::NotFound)?;

    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, url.parse().unwrap());
    Ok((StatusCode::FOUND, headers))
}

// =============================================================================
// 使用 thiserror 进行错误处理
// 实现 IntoResponse

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("Conflict shortener id")]
    ConflictShortenerId(ShortenerIdConflictInfo),

    #[error("No url found by the given condition")]
    NotFound,

    #[error("Failed to shorten URL: {0}")]
    UnprocessableEntity(String),

    #[error("Database error")]
    DbError(sqlx::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, self.to_string()).into_response(),
            AppError::DbError(_) | AppError::ConflictShortenerId(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
            }
            AppError::UnprocessableEntity(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, msg).into_response()
            }
        }
    }
}

// =============================================================================
// 处理id重复错误

// 解析id冲突
#[derive(Debug, Clone, PartialEq, Eq)]
enum ShortenerIdConflictInfo {
    Parsed(ShortenerIdConflict),
    Unparsed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShortenerIdConflict {
    // id重复错误信息
    // detail: Some(
    //     "Key (id)=(123456) already exists.",
    // ),
    conflict_id: String, // 重复的id
}

impl FromStr for ShortenerIdConflict {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // 使用正则表达式解析错误信息, 提取detail中的信息
        // 错误信息如下
        // Database(
        //     PgDatabaseError {
        //         severity: Error,
        //         code: "23505",
        //         message: "duplicate key value violates unique constraint \"urls_pkey\"",
        //         detail: Some(
        //             "Key (id)=(123456) already exists.",
        //         ),
        //         hint: None,
        //         position: None,
        //         where: None,
        //         schema: Some(
        //             "public",
        //         ),
        //         table: Some(
        //             "urls",
        //         ),
        //         column: None,
        //         data_type: None,
        //         constraint: Some(
        //             "urls_pkey",
        //         ),
        //         file: Some(
        //             "nbtinsert.c",
        //         ),
        //         line: Some(
        //             673,
        //         ),
        //         routine: Some(
        //             "_bt_check_unique",
        //         ),
        //     },
        // )
        let re =
            Regex::new(r#"\((?P<k1>[a-zA-Z0-9_-]+)\s*\)=\((?P<v1>[a-zA-Z0-9_-]+)\s*\)"#).unwrap();
        let Some(cap) = re.captures(s) else {
            return Err(());
        };
        if cap["k1"] != *"id" {
            // TODO: 理论上不应该走到这步
            // unreachable!("理论上不应该走到这步")
            return Err(());
        }
        Ok(Self {
            conflict_id: cap["v1"].to_string(),
        })
    }
}

impl FromStr for ShortenerIdConflictInfo {
    // Infallible: 表示错误永远不会发生
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(conflict) = s.parse() {
            Ok(ShortenerIdConflictInfo::Parsed(conflict))
        } else {
            Ok(ShortenerIdConflictInfo::Unparsed(s.to_string()))
        }
    }
}
impl std::fmt::Display for ShortenerIdConflictInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::Database(e) => {
                let err: &PgDatabaseError = e.downcast_ref();
                match (err.code(), err.table()) {
                    // 匹配conflict编码，和表名
                    ("23505", Some("urls")) => AppError::ConflictShortenerId(
                        err.detail().unwrap().to_string().parse().unwrap(),
                    ),
                    _ => AppError::DbError(sqlx::Error::Database(e)),
                }
            }
            sqlx::Error::RowNotFound => AppError::NotFound,

            _ => AppError::DbError(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx_db_tester::TestPg;
    use std::path::Path;

    // 测试id重复错误
    #[tokio::test]
    async fn test_conflict_id() -> Result<()> {
        let tdb = TestPg::new(
            "postgres://postgres:postgres@localhost:5432".to_string(),
            Path::new("./"),
        );
        let pool = tdb.get_pool().await;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS urls (
                id CHAR(6) PRIMARY KEY,
                url TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&pool)
        .await?;

        let id = "123456".to_string();
        let url = "www.google.com".to_string();
        let ret: UrlRecord = sqlx::query_as(
            "INSERT INTO urls (id, url) VALUES ($1, $2) ON CONFLICT(url) DO UPDATE SET url=EXCLUDED.url RETURNING *",
        )
        .bind(&id)
        .bind(&url)
        .fetch_one(&pool)
        .await?;
        assert_eq!(ret.id, id.clone());
        assert_eq!(ret.url, url.clone());

        let ret: UrlRecord = sqlx::query_as(
            "INSERT INTO urls (id, url) VALUES ($1, $2) ON CONFLICT(url) DO UPDATE SET url=EXCLUDED.url RETURNING *",
        )
        .bind(&id)
        .bind(&url)
        .fetch_one(&pool)
        .await?;
        assert_eq!(ret.id, id.clone());
        assert_eq!(ret.url, url.clone());

        // 由于设置了 ON CONFLICT(url)
        // 所以只有当 url 不同且 id重复的情况下才会触发 conflict 错误
        let url = "www.abc.com".to_string();
        let Err(e): std::result::Result<(), sqlx::Error> = sqlx::query_as(
            "INSERT INTO urls (id, url) VALUES ($1, $2) ON CONFLICT(url) DO UPDATE SET url=EXCLUDED.url",
        )
        .bind(&id)
        .bind(&url)
        .fetch_one(&pool)
        .await else {
            panic!("should be error");
        };
        let conflict_err: AppError = e.into();
        let AppError::ConflictShortenerId(conflict_shortener_id) = conflict_err else {
            panic!("should be conflict error");
        };
        match conflict_shortener_id {
            ShortenerIdConflictInfo::Parsed(c) => assert_eq!(c.conflict_id, id),
            ShortenerIdConflictInfo::Unparsed(s) => {
                panic!("{}", format!("unparsed conflict error {s}"))
            }
        }
        Ok(())
    }

    // 测试id错误重试
    #[tokio::test]
    async fn test_conflict_id_with_retries() -> Result<()> {
        let tdb = TestPg::new(
            "postgres://postgres:postgres@localhost:5432".to_string(),
            Path::new("./"),
        );
        let pool = tdb.get_pool().await;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS urls (
                id CHAR(6) PRIMARY KEY,
                url TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&pool)
        .await?;

        let id = "123456".to_string();
        let url = "www.google.com".to_string();
        let ret = shorten_with_conflict_retry(&pool, &url, 1, Some(id.clone())).await?;
        assert_eq!(ret.id, id.clone());
        assert_eq!(ret.url, url);

        // 使用重复id 测试重复id错误
        let url = "www.abc.com".to_string();
        let Err(_) = shorten_with_conflict_retry(&pool, &url, 2, Some(id)).await else {
            panic!("should be error")
        };

        // 重新生成id
        let ret = shorten_with_conflict_retry(&pool, &url, 2, None).await?;
        assert!(!ret.id.is_empty());
        assert_eq!(ret.url, url);
        Ok(())
    }
}
