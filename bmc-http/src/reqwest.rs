// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementation of [`HttpClient`] trait using reqwest crate.

use crate::BmcCredentials;
use crate::CacheableError;
use crate::HttpClient;
use crate::MultipartHttpClient;
use crate::MultipartUploadFile;
use crate::MultipartUploadReader;
use crate::MultipartUploadResponse;
use crate::RawFileUploadHttpClient;
use crate::RawJsonHttpClient;
use crate::RawUploadResponse;
use crate::RedfishUriError;
use crate::UploadError;
use crate::UploadFile;
use crate::UploadReader;
use futures_util::StreamExt as _;
use http::header;
use http::HeaderMap;
use nv_redfish_core::AsyncTask;
use nv_redfish_core::BoxTryStream;
use nv_redfish_core::ModificationResponse;
use nv_redfish_core::ODataETag;
use nv_redfish_core::ODataId;
use nv_redfish_core::SessionCreateResponse;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use reqwest::redirect::Policy as RedirectPolicy;
use reqwest::Client as ReqwestClient;
use reqwest::Error as ReqwestError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use url::Url;

const JSON_MIME: &str = "application/json";
const OCTET_STREAM_MIME: &str = "application/octet-stream";
const UNREADABLE_ERROR_BODY: &str = "<no data>";
const UPDATE_FILE_PART_NAME: &str = "UpdateFile";
const UPDATE_PARAMETERS_PART_NAME: &str = "UpdateParameters";

/// Errors of reqwest implementation of the HTTP trait.
#[derive(Debug)]
pub enum BmcError {
    /// Direct mapping of underlying reqwest error.
    ReqwestError(reqwest::Error),
    /// JSON to model deserialize error with path tracking.
    JsonError(serde_path_to_error::Error<serde_json::Error>),
    /// Unexpected HTTP response.
    InvalidResponse {
        /// URL in request that caused error.
        url: url::Url,
        /// Returned status.
        status: reqwest::StatusCode,
        /// Text in the response.
        text: String,
    },
    /// SSE stream error.
    SseStreamError(sse_stream::Error),
    /// No resource found in cache.
    CacheMiss,
    /// HTTP cache error.
    CacheError(String),
    /// Caller supplied URI failed Redfish endpoint validation.
    InvalidRedfishUri(RedfishUriError),
    /// JSON deserialization error.
    DecodeError(serde_json::Error),
}

impl From<reqwest::Error> for BmcError {
    fn from(value: reqwest::Error) -> Self {
        Self::ReqwestError(value)
    }
}

impl CacheableError for BmcError {
    fn is_cached(&self) -> bool {
        match self {
            Self::InvalidResponse { status, .. } => status == &reqwest::StatusCode::NOT_MODIFIED,
            _ => false,
        }
    }

    fn cache_miss() -> Self {
        Self::CacheMiss
    }

    fn cache_error(reason: String) -> Self {
        Self::CacheError(reason)
    }
}

impl From<RedfishUriError> for BmcError {
    fn from(error: RedfishUriError) -> Self {
        Self::InvalidRedfishUri(error)
    }
}

impl fmt::Display for BmcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReqwestError(e) => write!(f, "HTTP client error: {e:?}"),
            Self::InvalidResponse { url, status, text } => {
                write!(
                    f,
                    "Invalid HTTP response - url: {url} status: {status} text: {text}"
                )
            }
            Self::CacheMiss => write!(f, "Resource not found in cache"),
            Self::CacheError(r) => write!(f, "Error occurred in cache {r:?}"),
            Self::InvalidRedfishUri(e) => write!(f, "Invalid Redfish URI: {e}"),
            Self::JsonError(e) => write!(
                f,
                "JSON deserialization error at line {} column {} path {}: {e}",
                e.inner().line(),
                e.inner().column(),
                e.path(),
            ),
            Self::SseStreamError(e) => write!(f, "SSE stream decode error: {e}"),
            Self::DecodeError(e) => write!(f, "JSON Decode error: {e}"),
        }
    }
}

impl StdError for BmcError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::ReqwestError(e) => Some(e),
            Self::JsonError(e) => Some(e.inner()),
            Self::SseStreamError(e) => Some(e),
            Self::InvalidRedfishUri(e) => Some(e),
            Self::DecodeError(e) => Some(e),
            _ => None,
        }
    }
}

/// Configuration parameters for the reqwest HTTP client.
///
/// This struct allows customizing various aspects of the reqwest client behavior,
/// including timeouts, TLS settings, and connection pooling.
///
/// # Examples
///
/// ```rust
/// use nv_redfish_bmc_http::reqwest::ClientParams;
/// use std::time::Duration;
///
/// let params = ClientParams::new()
///     .timeout(Duration::from_secs(30))
///     .connect_timeout(Duration::from_secs(10))
///     .user_agent("MyApp/1.0")
///     .accept_invalid_certs(true);
/// ```
#[derive(Debug, Clone)]
pub struct ClientParams {
    /// HTTP request timeout
    pub timeout: Option<Duration>,
    /// TCP connection timeout
    pub connect_timeout: Option<Duration>,
    /// User-Agent header value
    pub user_agent: Option<String>,
    /// Whether to accept invalid TLS certificates
    pub accept_invalid_certs: bool,
    /// Maximum number of HTTP redirects to follow
    pub max_redirects: Option<usize>,
    /// TCP keep-alive timeout
    pub tcp_keepalive: Option<Duration>,
    /// Connection pool idle timeout
    pub pool_idle_timeout: Option<Duration>,
    /// Maximum idle connections per host
    pub pool_max_idle_per_host: Option<usize>,
    /// List of default headers, added to every request
    pub default_headers: Option<HeaderMap>,
    /// Forces use of rust TLS, enabled by default
    pub use_rust_tls: bool,
}

impl Default for ClientParams {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(120)),
            connect_timeout: Some(Duration::from_secs(5)),
            user_agent: Some("nv-redfish/v1".to_string()),
            accept_invalid_certs: false,
            max_redirects: Some(10),
            tcp_keepalive: Some(Duration::from_secs(60)),
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_idle_per_host: Some(1),
            default_headers: None,
            use_rust_tls: true,
        }
    }
}

impl ClientParams {
    /// Creates new client parameters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// See: [`reqwest::ClientBuilder::timeout`].
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// See: [`reqwest::ClientBuilder::connect_timeout`].
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// See: [`reqwest::ClientBuilder::user_agent`].
    #[must_use]
    pub fn user_agent<S: Into<String>>(mut self, user_agent: S) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// See: [`reqwest::ClientBuilder::danger_accept_invalid_certs`].
    #[must_use]
    pub const fn accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// See: [`reqwest::ClientBuilder::redirect`].
    #[must_use]
    pub const fn max_redirects(mut self, max: usize) -> Self {
        self.max_redirects = Some(max);
        self
    }

    /// See: [`reqwest::ClientBuilder::tcp_keepalive`].
    #[must_use]
    pub const fn tcp_keepalive(mut self, keepalive: Duration) -> Self {
        self.tcp_keepalive = Some(keepalive);
        self
    }

    /// See: [`reqwest::ClientBuilder::pool_max_idle_per_host`].
    #[must_use]
    pub const fn pool_max_idle_per_host(mut self, pool_max_idle_per_host: usize) -> Self {
        self.pool_max_idle_per_host = Some(pool_max_idle_per_host);
        self
    }

    /// See: [`reqwest::ClientBuilder::pool_idle_timeout`].
    #[must_use]
    pub const fn idle_timeout(mut self, pool_idle_timeout: Duration) -> Self {
        self.pool_idle_timeout = Some(pool_idle_timeout);
        self
    }

    /// Clears timeout for this client.
    #[must_use]
    pub const fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// See: [`reqwest::ClientBuilder::default_headers`].
    #[must_use]
    pub fn default_headers(mut self, default_headers: HeaderMap) -> Self {
        self.default_headers = Some(default_headers);
        self
    }
}

/// HTTP client implementation using the reqwest library.
///
/// This provides a concrete implementation of [`HttpClient`] using the popular
/// reqwest HTTP client library. It supports all standard HTTP features including
/// TLS, authentication, and connection pooling.
///
#[derive(Clone)]
pub struct Client {
    client: ReqwestClient,
}

impl Client {
    /// Create client with default [`ClientParams`].
    ///
    /// # Errors
    ///
    /// Internally it builds [`reqwest::ClientBuilder::build`]. This function
    /// transparently passes errors of this call to caller.
    pub fn new() -> Result<Self, ReqwestError> {
        Self::with_params(ClientParams::default())
    }

    /// Build client from parameters.
    ///
    /// # Errors
    ///
    /// Internally it builds [`reqwest::ClientBuilder::build`]. This function
    /// transparently passes errors of this call to caller.
    pub fn with_params(params: ClientParams) -> Result<Self, reqwest::Error> {
        let mut builder = ReqwestClient::builder();

        if params.use_rust_tls {
            builder = builder.use_rustls_tls();
        }

        if let Some(timeout) = params.timeout {
            builder = builder.timeout(timeout);
        }

        if let Some(connect_timeout) = params.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }

        if let Some(user_agent) = params.user_agent {
            builder = builder.user_agent(user_agent);
        }

        if params.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(max_redirects) = params.max_redirects {
            builder = builder.redirect(RedirectPolicy::limited(max_redirects));
        }

        if let Some(keepalive) = params.tcp_keepalive {
            builder = builder.tcp_keepalive(keepalive);
        }

        if let Some(idle_timeout) = params.pool_idle_timeout {
            builder = builder.pool_idle_timeout(idle_timeout);
        }

        if let Some(max_idle) = params.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(max_idle);
        }

        if let Some(default_headers) = params.default_headers {
            builder = builder.default_headers(default_headers);
        }

        Ok(Self {
            client: builder.build()?,
        })
    }

    /// Use pre-built [`reqwest::Client`] as internal client.
    #[must_use]
    pub const fn with_client(client: ReqwestClient) -> Self {
        Self { client }
    }
}

impl crate::HttpBmc<Client> {
    /// POST a Redfish UpdateService multipart upload with `UpdateFile` read from a file path.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, the multipart body cannot be
    /// built, the request fails, or the BMC returns an unsuccessful HTTP status.
    pub async fn post_update_multipart_from_path<P, V>(
        &self,
        multipart_uri: &str,
        update_parameters: &V,
        update_file: P,
        upload_timeout: Duration,
    ) -> Result<MultipartUploadResponse, UploadError<BmcError>>
    where
        P: AsRef<Path>,
        V: Serialize + Send + Sync,
    {
        let update_file = multipart_upload_file_from_path(update_file.as_ref())
            .await
            .map_err(UploadError::File)?;

        self.post_update_multipart_file(
            multipart_uri,
            update_parameters,
            update_file,
            upload_timeout,
        )
        .await
        .map_err(UploadError::Request)
    }

    /// PUT a raw Redfish update file read from a file path.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, the request fails, or the
    /// BMC returns an unsuccessful HTTP status.
    pub async fn put_update_file_from_path<P>(
        &self,
        update_uri: &str,
        update_file: P,
        upload_timeout: Duration,
    ) -> Result<RawUploadResponse, UploadError<BmcError>>
    where
        P: AsRef<Path>,
    {
        let update_file = upload_file_from_path(update_file.as_ref())
            .await
            .map_err(UploadError::File)?;

        self.put_update_file(update_uri, update_file, upload_timeout)
            .await
            .map_err(UploadError::Request)
    }
}

impl Client {
    async fn handle_response<T>(&self, response: reqwest::Response) -> Result<T, BmcError>
    where
        T: DeserializeOwned,
    {
        if !response.status().is_success() {
            return Err(BmcError::InvalidResponse {
                url: response.url().clone(),
                status: response.status(),
                text: response
                    .text()
                    .await
                    .unwrap_or_else(|_| UNREADABLE_ERROR_BODY.into()),
            });
        }

        let headers = response.headers().clone();

        let etag_header = etag_from_headers(&headers);

        let mut value: serde_json::Value = response.json().await.map_err(BmcError::ReqwestError)?;

        if let Some(etag) = etag_header {
            inject_etag(&etag, &mut value);
        }

        serde_path_to_error::deserialize(value).map_err(BmcError::JsonError)
    }

    async fn handle_json_response(
        &self,
        response: reqwest::Response,
    ) -> Result<serde_json::Value, BmcError> {
        let status = response.status();
        let url = response.url().clone();

        if !status.is_success() {
            return Err(BmcError::InvalidResponse {
                url,
                status,
                text: response
                    .text()
                    .await
                    .unwrap_or_else(|_| UNREADABLE_ERROR_BODY.into()),
            });
        }

        let bytes = response.bytes().await.map_err(BmcError::ReqwestError)?;

        json_value_from_bytes(&bytes)
    }

    async fn handle_modification_response<T>(
        &self,
        response: reqwest::Response,
    ) -> Result<ModificationResponse<T>, BmcError>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let status = response.status();
        let url = response.url().clone();
        let headers = response.headers().clone();
        if !status.is_success() {
            return Err(BmcError::InvalidResponse {
                url,
                status,
                text: response
                    .text()
                    .await
                    .unwrap_or_else(|_| UNREADABLE_ERROR_BODY.into()),
            });
        }

        let etag = etag_from_headers(&headers);
        let location = location_from_headers(&headers);

        match status {
            reqwest::StatusCode::NO_CONTENT => Ok(ModificationResponse::Empty),
            reqwest::StatusCode::ACCEPTED => {
                let Some(task_monitor_id) = location else {
                    return Err(BmcError::InvalidResponse {
                        url,
                        status,
                        text: String::from("202 Accepted without Location header"),
                    });
                };

                Ok(ModificationResponse::Task(AsyncTask {
                    id: task_monitor_id,
                    retry_after_secs: retry_after_from_headers(&headers),
                }))
            }
            reqwest::StatusCode::OK | reqwest::StatusCode::CREATED => {
                let bytes = response.bytes().await.map_err(BmcError::ReqwestError)?;

                if let Some(entity) = modification_entity_from_odata_body(&bytes, etag.as_ref())? {
                    return Ok(entity);
                }

                if let Some(location) = location {
                    let value = serde_json::json!({ "@odata.id": location });

                    return serde_path_to_error::deserialize(value)
                        .map(ModificationResponse::Entity)
                        .map_err(BmcError::JsonError);
                }

                Ok(ModificationResponse::Empty)
            }
            _ => Err(BmcError::InvalidResponse {
                url,
                status,
                text: format!("Unexpected successful status code: {status}"),
            }),
        }
    }

    async fn handle_session_response<T>(
        &self,
        response: reqwest::Response,
    ) -> Result<SessionCreateResponse<T>, BmcError>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let status = response.status();
        let url = response.url().clone();
        let headers = response.headers().clone();
        if !status.is_success() {
            return Err(BmcError::InvalidResponse {
                url,
                status,
                text: response
                    .text()
                    .await
                    .unwrap_or_else(|_| UNREADABLE_ERROR_BODY.into()),
            });
        }

        let Some(auth_token) = auth_token_from_headers(&headers) else {
            return Err(BmcError::InvalidResponse {
                url,
                status,
                text: String::from("session creation response missing X-Auth-Token header"),
            });
        };
        let Some(location) = location_from_headers(&headers) else {
            return Err(BmcError::InvalidResponse {
                url,
                status,
                text: String::from("session creation response missing Location header"),
            });
        };

        match status {
            reqwest::StatusCode::OK | reqwest::StatusCode::CREATED => {
                let etag = etag_from_headers(&headers);
                let bytes = response.bytes().await.map_err(BmcError::ReqwestError)?;
                if bytes.is_empty() {
                    return Err(BmcError::InvalidResponse {
                        url,
                        status,
                        text: String::from("session creation response missing entity body"),
                    });
                }

                let mut value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(BmcError::DecodeError)?;
                if let Some(etag) = etag {
                    inject_etag(&etag, &mut value);
                }
                let entity =
                    serde_path_to_error::deserialize(value).map_err(BmcError::JsonError)?;

                Ok(SessionCreateResponse {
                    entity,
                    auth_token,
                    location,
                })
            }
            reqwest::StatusCode::ACCEPTED => Err(BmcError::InvalidResponse {
                url,
                status,
                text: String::from("session creation returned 202 Accepted without session entity"),
            }),
            reqwest::StatusCode::NO_CONTENT => Err(BmcError::InvalidResponse {
                url,
                status,
                text: String::from("session creation returned 204 No Content"),
            }),
            _ => Err(BmcError::InvalidResponse {
                url,
                status,
                text: format!("Unexpected successful status code for session creation: {status}"),
            }),
        }
    }
}

fn update_multipart_form<R, V>(
    update_parameters: &V,
    update_file: MultipartUploadFile<R>,
) -> Result<Form, BmcError>
where
    R: MultipartUploadReader,
    V: Serialize + Send + Sync,
{
    let update_parameters_json =
        serde_json::to_vec(update_parameters).map_err(BmcError::DecodeError)?;

    let update_parameters_part = part_with_mime(Part::bytes(update_parameters_json), JSON_MIME)?;

    let (file_name, reader, content_length) = update_file.into_parts();
    let body = reqwest::Body::wrap_stream(ReaderStream::new(reader));
    let file_part = match content_length {
        Some(length) => Part::stream_with_length(body, length),
        None => Part::stream(body),
    };

    let file_part = part_with_mime(file_part.file_name(file_name), OCTET_STREAM_MIME)?;

    Ok(Form::new()
        .part(UPDATE_PARAMETERS_PART_NAME, update_parameters_part)
        .part(UPDATE_FILE_PART_NAME, file_part))
}

fn raw_upload_body<R>(update_file: UploadFile<R>) -> (reqwest::Body, Option<u64>)
where
    R: UploadReader,
{
    let (reader, content_length) = update_file.into_parts();
    let body = reqwest::Body::wrap_stream(ReaderStream::new(reader));

    (body, content_length)
}

async fn handle_upload_response(
    response: reqwest::Response,
) -> Result<MultipartUploadResponse, BmcError> {
    let status = response.status();
    let url = response.url().clone();
    let headers = response.headers().clone();

    if !status.is_success() {
        #[allow(clippy::unnecessary_result_map_or_else)]
        let text = response
            .text()
            .await
            .map_or_else(|_| UNREADABLE_ERROR_BODY.into(), |text| text);

        return Err(BmcError::InvalidResponse { url, status, text });
    }

    let bytes = response.bytes().await.map_err(BmcError::ReqwestError)?;
    let body = if bytes.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&bytes).map_err(BmcError::DecodeError)?)
    };

    let odata_id = body.as_ref().and_then(odata_id_from_body);

    Ok(MultipartUploadResponse {
        status: status.as_u16(),
        location: location_from_headers(&headers),
        odata_id,
        retry_after_secs: retry_after_from_headers(&headers),
        body,
    })
}

async fn multipart_upload_file_from_path(
    path: &Path,
) -> Result<MultipartUploadFile<File>, io::Error> {
    let (file_name, file, length) = upload_file_parts_from_path(path).await?;

    Ok(MultipartUploadFile::new(file_name, file).with_content_length(length))
}

async fn upload_file_from_path(path: &Path) -> Result<UploadFile<File>, io::Error> {
    let (_, file, length) = upload_file_parts_from_path(path).await?;

    Ok(UploadFile::new(file).with_content_length(length))
}

async fn upload_file_parts_from_path(path: &Path) -> Result<(String, File, u64), io::Error> {
    let file_name = file_name_from_path(path);
    let file = File::open(path).await?;

    let length = file.metadata().await?.len();

    Ok((file_name, file, length))
}

fn odata_id_from_body(body: &serde_json::Value) -> Option<ODataId> {
    body.get("@odata.id")
        .and_then(serde_json::Value::as_str)
        .map(|id| id.to_string().into())
}

fn modification_entity_from_odata_body<T>(
    bytes: &[u8],
    etag: Option<&ODataETag>,
) -> Result<Option<ModificationResponse<T>>, BmcError>
where
    T: DeserializeOwned + Send + Sync,
{
    if bytes.is_empty() {
        return Ok(None);
    }

    let mut value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(BmcError::DecodeError)?;

    if value.get("@odata.id").is_none() {
        return Ok(None);
    }

    if let Some(etag) = etag {
        inject_etag(etag, &mut value);
    }

    serde_path_to_error::deserialize(value)
        .map(ModificationResponse::Entity)
        .map(Some)
        .map_err(BmcError::JsonError)
}

fn json_value_from_bytes(bytes: &[u8]) -> Result<serde_json::Value, BmcError> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }

    serde_json::from_slice(bytes).map_err(BmcError::DecodeError)
}

fn file_name_from_path(path: &Path) -> String {
    path.file_name()
        .and_then(|file_name| file_name.to_str())
        .map_or_else(|| "update.bin".to_string(), ToString::to_string)
}

fn part_with_mime(part: Part, mime: &'static str) -> Result<Part, BmcError> {
    part.mime_str(mime).map_err(BmcError::ReqwestError)
}

fn location_from_headers(headers: &HeaderMap) -> Option<ODataId> {
    headers
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .map(|raw| {
            Url::parse(raw).map_or_else(
                |_| raw.to_string().into(),
                |url| {
                    let mut path = url.path().to_string();
                    if let Some(query) = url.query() {
                        path.push('?');
                        path.push_str(query);
                    }
                    path.into()
                },
            )
        })
}

fn auth_token_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-auth-token")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn etag_from_headers(headers: &HeaderMap) -> Option<ODataETag> {
    headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(|v| v.to_string().into())
}

fn retry_after_from_headers(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

fn inject_etag(etag: &ODataETag, body: &mut serde_json::Value) {
    if let Some(obj) = body.as_object_mut() {
        let etag_value = serde_json::Value::String(etag.to_string());

        // Handles both absent and null values
        obj.entry("@odata.etag")
            .and_modify(|v| *v = etag_value.clone())
            .or_insert(etag_value);
    }
}

fn auth_headers(
    request: reqwest::RequestBuilder,
    credentials: &BmcCredentials,
) -> reqwest::RequestBuilder {
    match credentials {
        BmcCredentials::UsernamePassword { username, .. } if username.is_empty() => request,
        BmcCredentials::UsernamePassword { username, password } => {
            request.basic_auth(username, password.as_ref())
        }
        BmcCredentials::Token { token } => request.header("X-Auth-Token", token),
    }
}

impl RawJsonHttpClient for Client {
    async fn get_json(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<serde_json::Value, Self::Error> {
        let response = auth_headers(self.client.get(url), credentials)
            .headers(custom_headers.clone())
            .send()
            .await?;

        self.handle_json_response(response).await
    }

    async fn post_json<B>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<serde_json::Value, Self::Error>
    where
        B: Serialize + Send + Sync,
    {
        let response = auth_headers(self.client.post(url), credentials)
            .headers(custom_headers.clone())
            .json(body)
            .send()
            .await?;

        self.handle_json_response(response).await
    }

    async fn patch_json<B>(
        &self,
        url: Url,
        etag: Option<&ODataETag>,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<serde_json::Value, Self::Error>
    where
        B: Serialize + Send + Sync,
    {
        let mut request =
            auth_headers(self.client.patch(url), credentials).headers(custom_headers.clone());

        if let Some(etag) = etag {
            request = request.header(header::IF_MATCH, etag.to_string());
        }

        let response = request.json(body).send().await?;

        self.handle_json_response(response).await
    }
}

impl HttpClient for Client {
    type Error = BmcError;

    async fn get<T>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        etag: Option<ODataETag>,
        custom_headers: &HeaderMap,
    ) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
    {
        let mut request =
            auth_headers(self.client.get(url), credentials).headers(custom_headers.clone());

        if let Some(etag) = etag {
            request = request.header(header::IF_NONE_MATCH, etag.to_string());
        }

        let response = request.send().await?;

        self.handle_response(response).await
    }

    async fn post<B, T>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<ModificationResponse<T>, Self::Error>
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync,
    {
        let response = auth_headers(self.client.post(url), credentials)
            .headers(custom_headers.clone())
            .json(body)
            .send()
            .await?;

        self.handle_modification_response(response).await
    }

    async fn post_session<B, T>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<SessionCreateResponse<T>, Self::Error>
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync,
    {
        let response = auth_headers(self.client.post(url), credentials)
            .headers(custom_headers.clone())
            .json(body)
            .send()
            .await?;

        self.handle_session_response(response).await
    }

    async fn patch<B, T>(
        &self,
        url: Url,
        etag: ODataETag,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<ModificationResponse<T>, Self::Error>
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync,
    {
        let response = auth_headers(self.client.patch(url), credentials)
            .headers(custom_headers.clone())
            .header(header::IF_MATCH, etag.to_string())
            .json(body)
            .send()
            .await?;

        self.handle_modification_response(response).await
    }

    async fn delete<T>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<ModificationResponse<T>, Self::Error>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let response = auth_headers(self.client.delete(url), credentials)
            .headers(custom_headers.clone())
            .send()
            .await?;

        self.handle_modification_response(response).await
    }

    async fn sse<T: Send + Sized + for<'de> serde::Deserialize<'de>>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> Result<BoxTryStream<T, Self::Error>, Self::Error> {
        let response = auth_headers(self.client.get(url), credentials)
            .headers(custom_headers.clone())
            .header(header::ACCEPT, "text/event-stream")
            .timeout(Duration::MAX)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(BmcError::InvalidResponse {
                url: response.url().clone(),
                status: response.status(),
                text: response
                    .text()
                    .await
                    .unwrap_or_else(|_| UNREADABLE_ERROR_BODY.into()),
            });
        }

        let stream = sse_stream::SseStream::from_byte_stream(response.bytes_stream()).filter_map(
            |event| async move {
                match event {
                    Err(err) => Some(Err(BmcError::SseStreamError(err))),
                    Ok(sse) => sse.data.map(|data| {
                        serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_str(
                            &data,
                        ))
                        .map_err(BmcError::JsonError)
                    }),
                }
            },
        );

        Ok(Box::pin(stream))
    }
}

impl MultipartHttpClient for Client {
    async fn post_multipart_update<R, V>(
        &self,
        url: Url,
        update_parameters: &V,
        update_file: MultipartUploadFile<R>,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
        upload_timeout: Duration,
    ) -> Result<MultipartUploadResponse, Self::Error>
    where
        R: MultipartUploadReader,
        V: Serialize + Send + Sync,
    {
        let form = update_multipart_form(update_parameters, update_file)?;

        let response = auth_headers(self.client.post(url), credentials)
            .headers(custom_headers.clone())
            .multipart(form)
            .timeout(upload_timeout)
            .send()
            .await?;

        handle_upload_response(response).await
    }
}

impl RawFileUploadHttpClient for Client {
    async fn put_raw_update<R>(
        &self,
        url: Url,
        update_file: UploadFile<R>,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
        upload_timeout: Duration,
    ) -> Result<RawUploadResponse, Self::Error>
    where
        R: UploadReader,
    {
        let (body, content_length) = raw_upload_body(update_file);

        let mut request = auth_headers(self.client.put(url), credentials)
            .headers(custom_headers.clone())
            .header(header::CONTENT_TYPE, OCTET_STREAM_MIME)
            .body(body)
            .timeout(upload_timeout);

        if let Some(content_length) = content_length {
            request = request.header(header::CONTENT_LENGTH, content_length);
        }

        let response = request.send().await?;

        handle_upload_response(response).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cacheable_error_trait() -> Result<(), Box<dyn StdError>> {
        let mock_response =
            reqwest::Response::from(http::Response::builder().status(304).body("")?);
        let error = BmcError::InvalidResponse {
            url: "http://example.com/redfish/v1".parse()?,
            status: mock_response.status(),
            text: String::new(),
        };

        assert!(error.is_cached());

        let cache_miss = BmcError::CacheMiss;
        assert!(!cache_miss.is_cached());

        let created_miss = BmcError::cache_miss();
        assert!(matches!(created_miss, BmcError::CacheMiss));

        Ok(())
    }
}
