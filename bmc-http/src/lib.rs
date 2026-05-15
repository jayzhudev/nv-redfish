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

#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::suspicious,
    clippy::complexity,
    clippy::perf
)]
#![deny(
    clippy::absolute_paths,
    clippy::todo,
    clippy::unimplemented,
    clippy::tests_outside_test_module,
    clippy::panic,
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::unused_trait_names,
    clippy::print_stdout,
    clippy::print_stderr
)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::duration_suboptimal_units)]
#![deny(missing_docs)]

//! HTTP implementation of [`nv_redfish_core::Bmc`] trait.

pub mod cache;
pub mod credentials;

#[cfg(feature = "reqwest")]
pub mod reqwest;

use crate::cache::TypeErasedCarCache;
use http::HeaderMap;
use nv_redfish_core::query::ExpandQuery;
use nv_redfish_core::Action;
use nv_redfish_core::Bmc;
use nv_redfish_core::BoxTryStream;
use nv_redfish_core::EntityTypeRef;
use nv_redfish_core::Expandable;
use nv_redfish_core::FilterQuery;
use nv_redfish_core::ModificationResponse;
use nv_redfish_core::ODataETag;
use nv_redfish_core::ODataId;
use nv_redfish_core::SessionCreateResponse;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
#[cfg(feature = "reqwest")]
use std::io;
use std::sync::Arc;
use std::sync::RwLock;
#[cfg(feature = "reqwest")]
use std::time::Duration;
#[cfg(feature = "reqwest")]
use tokio::io::AsyncRead;
use url::Url;

#[doc(inline)]
pub use credentials::BmcCredentials;

/// Error returned when a caller supplied URI is not safe for Redfish use.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedfishUriError {
    /// Absolute URL does not match the configured BMC endpoint origin.
    OriginMismatch {
        /// Requested absolute URI.
        uri: String,
    },
    /// URI path is outside the Redfish service root.
    NonRedfishPath {
        /// Requested URI path.
        path: String,
    },
    /// URI path contains a `.` or `..` segment.
    DotSegment {
        /// Requested URI path.
        path: String,
    },
}

impl fmt::Display for RedfishUriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OriginMismatch { uri } => {
                write!(
                    f,
                    "absolute redfish URI origin does not match BMC endpoint: {uri}"
                )
            }
            Self::NonRedfishPath { path } => {
                write!(
                    f,
                    "redfish URI path must be /redfish or start with /redfish/: {path}"
                )
            }
            Self::DotSegment { path } => {
                write!(f, "redfish URI path must not contain dot segments: {path}")
            }
        }
    }
}

impl StdError for RedfishUriError {}

/// Error returned by path-based upload helpers.
#[cfg(feature = "reqwest")]
#[derive(Debug)]
pub enum UploadError<E> {
    /// The upload file could not be opened or inspected.
    File(io::Error),
    /// The HTTP upload request failed.
    Request(E),
}

#[cfg(feature = "reqwest")]
impl<E> UploadError<E> {
    /// Map the request error while preserving file errors.
    #[must_use]
    pub fn map_request<T>(self, f: impl FnOnce(E) -> T) -> UploadError<T> {
        match self {
            Self::File(err) => UploadError::File(err),
            Self::Request(err) => UploadError::Request(f(err)),
        }
    }
}

#[cfg(feature = "reqwest")]
impl<E> fmt::Display for UploadError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(err) => write!(f, "upload file IO error: {err}"),
            Self::Request(err) => write!(f, "upload request error: {err}"),
        }
    }
}

#[cfg(feature = "reqwest")]
impl<E> StdError for UploadError<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::File(err) => Some(err),
            Self::Request(err) => Some(err),
        }
    }
}

/// Async reader type accepted by upload methods.
#[cfg(feature = "reqwest")]
pub trait UploadReader: AsyncRead + Send + 'static {}

#[cfg(feature = "reqwest")]
impl<T> UploadReader for T where T: AsyncRead + Send + 'static {}

/// File stream and metadata for a Redfish upload.
#[cfg(feature = "reqwest")]
pub struct UploadFile<R: UploadReader> {
    reader: R,
    content_length: Option<u64>,
}

#[cfg(feature = "reqwest")]
impl<R: UploadReader> UploadFile<R> {
    /// Create an upload file from an async reader.
    #[must_use]
    pub const fn new(reader: R) -> Self {
        Self {
            reader,
            content_length: None,
        }
    }

    /// Attach a known content length for transports that can use it.
    #[must_use]
    pub const fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);

        self
    }

    /// Split the upload file into transport-owned parts.
    #[must_use]
    pub fn into_parts(self) -> (R, Option<u64>) {
        (self.reader, self.content_length)
    }
}

/// Async reader type accepted by multipart upload methods.
#[cfg(feature = "reqwest")]
pub trait MultipartUploadReader: UploadReader {}

#[cfg(feature = "reqwest")]
impl<T> MultipartUploadReader for T where T: UploadReader {}

/// `UpdateFile` stream and metadata for a Redfish multipart upload.
#[cfg(feature = "reqwest")]
pub struct MultipartUploadFile<R: MultipartUploadReader> {
    file_name: String,
    reader: R,
    content_length: Option<u64>,
}

#[cfg(feature = "reqwest")]
impl<R: MultipartUploadReader> MultipartUploadFile<R> {
    /// Create a multipart upload file from a file name and async reader.
    #[must_use]
    pub const fn new(file_name: String, reader: R) -> Self {
        Self {
            file_name,
            reader,
            content_length: None,
        }
    }

    /// Attach a known content length for transports that can use it.
    #[must_use]
    pub const fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);

        self
    }

    /// Split the upload file into transport-owned parts.
    #[must_use]
    pub fn into_parts(self) -> (String, R, Option<u64>) {
        (self.file_name, self.reader, self.content_length)
    }
}

/// Structured response from a Redfish multipart upload request.
#[cfg(feature = "reqwest")]
#[derive(Debug, Clone)]
pub struct MultipartUploadResponse {
    /// HTTP status code returned by the BMC.
    pub status: u16,
    /// `Location` header, usually a Redfish task monitor URI for 202 responses.
    pub location: Option<ODataId>,
    /// Response body `@odata.id`, when the BMC returns a Redfish resource body.
    pub odata_id: Option<ODataId>,
    /// Recommended task polling delay from the `Retry-After` header.
    pub retry_after_secs: Option<u64>,
    /// Parsed JSON response body for caller-specific response details.
    pub body: Option<serde_json::Value>,
}

#[cfg(feature = "reqwest")]
impl MultipartUploadResponse {
    /// Best available task URI.
    ///
    /// Body `@odata.id` is preferred because a 202 `Location` can point to a
    /// task monitor rather than the task resource itself.
    #[must_use]
    pub fn task_uri(&self) -> Option<&ODataId> {
        self.odata_id.as_ref().or(self.location.as_ref())
    }

    /// Best available task id, derived from [`Self::task_uri`].
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.task_uri().and_then(ODataId::last_segment)
    }
}

/// Alias for a Redfish raw file upload response.
#[cfg(feature = "reqwest")]
pub type RawUploadResponse = MultipartUploadResponse;

/// HTTP client extension for Redfish multipart uploads.
#[cfg(feature = "reqwest")]
pub trait MultipartHttpClient: HttpClient {
    /// Perform a Redfish UpdateService multipart upload POST request.
    fn post_multipart_update<R, V>(
        &self,
        url: Url,
        update_parameters: &V,
        update_file: MultipartUploadFile<R>,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
        upload_timeout: Duration,
    ) -> impl Future<Output = Result<MultipartUploadResponse, Self::Error>> + Send
    where
        R: MultipartUploadReader,
        V: Serialize + Send + Sync;
}

/// HTTP client extension for Redfish raw file uploads.
#[cfg(feature = "reqwest")]
pub trait RawFileUploadHttpClient: HttpClient {
    /// Perform a Redfish raw file upload PUT request.
    fn put_raw_update<R>(
        &self,
        url: Url,
        update_file: UploadFile<R>,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
        upload_timeout: Duration,
    ) -> impl Future<Output = Result<RawUploadResponse, Self::Error>> + Send
    where
        R: UploadReader;
}

/// HTTP client extension for raw JSON Redfish passthrough requests.
///
/// These methods are intentionally schema-free for callers that proxy Redfish
/// resources not modeled by nv-redfish, such as RMS gRPC passthrough, while
/// still reusing [`HttpBmc`] URL resolution, credentials, and custom headers.
/// They return `serde_json::Value` so successful bodies without `@odata.id`
/// are preserved.
pub trait RawJsonHttpClient: HttpClient {
    /// Perform an HTTP GET request and return the successful JSON response body.
    fn get_json(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<serde_json::Value, Self::Error>> + Send;

    /// Perform an HTTP POST request and return the successful JSON response body.
    fn post_json<B>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<serde_json::Value, Self::Error>> + Send
    where
        B: Serialize + Send + Sync;

    /// Perform an HTTP PATCH request and return the successful JSON response body.
    fn patch_json<B>(
        &self,
        url: Url,
        etag: Option<&ODataETag>,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<serde_json::Value, Self::Error>> + Send
    where
        B: Serialize + Send + Sync;
}

/// HTTP Client trait.
///
/// nv-redfish-bmc-http supports any HTTP implementation that
/// implements this [`HttpClient`] trait.
pub trait HttpClient: Send + Sync {
    /// HTTP client error.
    type Error: Send + StdError;

    /// Perform an HTTP GET request with optional conditional headers.
    fn get<T>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        etag: Option<ODataETag>,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<T, Self::Error>> + Send
    where
        T: DeserializeOwned + Send + Sync;

    /// Perform an HTTP POST request.
    fn post<B, T>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<ModificationResponse<T>, Self::Error>> + Send
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync;

    /// Perform a Redfish session creation POST request.
    fn post_session<B, T>(
        &self,
        url: Url,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<SessionCreateResponse<T>, Self::Error>> + Send
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync;

    /// Perform an HTTP PATCH request.
    fn patch<B, T>(
        &self,
        url: Url,
        etag: ODataETag,
        body: &B,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<ModificationResponse<T>, Self::Error>> + Send
    where
        B: Serialize + Send + Sync,
        T: DeserializeOwned + Send + Sync;

    /// Perform an HTTP DELETE request.
    fn delete<T>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<ModificationResponse<T>, Self::Error>> + Send
    where
        T: DeserializeOwned + Send + Sync;

    /// Open an SSE stream
    fn sse<T: Sized + for<'de> Deserialize<'de> + Send>(
        &self,
        url: Url,
        credentials: &BmcCredentials,
        custom_headers: &HeaderMap,
    ) -> impl Future<Output = Result<BoxTryStream<T, Self::Error>, Self::Error>> + Send;
}

/// HTTP-based BMC implementation that wraps an [`HttpClient`].
///
/// This struct combines an HTTP client with BMC endpoint information and credentials
/// to provide a complete Redfish client implementation. It implements the [`Bmc`] trait
/// to provide standardized access to Redfish services.
///
/// # Type Parameters
///
/// * `C` - The HTTP client implementation to use
///
pub struct HttpBmc<C: HttpClient> {
    client: C,
    redfish_endpoint: RedfishEndpoint,
    credentials: RwLock<Arc<BmcCredentials>>,
    cache: RwLock<TypeErasedCarCache<ODataId>>,
    etags: RwLock<HashMap<ODataId, ODataETag>>,
    custom_headers: HeaderMap,
}

impl<C: HttpClient> HttpBmc<C>
where
    C::Error: CacheableError,
{
    /// Create a new HTTP-based BMC client with ETag-based caching.
    ///
    /// # Arguments
    ///
    /// * `client` - The HTTP client implementation to use for requests
    /// * `redfish_endpoint` - The base URL of the Redfish service (e.g., `https://192.168.1.100`)
    /// * `credentials` - Authentication credentials for the BMC
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "reqwest")]
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use nv_redfish_bmc_http::BmcCredentials;
    /// use nv_redfish_bmc_http::CacheSettings;
    /// use nv_redfish_bmc_http::HttpBmc;
    /// use nv_redfish_bmc_http::reqwest::Client;
    /// use url::Url;
    ///
    /// let credentials = BmcCredentials::username_password("admin".to_string(), Some("password".to_string()));
    /// let http_client = Client::new()?;
    /// let endpoint = Url::parse("https://192.168.1.100")?;
    ///
    /// let bmc = HttpBmc::new(http_client, endpoint, credentials, CacheSettings::default());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        client: C,
        redfish_endpoint: Url,
        credentials: BmcCredentials,
        cache_settings: CacheSettings,
    ) -> Self {
        Self::with_custom_headers(
            client,
            redfish_endpoint,
            credentials,
            cache_settings,
            HeaderMap::new(),
        )
    }

    /// Create a new HTTP-based BMC client with custom headers and ETag-based caching.
    ///
    /// This is an alternative constructor that allows specifying custom HTTP headers
    /// that will be included in all requests. Use this when you need vendor-specific
    /// headers, custom authentication tokens, or other HTTP headers required by the
    /// Redfish service at construction time.
    ///
    /// For most use cases, prefer [`HttpBmc::new`] which creates a client without
    /// custom headers.
    ///
    /// # Arguments
    ///
    /// * `client` - The HTTP client implementation to use for requests
    /// * `redfish_endpoint` - The base URL of the Redfish service (e.g., `https://192.168.1.100`)
    /// * `credentials` - Authentication credentials for the BMC
    /// * `cache_settings` - Cache configuration for response caching
    /// * `custom_headers` - Custom HTTP headers to include in all requests
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "reqwest")]
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use http::HeaderMap;
    /// use nv_redfish_bmc_http::BmcCredentials;
    /// use nv_redfish_bmc_http::CacheSettings;
    /// use nv_redfish_bmc_http::HttpBmc;
    /// use nv_redfish_bmc_http::reqwest::Client;
    /// use url::Url;
    ///
    /// let credentials = BmcCredentials::username_password("admin".to_string(), Some("password".to_string()));
    /// let http_client = Client::new()?;
    /// let endpoint = Url::parse("https://192.168.1.100")?;
    ///
    /// // Create custom headers
    /// let mut headers = HeaderMap::new();
    /// headers.insert("X-Auth-Token", "custom-token-value".parse()?);
    /// headers.insert("X-Vendor-Header", "vendor-specific-value".parse()?);
    ///
    /// // Create BMC client with custom headers
    /// let bmc = HttpBmc::with_custom_headers(
    ///     http_client,
    ///     endpoint,
    ///     credentials,
    ///     CacheSettings::default(),
    ///     headers,
    /// );
    ///
    /// // All requests will include the custom headers
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_custom_headers(
        client: C,
        redfish_endpoint: Url,
        credentials: BmcCredentials,
        cache_settings: CacheSettings,
        custom_headers: HeaderMap,
    ) -> Self {
        Self {
            client,
            redfish_endpoint: RedfishEndpoint::from(redfish_endpoint),
            credentials: RwLock::new(Arc::new(credentials)),
            cache: RwLock::new(TypeErasedCarCache::new(cache_settings.capacity)),
            etags: RwLock::new(HashMap::new()),
            custom_headers,
        }
    }

    /// Replace the credentials used for subsequent requests.
    ///
    /// Existing cache and ETag state is preserved.
    ///
    /// # Panics
    ///
    /// Panics if the internal credentials lock is poisoned. This should not
    /// occur in normal operation.
    #[allow(clippy::panic)] // See panics section.
    pub fn set_credentials(&self, credentials: BmcCredentials) {
        *self.credentials.write().expect("poisoned") = Arc::new(credentials);
    }
}

/// A tagged type representing a Redfish endpoint URL.
///
/// Provides convenient conversion methods to build endpoint URLs from `ODataId` paths.
#[derive(Debug, Clone)]
pub struct RedfishEndpoint {
    base_url: Url,
}

impl RedfishEndpoint {
    /// Create a new `RedfishEndpoint` from a base URL
    #[must_use]
    pub const fn new(base_url: Url) -> Self {
        Self { base_url }
    }

    /// Convert a path to a full Redfish endpoint URL
    #[must_use]
    pub fn with_path(&self, path: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(path);
        url
    }

    /// Convert a path to a full Redfish endpoint URL with query parameters
    #[must_use]
    pub fn with_path_and_query(&self, path: &str, query: &str) -> Url {
        let mut url = self.with_path(path);
        url.set_query(Some(query));
        url
    }
}

/// `CacheSettings` for internal BMC cache with etags
#[derive(Clone, Copy)]
pub struct CacheSettings {
    capacity: usize,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self { capacity: 100 }
    }
}

impl CacheSettings {
    /// Define capacity of the cache measured in number of items.
    #[must_use]
    pub const fn with_capacity(capacity: usize) -> Self {
        Self { capacity }
    }
}

impl From<Url> for RedfishEndpoint {
    fn from(url: Url) -> Self {
        Self::new(url)
    }
}

impl From<&RedfishEndpoint> for Url {
    fn from(endpoint: &RedfishEndpoint) -> Self {
        endpoint.base_url.clone()
    }
}

/// Trait for errors that can indicate whether they represent a cached response
/// and provide a way to create cache-related errors.
pub trait CacheableError {
    /// Returns true if this error indicates the resource should be served from cache.
    /// Typically true for HTTP 304 Not Modified responses.
    fn is_cached(&self) -> bool;

    /// Create an error for when cached data is requested but not available.
    fn cache_miss() -> Self;

    /// Cache error
    fn cache_error(reason: String) -> Self;
}

impl<C: HttpClient> HttpBmc<C>
where
    C::Error: CacheableError + StdError + Send + Sync,
{
    #[allow(clippy::panic)] // See set_credentials Panic doc.
    fn read_credentials(&self) -> Arc<BmcCredentials> {
        self.credentials
            .read()
            .map(|credentials| Arc::clone(&credentials))
            .expect("lock poisoned")
    }

    /// Perform a GET request with `ETag` caching support
    ///
    /// This handles:
    /// - Retrieving cached `ETag` before request
    /// - Sending conditional GET with If-None-Match
    /// - Handling 304 Not Modified responses from cache
    /// - Updating cache and `ETag` storage on success
    #[allow(clippy::significant_drop_tightening)]
    async fn get_with_cache<T: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        endpoint_url: Url,
        id: &ODataId,
    ) -> Result<Arc<T>, C::Error> {
        // Retrieve cached etag
        let etag: Option<ODataETag> = {
            let etags = self
                .etags
                .read()
                .map_err(|e| C::Error::cache_error(e.to_string()))?;
            etags.get(id).cloned()
        };
        let credentials = self.read_credentials();

        // Perform GET request
        match self
            .client
            .get::<T>(
                endpoint_url,
                credentials.as_ref(),
                etag,
                &self.custom_headers,
            )
            .await
        {
            Ok(response) => {
                let entity = Arc::new(response);

                // Update cache if entity has etag
                if let Some(etag) = entity.etag() {
                    let mut cache = self
                        .cache
                        .write()
                        .map_err(|e| C::Error::cache_error(e.to_string()))?;

                    let mut etags = self
                        .etags
                        .write()
                        .map_err(|e| C::Error::cache_error(e.to_string()))?;

                    if let Some(evicted_id) = cache.put_typed(id.clone(), Arc::clone(&entity)) {
                        etags.remove(&evicted_id);
                    }
                    etags.insert(id.clone(), etag.clone());
                }
                Ok(entity)
            }
            Err(e) => {
                // Handle 304 Not Modified - return from cache
                if e.is_cached() {
                    let mut cache = self
                        .cache
                        .write()
                        .map_err(|e| C::Error::cache_error(e.to_string()))?;
                    cache
                        .get_typed::<Arc<T>>(id)
                        .cloned()
                        .ok_or_else(C::Error::cache_miss)
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl<C: RawJsonHttpClient> HttpBmc<C>
where
    C::Error: CacheableError + From<RedfishUriError> + StdError + Send + Sync,
{
    /// GET a raw JSON Redfish passthrough request.
    ///
    /// The URI must be a relative Redfish path or an absolute URL that matches
    /// this BMC endpoint origin.
    ///
    /// Empty successful response bodies are returned as `{}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the BMC returns an unsuccessful
    /// HTTP status, or the successful response body is not valid JSON.
    pub async fn get_json(&self, uri: impl AsRef<str>) -> Result<serde_json::Value, C::Error> {
        let endpoint_url =
            endpoint_url_from_uri(&self.redfish_endpoint, uri.as_ref()).map_err(C::Error::from)?;

        let credentials = self.read_credentials();

        self.client
            .get_json(endpoint_url, credentials.as_ref(), &self.custom_headers)
            .await
    }

    /// POST a raw JSON Redfish passthrough request.
    ///
    /// The URI must be a relative Redfish path or an absolute URL that matches
    /// this BMC endpoint origin.
    ///
    /// Empty successful response bodies are returned as `{}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the BMC returns an unsuccessful
    /// HTTP status, or the successful response body is not valid JSON.
    pub async fn post_json<B>(
        &self,
        uri: impl AsRef<str>,
        body: &B,
    ) -> Result<serde_json::Value, C::Error>
    where
        B: Serialize + Send + Sync,
    {
        let endpoint_url =
            endpoint_url_from_uri(&self.redfish_endpoint, uri.as_ref()).map_err(C::Error::from)?;

        let credentials = self.read_credentials();

        self.client
            .post_json(
                endpoint_url,
                body,
                credentials.as_ref(),
                &self.custom_headers,
            )
            .await
    }

    /// PATCH a raw JSON Redfish passthrough request.
    ///
    /// The URI must be a relative Redfish path or an absolute URL that matches
    /// this BMC endpoint origin.
    ///
    /// `If-Match` is sent only when `etag` is `Some`. Empty successful
    /// response bodies are returned as `{}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the BMC returns an unsuccessful
    /// HTTP status, or the successful response body is not valid JSON.
    pub async fn patch_json<B>(
        &self,
        uri: impl AsRef<str>,
        etag: Option<&ODataETag>,
        body: &B,
    ) -> Result<serde_json::Value, C::Error>
    where
        B: Serialize + Send + Sync,
    {
        let endpoint_url =
            endpoint_url_from_uri(&self.redfish_endpoint, uri.as_ref()).map_err(C::Error::from)?;

        let credentials = self.read_credentials();

        self.client
            .patch_json(
                endpoint_url,
                etag,
                body,
                credentials.as_ref(),
                &self.custom_headers,
            )
            .await
    }
}

#[cfg(feature = "reqwest")]
impl<C: MultipartHttpClient> HttpBmc<C>
where
    C::Error: CacheableError + From<RedfishUriError> + StdError + Send + Sync,
{
    /// POST a Redfish UpdateService multipart upload with `UpdateFile` read from an async reader.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the multipart request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn post_update_multipart_from_reader<R, V>(
        &self,
        multipart_uri: impl AsRef<str>,
        update_parameters: &V,
        file_name: impl Into<String>,
        update_file: R,
        upload_timeout: Duration,
    ) -> Result<MultipartUploadResponse, C::Error>
    where
        R: MultipartUploadReader,
        V: Serialize + Send + Sync,
    {
        let update_file = MultipartUploadFile::new(file_name.into(), update_file);

        self.post_update_multipart_file(
            multipart_uri,
            update_parameters,
            update_file,
            upload_timeout,
        )
        .await
    }

    /// POST a Redfish UpdateService multipart upload with a pre-built `UpdateFile`.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    /// The URI must be a relative Redfish path or an absolute URL that matches
    /// this BMC endpoint origin.
    ///
    /// # Errors
    ///
    /// Returns an error if the multipart request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn post_update_multipart_file<R, V>(
        &self,
        multipart_uri: impl AsRef<str>,
        update_parameters: &V,
        update_file: MultipartUploadFile<R>,
        upload_timeout: Duration,
    ) -> Result<MultipartUploadResponse, C::Error>
    where
        R: MultipartUploadReader,
        V: Serialize + Send + Sync,
    {
        let endpoint_url = endpoint_url_from_uri(&self.redfish_endpoint, multipart_uri.as_ref())
            .map_err(C::Error::from)?;

        let credentials = self.read_credentials();

        self.client
            .post_multipart_update(
                endpoint_url,
                update_parameters,
                update_file,
                credentials.as_ref(),
                &self.custom_headers,
                upload_timeout,
            )
            .await
    }
}

#[cfg(feature = "reqwest")]
impl<C: RawFileUploadHttpClient> HttpBmc<C>
where
    C::Error: CacheableError + From<RedfishUriError> + StdError + Send + Sync,
{
    /// PUT a raw Redfish update file read from an async reader.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn put_update_file_from_reader<R>(
        &self,
        update_uri: impl AsRef<str>,
        update_file: R,
        upload_timeout: Duration,
    ) -> Result<RawUploadResponse, C::Error>
    where
        R: UploadReader,
    {
        self.put_update_file(update_uri, UploadFile::new(update_file), upload_timeout)
            .await
    }

    /// PUT a raw Redfish update file.
    ///
    /// The request reuses this BMC's HTTP client, credentials, and custom headers.
    /// The URI must be a relative Redfish path or an absolute URL that matches
    /// this BMC endpoint origin.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn put_update_file<R>(
        &self,
        update_uri: impl AsRef<str>,
        update_file: UploadFile<R>,
        upload_timeout: Duration,
    ) -> Result<RawUploadResponse, C::Error>
    where
        R: UploadReader,
    {
        let endpoint_url = endpoint_url_from_uri(&self.redfish_endpoint, update_uri.as_ref())
            .map_err(C::Error::from)?;

        let credentials = self.read_credentials();

        self.client
            .put_raw_update(
                endpoint_url,
                update_file,
                credentials.as_ref(),
                &self.custom_headers,
                upload_timeout,
            )
            .await
    }
}

fn endpoint_url_from_uri(endpoint: &RedfishEndpoint, uri: &str) -> Result<Url, RedfishUriError> {
    if let Ok(url) = Url::parse(uri) {
        validate_same_endpoint_origin(endpoint, &url)?;
        validate_redfish_path(url.path())?;

        return Ok(url);
    }

    let mut parts = uri.splitn(2, '?');
    let path = parts.next().map_or(uri, |path| path);
    let query = parts.next();

    validate_redfish_path(path)?;

    let mut url = endpoint.with_path(path);

    if let Some(query) = query {
        url.set_query(Some(query));
    }

    Ok(url)
}

fn validate_same_endpoint_origin(
    endpoint: &RedfishEndpoint,
    url: &Url,
) -> Result<(), RedfishUriError> {
    let base_url = Url::from(endpoint);
    let matches_origin = url.scheme() == base_url.scheme()
        && url.host_str() == base_url.host_str()
        && url.port_or_known_default() == base_url.port_or_known_default();

    if matches_origin {
        Ok(())
    } else {
        Err(RedfishUriError::OriginMismatch {
            uri: url.to_string(),
        })
    }
}

fn validate_redfish_path(path: &str) -> Result<(), RedfishUriError> {
    if path != "/redfish" && !path.starts_with("/redfish/") {
        return Err(RedfishUriError::NonRedfishPath {
            path: path.to_string(),
        });
    }

    if path.split('/').any(is_dot_segment) {
        return Err(RedfishUriError::DotSegment {
            path: path.to_string(),
        });
    }

    Ok(())
}

fn is_dot_segment(segment: &str) -> bool {
    matches!(segment, "." | "..")
        || segment.eq_ignore_ascii_case("%2e")
        || segment.eq_ignore_ascii_case("%2e%2e")
        || segment.eq_ignore_ascii_case(".%2e")
        || segment.eq_ignore_ascii_case("%2e.")
}

impl<C: HttpClient> Bmc for HttpBmc<C>
where
    C::Error: CacheableError + StdError + Send + Sync,
{
    type Error = C::Error;

    async fn get<T: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
    ) -> Result<Arc<T>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&id.to_string());
        self.get_with_cache(endpoint_url, id).await
    }

    async fn expand<T: Expandable + 'static>(
        &self,
        id: &ODataId,
        query: ExpandQuery,
    ) -> Result<Arc<T>, Self::Error> {
        let endpoint_url = self
            .redfish_endpoint
            .with_path_and_query(&id.to_string(), &query.to_query_string());

        self.get_with_cache(endpoint_url, id).await
    }

    async fn create<V: Sync + Send + Serialize, R: Sync + Send + for<'de> Deserialize<'de>>(
        &self,
        id: &ODataId,
        v: &V,
    ) -> Result<ModificationResponse<R>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&id.to_string());
        let credentials = self.read_credentials();
        self.client
            .post(endpoint_url, v, credentials.as_ref(), &self.custom_headers)
            .await
    }

    async fn create_session<
        V: Sync + Send + Serialize,
        R: Sync + Send + for<'de> Deserialize<'de>,
    >(
        &self,
        id: &ODataId,
        v: &V,
    ) -> Result<SessionCreateResponse<R>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&id.to_string());
        let credentials = self.read_credentials();
        self.client
            .post_session(endpoint_url, v, credentials.as_ref(), &self.custom_headers)
            .await
    }

    async fn update<V: Sync + Send + Serialize, R: Sync + Send + for<'de> Deserialize<'de>>(
        &self,
        id: &ODataId,
        etag: Option<&ODataETag>,
        v: &V,
    ) -> Result<ModificationResponse<R>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&id.to_string());
        let credentials = self.read_credentials();
        let etag = etag
            .cloned()
            .unwrap_or_else(|| ODataETag::from("*".to_string()));

        self.client
            .patch(
                endpoint_url,
                etag,
                v,
                credentials.as_ref(),
                &self.custom_headers,
            )
            .await
    }

    async fn delete<T: Sync + Send + for<'de> Deserialize<'de>>(
        &self,
        id: &ODataId,
    ) -> Result<ModificationResponse<T>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&id.to_string());
        let credentials = self.read_credentials();
        self.client
            .delete(endpoint_url, credentials.as_ref(), &self.custom_headers)
            .await
    }

    async fn action<T: Send + Sync + Serialize, R: Send + Sync + for<'de> Deserialize<'de>>(
        &self,
        action: &Action<T, R>,
        params: &T,
    ) -> Result<ModificationResponse<R>, Self::Error> {
        let endpoint_url = self.redfish_endpoint.with_path(&action.target.to_string());
        let credentials = self.read_credentials();
        self.client
            .post(
                endpoint_url,
                params,
                credentials.as_ref(),
                &self.custom_headers,
            )
            .await
    }

    async fn filter<T: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
        query: FilterQuery,
    ) -> Result<Arc<T>, Self::Error> {
        let endpoint_url = self
            .redfish_endpoint
            .with_path_and_query(&id.to_string(), &query.to_query_string());

        self.get_with_cache(endpoint_url, id).await
    }

    async fn stream<T: Send + Sized + for<'de> Deserialize<'de>>(
        &self,
        uri: &str,
    ) -> Result<BoxTryStream<T, Self::Error>, Self::Error> {
        let endpoint_url = Url::parse(uri).unwrap_or_else(|_| self.redfish_endpoint.with_path(uri));
        let credentials = self.read_credentials();
        self.client
            .sse(endpoint_url, credentials.as_ref(), &self.custom_headers)
            .await
    }
}
