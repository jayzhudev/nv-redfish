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

//! Update Service entities and collections.
//!
//! This module provides types for working with Redfish UpdateService resources
//! and their sub-resources like firmware and software inventory.

mod software_inventory;

use crate::core::NavProperty;
use crate::patch_support::Payload;
use crate::patch_support::ReadPatchFn;
use crate::schema::update_service::UpdateService as UpdateServiceSchema;
use crate::schema::update_service::UpdateServiceSimpleUpdateAction;
use crate::Error;
use crate::NvBmc;
use crate::Resource;
use crate::ResourceSchema;
use crate::ServiceRoot;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::reqwest::Client as ReqwestClient;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::CacheableError;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::HttpBmc;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::MultipartHttpClient;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::MultipartUploadReader;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::RawFileUploadHttpClient;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::RedfishUriError;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::UploadError;
#[cfg(feature = "bmc-http")]
use nv_redfish_bmc_http::UploadReader;
use nv_redfish_core::Bmc;
use nv_redfish_core::ModificationResponse;
use serde_json::Value as JsonValue;
use software_inventory::SoftwareInventoryCollection;
#[cfg(feature = "bmc-http")]
use std::error::Error as StdError;
#[cfg(feature = "bmc-http")]
use std::fmt;
#[cfg(feature = "bmc-http")]
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "bmc-http")]
use std::time::Duration;

#[doc(inline)]
pub use crate::schema::update_service::TransferProtocolType;
#[cfg(feature = "bmc-http")]
#[doc(inline)]
pub use nv_redfish_bmc_http::MultipartUploadResponse as MultipartUpdateResponse;
#[cfg(feature = "bmc-http")]
#[doc(inline)]
pub use nv_redfish_bmc_http::RawUploadResponse as RawUpdateResponse;
#[doc(inline)]
pub use software_inventory::SoftwareInventory;
#[doc(inline)]
pub use software_inventory::Version;
#[doc(inline)]
pub use software_inventory::VersionRef;

/// Common Redfish UpdateService multipart upload URI.
#[cfg(feature = "bmc-http")]
pub const UPDATE_MULTIPART_URI: &str = "/redfish/v1/UpdateService/update-multipart";

/// Common Redfish UpdateService raw file upload URI.
#[cfg(feature = "bmc-http")]
pub const UPDATE_URI: &str = "/redfish/v1/UpdateService/update";

#[cfg(feature = "bmc-http")]
const FORCE_UPDATE_PARAMETER: &str = "ForceUpdate";
#[cfg(feature = "bmc-http")]
const TARGETS_PARAMETER: &str = "Targets";

/// Typed `UpdateParameters` part for Redfish UpdateService multipart uploads.
#[cfg(feature = "bmc-http")]
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct MultipartUpdateParameters {
    /// Force the update even when the service would otherwise reject it by policy.
    #[serde(rename = "ForceUpdate")]
    pub force_update: bool,
    /// Redfish target resource URIs to update.
    #[serde(rename = "Targets")]
    pub targets: Vec<String>,
    /// Additional Redfish or vendor-specific update parameters.
    ///
    /// Use [`Self::with_parameter`] to add entries without colliding with typed
    /// `ForceUpdate` or `Targets` fields.
    #[serde(flatten)]
    additional_parameters: serde_json::Map<String, JsonValue>,
}

#[cfg(feature = "bmc-http")]
impl MultipartUpdateParameters {
    /// Create update parameters with the required Redfish fields.
    #[must_use]
    pub fn new(force_update: bool, targets: Vec<String>) -> Self {
        Self {
            force_update,
            targets,
            additional_parameters: serde_json::Map::new(),
        }
    }

    /// Add an extra update parameter to the JSON body.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is a typed field already represented by this
    /// struct.
    pub fn with_parameter(
        mut self,
        name: impl Into<String>,
        value: JsonValue,
    ) -> Result<Self, MultipartUpdateParameterError> {
        let name = name.into();

        if is_reserved_update_parameter(&name) {
            return Err(MultipartUpdateParameterError { name });
        }

        self.additional_parameters.insert(name, value);

        Ok(self)
    }

    /// Additional Redfish or vendor-specific update parameters.
    #[must_use]
    pub const fn additional_parameters(&self) -> &serde_json::Map<String, JsonValue> {
        &self.additional_parameters
    }
}

/// Error returned when an additional update parameter duplicates a typed field.
#[cfg(feature = "bmc-http")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUpdateParameterError {
    name: String,
}

#[cfg(feature = "bmc-http")]
impl MultipartUpdateParameterError {
    /// The rejected parameter name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(feature = "bmc-http")]
impl fmt::Display for MultipartUpdateParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reserved update parameter name: {}", self.name)
    }
}

#[cfg(feature = "bmc-http")]
impl StdError for MultipartUpdateParameterError {}

#[cfg(feature = "bmc-http")]
fn is_reserved_update_parameter(name: &str) -> bool {
    matches!(name, FORCE_UPDATE_PARAMETER | TARGETS_PARAMETER)
}

/// Update service.
///
/// Provides functions to access firmware and software inventory, and perform update actions.
pub struct UpdateService<B: Bmc> {
    bmc: NvBmc<B>,
    data: Arc<UpdateServiceSchema>,
    fw_inventory_read_patch_fn: Option<ReadPatchFn>,
}

impl<B: Bmc> UpdateService<B> {
    /// Create a new update service handle.
    pub(crate) async fn new(
        bmc: &NvBmc<B>,
        root: &ServiceRoot<B>,
    ) -> Result<Option<Self>, Error<B>> {
        let mut service_patches = Vec::new();
        if bmc.quirks.bug_missing_update_service_name_field() {
            service_patches.push(add_default_update_service_name);
        }
        let service_patch_fn = (!service_patches.is_empty()).then(|| {
            Arc::new(move |v| service_patches.iter().fold(v, |acc, f| f(acc))) as ReadPatchFn
        });

        let mut fw_inventory_patches = Vec::new();
        if bmc.quirks.fw_inventory_wrong_release_date() {
            fw_inventory_patches.push(fw_inventory_patch_wrong_release_date);
        }
        let fw_inventory_read_patch_fn = (!fw_inventory_patches.is_empty()).then(|| {
            Arc::new(move |v| fw_inventory_patches.iter().fold(v, |acc, f| f(acc))) as ReadPatchFn
        });

        if let Some(nav) = &root.root.update_service {
            if let Some(service_patch_fn) = service_patch_fn {
                Payload::get(bmc.as_ref(), nav, service_patch_fn.as_ref()).await
            } else {
                nav.get(bmc.as_ref()).await.map_err(Error::Bmc)
            }
            .map(Some)
        } else if bmc.quirks.bug_missing_root_nav_properties() {
            let nav =
                NavProperty::new_reference(format!("{}/UpdateService", root.odata_id()).into());
            if let Some(service_patch_fn) = service_patch_fn {
                Payload::get(bmc.as_ref(), &nav, service_patch_fn.as_ref()).await
            } else {
                nav.get(bmc.as_ref()).await.map_err(Error::Bmc)
            }
            .map(Some)
        } else {
            Ok(None)
        }
        .map(|d| {
            d.map(|data| Self {
                bmc: bmc.clone(),
                data,
                fw_inventory_read_patch_fn,
            })
        })
    }

    /// Get the raw schema data for this update service.
    ///
    /// Returns an `Arc` to the underlying schema, allowing cheap cloning
    /// and sharing of the data.
    #[must_use]
    pub fn raw(&self) -> Arc<UpdateServiceSchema> {
        self.data.clone()
    }

    /// List all firmware inventory items.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The update service does not have a firmware inventory collection
    /// - Fetching firmware inventory data fails
    pub async fn firmware_inventories(
        &self,
    ) -> Result<Option<Vec<SoftwareInventory<B>>>, Error<B>> {
        if let Some(collection_ref) = &self.data.firmware_inventory {
            SoftwareInventoryCollection::new(
                &self.bmc,
                collection_ref,
                self.fw_inventory_read_patch_fn.clone(),
            )
            .await?
            .members()
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    }

    /// List all software inventory items.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The update service does not have a software inventory collection
    /// - Fetching software inventory data fails
    pub async fn software_inventories(
        &self,
    ) -> Result<Option<Vec<SoftwareInventory<B>>>, Error<B>> {
        if let Some(collection_ref) = &self.data.software_inventory {
            let collection = self.bmc.expand_property(collection_ref).await?;
            let mut items = Vec::new();
            for item_ref in &collection.members {
                items.push(SoftwareInventory::new(&self.bmc, item_ref, None).await?);
            }
            Ok(Some(items))
        } else {
            Ok(None)
        }
    }

    /// Perform a simple update with the specified image URI.
    ///
    /// This action updates software components by downloading and installing
    /// a software image from the specified URI.
    ///
    /// # Arguments
    ///
    /// * `image_uri` - The URI of the software image to install
    /// * `transfer_protocol` - Optional network protocol to use for retrieving the image
    /// * `targets` - Optional list of URIs indicating where to apply the update
    /// * `username` - Optional username for accessing the image URI
    /// * `password` - Optional password for accessing the image URI
    /// * `force_update` - Whether to bypass update policies (e.g., allow downgrade)
    /// * `stage` - Whether to stage the image for later activation instead of immediate installation
    /// * `local_image` - An indication of whether the service adds the image to the local image store
    /// * `exclude_targets` - An array of URIs that indicate where not to apply the update image
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The update service does not support the `SimpleUpdate` action
    /// - The action execution fails
    #[allow(clippy::too_many_arguments)]
    pub async fn simple_update(
        &self,
        image_uri: String,
        transfer_protocol: Option<TransferProtocolType>,
        targets: Option<Vec<String>>,
        username: Option<String>,
        password: Option<String>,
        force_update: Option<bool>,
        stage: Option<bool>,
        local_image: Option<bool>,
        exclude_targets: Option<Vec<String>>,
    ) -> Result<ModificationResponse<()>, Error<B>>
    where
        B::Error: nv_redfish_core::ActionError,
    {
        let actions = self
            .data
            .actions
            .as_ref()
            .ok_or(Error::ActionNotAvailable)?;

        actions
            .simple_update(
                self.bmc.as_ref(),
                &UpdateServiceSimpleUpdateAction {
                    image_uri: Some(image_uri),
                    transfer_protocol,
                    targets,
                    username,
                    password,
                    force_update,
                    stage,
                    local_image,
                    exclude_targets,
                },
            )
            .await
            .map_err(Error::Bmc)
    }

    /// Start updates that have been previously invoked with an `OperationApplyTime` of `OnStartUpdateRequest`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The update service does not support the `StartUpdate` action
    /// - The action execution fails
    pub async fn start_update(&self) -> Result<ModificationResponse<()>, Error<B>>
    where
        B::Error: nv_redfish_core::ActionError,
    {
        let actions = self
            .data
            .actions
            .as_ref()
            .ok_or(Error::ActionNotAvailable)?;

        actions
            .start_update(self.bmc.as_ref())
            .await
            .map_err(Error::Bmc)
    }
}

#[cfg(feature = "bmc-http")]
impl UpdateService<HttpBmc<ReqwestClient>> {
    /// Perform a multipart update upload with `UpdateFile` read from a file path.
    ///
    /// The request posts to the caller supplied multipart URI and reuses the BMC's
    /// reqwest client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, the multipart request fails,
    /// or the BMC returns an unsuccessful HTTP status.
    pub async fn multipart_update_from_path<P>(
        &self,
        multipart_uri: impl AsRef<str>,
        update_parameters: &MultipartUpdateParameters,
        update_file: P,
        upload_timeout: Duration,
    ) -> Result<MultipartUpdateResponse, UploadError<Error<HttpBmc<ReqwestClient>>>>
    where
        P: AsRef<Path>,
    {
        self.bmc
            .as_ref()
            .post_update_multipart_from_path(
                multipart_uri.as_ref(),
                update_parameters,
                update_file,
                upload_timeout,
            )
            .await
            .map_err(|err| err.map_request(Error::Bmc))
    }

    /// Perform a raw file update upload with the file read from a file path.
    ///
    /// The request uses HTTP PUT with an `application/octet-stream` body and
    /// reuses the BMC's reqwest client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, the upload request fails,
    /// or the BMC returns an unsuccessful HTTP status.
    pub async fn raw_update_from_path<P>(
        &self,
        update_uri: impl AsRef<str>,
        update_file: P,
        upload_timeout: Duration,
    ) -> Result<RawUpdateResponse, UploadError<Error<HttpBmc<ReqwestClient>>>>
    where
        P: AsRef<Path>,
    {
        self.bmc
            .as_ref()
            .put_update_file_from_path(update_uri.as_ref(), update_file, upload_timeout)
            .await
            .map_err(|err| err.map_request(Error::Bmc))
    }
}

#[cfg(feature = "bmc-http")]
impl<C> UpdateService<HttpBmc<C>>
where
    C: MultipartHttpClient,
    C::Error: CacheableError + From<RedfishUriError> + StdError + Send + Sync,
{
    /// Perform a multipart update upload with `UpdateFile` read from an async reader.
    ///
    /// The request posts to the caller supplied multipart URI and reuses the BMC's
    /// HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the multipart request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn multipart_update_from_reader<R>(
        &self,
        multipart_uri: impl AsRef<str>,
        update_parameters: &MultipartUpdateParameters,
        file_name: impl Into<String>,
        update_file: R,
        upload_timeout: Duration,
    ) -> Result<MultipartUpdateResponse, Error<HttpBmc<C>>>
    where
        R: MultipartUploadReader,
    {
        self.bmc
            .as_ref()
            .post_update_multipart_from_reader(
                multipart_uri,
                update_parameters,
                file_name,
                update_file,
                upload_timeout,
            )
            .await
            .map_err(Error::Bmc)
    }
}

#[cfg(feature = "bmc-http")]
impl<C> UpdateService<HttpBmc<C>>
where
    C: RawFileUploadHttpClient,
    C::Error: CacheableError + From<RedfishUriError> + StdError + Send + Sync,
{
    /// Perform a raw file update upload with the file read from an async reader.
    ///
    /// The request uses HTTP PUT with an `application/octet-stream` body and
    /// reuses the BMC's HTTP client, credentials, and custom headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload request fails or the BMC returns an
    /// unsuccessful HTTP status.
    pub async fn raw_update_from_reader<R>(
        &self,
        update_uri: impl AsRef<str>,
        update_file: R,
        upload_timeout: Duration,
    ) -> Result<RawUpdateResponse, Error<HttpBmc<C>>>
    where
        R: UploadReader,
    {
        self.bmc
            .as_ref()
            .put_update_file_from_reader(update_uri, update_file, upload_timeout)
            .await
            .map_err(Error::Bmc)
    }
}

impl<B: Bmc> Resource for UpdateService<B> {
    fn resource_ref(&self) -> &ResourceSchema {
        &self.data.as_ref().base
    }
}

// `ReleaseDate` is marked as `edm.DateTimeOffset`, but some systems
// puts "00:00:00Z" as ReleaseDate that is not conform to ABNF of the DateTimeOffset.
// we delete such fields...
fn fw_inventory_patch_wrong_release_date(v: JsonValue) -> JsonValue {
    if let JsonValue::Object(mut obj) = v {
        if let Some(JsonValue::String(date)) = obj.get("ReleaseDate") {
            if date == "00:00:00Z" || date == "0000-00-00T00:00:00Z" {
                obj.remove("ReleaseDate");
            }
        }
        JsonValue::Object(obj)
    } else {
        v
    }
}

fn add_default_update_service_name(v: JsonValue) -> JsonValue {
    if let JsonValue::Object(mut obj) = v {
        obj.entry("Name")
            .or_insert(JsonValue::String("Unnamed update service".into()));
        JsonValue::Object(obj)
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "bmc-http")]
    mod bmc_http {
        use super::super::*;
        use serde_json::json;

        type TestResult = Result<(), String>;

        #[test]
        fn multipart_update_parameters_accept_extra_parameter() -> TestResult {
            let params = MultipartUpdateParameters::new(false, Vec::new())
                .with_parameter("ApplyTime", json!("Immediate"))
                .map_err(|err| err.to_string())?;

            assert_eq!(
                params.additional_parameters().get("ApplyTime"),
                Some(&json!("Immediate"))
            );

            Ok(())
        }

        #[test]
        fn multipart_update_parameters_reject_reserved_parameters() -> TestResult {
            for name in [FORCE_UPDATE_PARAMETER, TARGETS_PARAMETER] {
                let result = MultipartUpdateParameters::new(false, Vec::new())
                    .with_parameter(name, json!(true));
                let Err(err) = result else {
                    return Err(format!("expected reserved parameter error for {name}"));
                };

                assert_eq!(err.name(), name);
            }

            Ok(())
        }
    }
}
