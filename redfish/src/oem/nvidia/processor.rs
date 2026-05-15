// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

//! NVIDIA processor OEM extensions.

use crate::schema::resource::Oem as ResourceOemSchema;
use crate::schema::resource::Resource as ResourceSchemaData;
use crate::Error;
use crate::NvBmc;
use crate::Resource;
use crate::ResourceSchema;
use nv_redfish_core::Action;
use nv_redfish_core::Bmc;
use nv_redfish_core::EntityTypeRef;
use nv_redfish_core::ModificationResponse;
use nv_redfish_core::NavProperty;
use nv_redfish_core::ODataETag;
use nv_redfish_core::ODataId;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

const NVIDIA_WORKLOAD_POWER_PROFILE: &str = "Oem/Nvidia/WorkloadPowerProfile";

/// NVIDIA MNNVLink topology data for a processor.
#[derive(Debug, Deserialize)]
pub struct NvidiaMnnvLinkTopology {
    /// Chassis serial number associated with the topology.
    #[serde(rename = "ChassisSerialNumber", default)]
    pub chassis_serial_number: Option<String>,
    /// Tray slot number associated with the topology.
    #[serde(rename = "TraySlotNumber", default)]
    pub tray_slot_number: Option<i64>,
    /// Tray slot index associated with the topology.
    #[serde(rename = "TraySlotIndex", default)]
    pub tray_slot_index: Option<i64>,
}

impl NvidiaMnnvLinkTopology {
    pub(crate) fn from_resource_oem<B: Bmc>(
        oem: &ResourceOemSchema,
    ) -> Result<Option<Self>, Error<B>> {
        let oem: NvidiaProcessorOem =
            serde_json::from_value(oem.additional_properties.clone()).map_err(Error::Json)?;

        Ok(oem.nvidia.and_then(|nvidia| nvidia.mnnvlink_topology))
    }
}

/// Represents the NVIDIA workload power profile resource.
pub struct NvidiaWorkloadPowerProfile<B: Bmc> {
    bmc: NvBmc<B>,
    data: Arc<NvidiaWorkloadPowerProfileData>,
}

impl<B: Bmc> NvidiaWorkloadPowerProfile<B> {
    pub(crate) async fn new(bmc: &NvBmc<B>, processor_id: &ODataId) -> Result<Self, Error<B>> {
        let id = format!("{processor_id}/{NVIDIA_WORKLOAD_POWER_PROFILE}").into();

        NavProperty::<NvidiaWorkloadPowerProfileData>::new_reference(id)
            .get(bmc.as_ref())
            .await
            .map_err(Error::Bmc)
            .map(|data| Self {
                bmc: bmc.clone(),
                data,
            })
    }

    /// Get the raw resource schema data for this workload power profile.
    #[must_use]
    pub fn raw(&self) -> &ResourceSchemaData {
        &self.data.base
    }

    /// Enable workload power profiles using a profile mask.
    ///
    /// # Errors
    ///
    /// Returns an error if the action is not available or the BMC rejects it.
    pub async fn enable_profiles(
        &self,
        profile_mask: impl Into<String>,
    ) -> Result<ModificationResponse<()>, Error<B>> {
        let action = self
            .data
            .actions
            .as_ref()
            .and_then(|actions| actions.enable_profiles.as_ref())
            .ok_or(Error::ActionNotAvailable)?;

        action
            .run(
                self.bmc.as_ref(),
                &NvidiaWorkloadPowerProfileRequest {
                    profile_mask: profile_mask.into(),
                },
            )
            .await
            .map_err(Error::Bmc)
    }

    /// Disable workload power profiles using a profile mask.
    ///
    /// # Errors
    ///
    /// Returns an error if the action is not available or the BMC rejects it.
    pub async fn disable_profiles(
        &self,
        profile_mask: impl Into<String>,
    ) -> Result<ModificationResponse<()>, Error<B>> {
        let action = self
            .data
            .actions
            .as_ref()
            .and_then(|actions| actions.disable_profiles.as_ref())
            .ok_or(Error::ActionNotAvailable)?;

        action
            .run(
                self.bmc.as_ref(),
                &NvidiaWorkloadPowerProfileRequest {
                    profile_mask: profile_mask.into(),
                },
            )
            .await
            .map_err(Error::Bmc)
    }
}

impl<B: Bmc> Resource for NvidiaWorkloadPowerProfile<B> {
    fn resource_ref(&self) -> &ResourceSchema {
        &self.data.base
    }
}

#[derive(Deserialize, Debug)]
struct NvidiaProcessorOem {
    #[serde(rename = "Nvidia", default)]
    nvidia: Option<NvidiaProcessorOemData>,
}

#[derive(Deserialize, Debug)]
struct NvidiaProcessorOemData {
    #[serde(rename = "MNNVLinkTopology", default)]
    mnnvlink_topology: Option<NvidiaMnnvLinkTopology>,
}

#[derive(Deserialize, Debug)]
struct NvidiaWorkloadPowerProfileData {
    #[serde(flatten)]
    base: ResourceSchema,
    #[serde(rename = "Actions", default)]
    actions: Option<NvidiaWorkloadPowerProfileActions>,
}

impl EntityTypeRef for NvidiaWorkloadPowerProfileData {
    fn odata_id(&self) -> &ODataId {
        self.base.odata_id()
    }

    fn etag(&self) -> Option<&ODataETag> {
        self.base.etag()
    }
}

#[derive(Deserialize, Debug)]
struct NvidiaWorkloadPowerProfileActions {
    #[serde(rename = "#NvidiaWorkloadPower.EnableProfiles", default)]
    enable_profiles: Option<Action<NvidiaWorkloadPowerProfileRequest, ()>>,
    #[serde(rename = "#NvidiaWorkloadPower.DisableProfiles", default)]
    disable_profiles: Option<Action<NvidiaWorkloadPowerProfileRequest, ()>>,
}

#[derive(Serialize, Debug)]
struct NvidiaWorkloadPowerProfileRequest {
    #[serde(rename = "ProfileMask")]
    profile_mask: String,
}
