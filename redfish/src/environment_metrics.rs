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

//! Environment metrics typed helpers.

use crate::schema::control::ControlExcerptSingle;
use crate::schema::environment_metrics::EnvironmentMetrics as EnvironmentMetricsSchema;
use crate::Error;
use crate::NvBmc;
use crate::Resource;
use crate::ResourceSchema;
use nv_redfish_core::Bmc;
use nv_redfish_core::EntityTypeRef as _;
use nv_redfish_core::ModificationResponse;
use nv_redfish_core::NavProperty;
use serde::Serialize;
use std::sync::Arc;

/// Represents a Redfish `EnvironmentMetrics` resource.
pub struct EnvironmentMetrics<B: Bmc> {
    bmc: NvBmc<B>,
    data: Arc<EnvironmentMetricsSchema>,
}

impl<B: Bmc> EnvironmentMetrics<B> {
    /// Create an environment metrics handle.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching environment metrics data fails.
    pub(crate) async fn new(
        bmc: &NvBmc<B>,
        nav: &NavProperty<EnvironmentMetricsSchema>,
    ) -> Result<Self, Error<B>> {
        nav.get(bmc.as_ref())
            .await
            .map_err(Error::Bmc)
            .map(|data| Self {
                bmc: bmc.clone(),
                data,
            })
    }

    /// Get the raw schema data for this environment metrics resource.
    #[must_use]
    pub fn raw(&self) -> Arc<EnvironmentMetricsSchema> {
        self.data.clone()
    }

    /// Get the power limit in watts, if the BMC reports it.
    #[must_use]
    pub fn power_limit_watts(&self) -> Option<&ControlExcerptSingle> {
        self.data.power_limit_watts.as_ref()
    }

    /// Patch `PowerLimitWatts.SetPoint`.
    ///
    /// # Errors
    ///
    /// Returns an error if the BMC rejects the patch or the response cannot be parsed.
    pub async fn set_power_limit_watts(&self, set_point: u32) -> Result<Option<Self>, Error<B>> {
        let update = EnvironmentMetricsPowerLimitWattsUpdate {
            power_limit_watts: PowerLimitWattsSetPoint { set_point },
        };

        match self
            .bmc
            .as_ref()
            .update::<_, EnvironmentMetricsSchema>(self.data.odata_id(), self.data.etag(), &update)
            .await
            .map_err(Error::Bmc)?
        {
            ModificationResponse::Entity(data) => Ok(Some(Self {
                bmc: self.bmc.clone(),
                data: Arc::new(data),
            })),
            ModificationResponse::Task(_) | ModificationResponse::Empty => Ok(None),
        }
    }
}

impl<B: Bmc> Resource for EnvironmentMetrics<B> {
    fn resource_ref(&self) -> &ResourceSchema {
        &self.data.as_ref().base
    }
}

#[derive(Serialize)]
struct EnvironmentMetricsPowerLimitWattsUpdate {
    #[serde(rename = "PowerLimitWatts")]
    power_limit_watts: PowerLimitWattsSetPoint,
}

#[derive(Serialize)]
struct PowerLimitWattsSetPoint {
    #[serde(rename = "SetPoint")]
    set_point: u32,
}
