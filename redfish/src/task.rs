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

//! Redfish task polling support.

use crate::schema::resource::Health;
use crate::schema::task::Task as TaskSchema;
use crate::Error;
use crate::NvBmc;
use crate::Resource;
use crate::ResourceSchema;
use nv_redfish_core::Bmc;
use nv_redfish_core::NavProperty;
use nv_redfish_core::ODataId;
use std::marker::PhantomData;
use std::sync::Arc;

#[doc(inline)]
pub use crate::schema::task::TaskState;

/// Represents a Redfish `Task` resource.
pub struct Task<B: Bmc> {
    data: Arc<TaskSchema>,
    _marker: PhantomData<B>,
}

impl<B: Bmc> Task<B> {
    /// Create a task handle from a task URI.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching task data fails.
    pub(crate) async fn new(bmc: &NvBmc<B>, id: ODataId) -> Result<Self, Error<B>> {
        NavProperty::<TaskSchema>::new_reference(id)
            .get(bmc.as_ref())
            .await
            .map_err(Error::Bmc)
            .map(|data| Self {
                data,
                _marker: PhantomData,
            })
    }

    /// Get the raw schema data for this task.
    #[must_use]
    pub fn raw(&self) -> Arc<TaskSchema> {
        self.data.clone()
    }

    /// Get the task state.
    #[must_use]
    pub fn task_state(&self) -> Option<TaskState> {
        self.data.task_state
    }

    /// Get the task status.
    #[must_use]
    pub fn task_status(&self) -> Option<Health> {
        self.data.task_status
    }

    /// Get the percent complete value.
    #[must_use]
    pub fn percent_complete(&self) -> Option<i64> {
        self.data.percent_complete.flatten()
    }

    /// Iterate over task message text values.
    pub fn messages(&self) -> impl Iterator<Item = &str> {
        self.data
            .messages
            .iter()
            .flatten()
            .filter_map(|message| message.message.as_deref())
    }
}

impl<B: Bmc> Resource for Task<B> {
    fn resource_ref(&self) -> &ResourceSchema {
        &self.data.as_ref().base
    }
}
