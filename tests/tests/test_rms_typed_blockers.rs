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
//! Integration tests for typed RMS migration blockers.

use nv_redfish::schema::resource::Health;
use nv_redfish::task::TaskState;
use nv_redfish::ServiceRoot;
use nv_redfish_core::EntityTypeRef as _;
use nv_redfish_core::ODataId;
use nv_redfish_tests::Bmc;
use nv_redfish_tests::Expect;
use nv_redfish_tests::ODATA_ID;
use nv_redfish_tests::ODATA_TYPE;
use serde_json::json;
use serde_json::Value;
use std::error::Error as StdError;
use std::io;
use std::sync::Arc;
use tokio::test;

const SERVICE_ROOT_DATA_TYPE: &str = "#ServiceRoot.v1_13_0.ServiceRoot";
const SYSTEM_COLLECTION_DATA_TYPE: &str = "#ComputerSystemCollection.ComputerSystemCollection";
const SYSTEM_DATA_TYPE: &str = "#ComputerSystem.v1_20_0.ComputerSystem";
const PROCESSOR_COLLECTION_DATA_TYPE: &str = "#ProcessorCollection.ProcessorCollection";
const PROCESSOR_DATA_TYPE: &str = "#Processor.v1_20_0.Processor";
const ENVIRONMENT_METRICS_DATA_TYPE: &str = "#EnvironmentMetrics.v1_3_0.EnvironmentMetrics";
const WORKLOAD_POWER_PROFILE_DATA_TYPE: &str = "#NvidiaWorkloadPower.v1_0_0.WorkloadPowerProfile";
const CHASSIS_COLLECTION_DATA_TYPE: &str = "#ChassisCollection.ChassisCollection";
const CHASSIS_DATA_TYPE: &str = "#Chassis.v1_22_0.Chassis";
const TASK_DATA_TYPE: &str = "#Task.v1_7_0.Task";

#[test]
async fn processor_environment_metrics_and_nvidia_profile_actions() -> Result<(), Box<dyn StdError>>
{
    let bmc = Arc::new(Bmc::default());
    let ids = ProcessorIds::new();
    let root = expect_service_root(
        bmc.clone(),
        json!({
            "Systems": { ODATA_ID: &ids.systems_id }
        }),
    )
    .await?;

    bmc.expect(Expect::get(
        &ids.systems_id,
        collection_payload(
            &ids.systems_id,
            SYSTEM_COLLECTION_DATA_TYPE,
            vec![system_payload(&ids)],
        ),
    ));

    let systems = root
        .systems()
        .await?
        .ok_or_else(|| test_error("missing systems collection"))?;
    let systems = systems.members().await?;
    let system = systems
        .into_iter()
        .next()
        .ok_or_else(|| test_error("missing system"))?;

    bmc.expect(Expect::get(
        &ids.processors_id,
        collection_payload(
            &ids.processors_id,
            PROCESSOR_COLLECTION_DATA_TYPE,
            vec![processor_payload(&ids)],
        ),
    ));

    let processors = system
        .processors()
        .await?
        .ok_or_else(|| test_error("missing processors collection"))?;
    let processor = processors
        .into_iter()
        .next()
        .ok_or_else(|| test_error("missing processor"))?;

    let topology = processor
        .nvidia_mnnvlink_topology()?
        .ok_or_else(|| test_error("missing topology"))?;
    assert_eq!(
        topology.chassis_serial_number.as_deref(),
        Some("CHASSIS123")
    );
    assert_eq!(topology.tray_slot_number, Some(7));
    assert_eq!(topology.tray_slot_index, Some(3));

    bmc.expect(Expect::get(
        &ids.environment_metrics_id,
        environment_metrics_payload(&ids.environment_metrics_id, 450),
    ));

    let metrics = processor
        .environment_metrics()
        .await?
        .ok_or_else(|| test_error("missing environment metrics"))?;
    let power_limit = metrics
        .power_limit_watts()
        .ok_or_else(|| test_error("missing power limit"))?;
    assert_eq!(power_limit.allowable_min.flatten(), Some(300.0));
    assert_eq!(power_limit.allowable_max.flatten(), Some(700.0));
    assert_eq!(power_limit.set_point.flatten(), Some(450.0));

    bmc.expect(Expect::update(
        &ids.environment_metrics_id,
        json!({
            "PowerLimitWatts": {
                "SetPoint": 650
            }
        }),
        environment_metrics_payload(&ids.environment_metrics_id, 650),
    ));

    let updated = metrics
        .set_power_limit_watts(650)
        .await?
        .ok_or_else(|| test_error("missing updated metrics"))?;
    assert_eq!(
        updated
            .power_limit_watts()
            .and_then(|power| power.set_point.flatten()),
        Some(650.0)
    );

    bmc.expect(Expect::get(
        &ids.workload_power_profile_id,
        workload_power_profile_payload(&ids),
    ));

    let profile = processor.nvidia_workload_power_profile().await?;
    bmc.expect(Expect::action(
        &ids.enable_profiles_id,
        json!({ "ProfileMask": "0x3" }),
        json!(null),
    ));
    profile.enable_profiles("0x3").await?;

    bmc.expect(Expect::action(
        &ids.disable_profiles_id,
        json!({ "ProfileMask": "0x2" }),
        json!(null),
    ));
    profile.disable_profiles("0x2").await?;

    Ok(())
}

#[test]
async fn chassis_fixed_power_actions_use_typed_bodies() -> Result<(), Box<dyn StdError>> {
    let bmc = Arc::new(Bmc::default());
    let ids = ChassisIds::new();
    let root = expect_service_root(
        bmc.clone(),
        json!({
            "Chassis": { ODATA_ID: &ids.chassis_collection_id }
        }),
    )
    .await?;

    bmc.expect(Expect::get(
        &ids.chassis_collection_id,
        collection_payload(
            &ids.chassis_collection_id,
            CHASSIS_COLLECTION_DATA_TYPE,
            vec![
                chassis_payload(&ids.bmc_chassis_id),
                chassis_payload(&ids.powershelf_chassis_id),
            ],
        ),
    ));

    let chassis = root
        .chassis()
        .await?
        .ok_or_else(|| test_error("missing chassis collection"))?
        .members()
        .await?;
    let bmc_chassis = chassis
        .iter()
        .find(|chassis| chassis.raw().base.odata_id().to_string() == ids.bmc_chassis_id)
        .ok_or_else(|| test_error("missing BMC chassis"))?;
    let powershelf = chassis
        .iter()
        .find(|chassis| chassis.raw().base.odata_id().to_string() == ids.powershelf_chassis_id)
        .ok_or_else(|| test_error("missing powershelf chassis"))?;

    bmc.expect(Expect::action(
        format!(
            "{}/Actions/Oem/NvidiaChassis.AuxPowerReset",
            ids.bmc_chassis_id
        ),
        json!({ "ResetType": "AuxPowerCycle" }),
        json!(null),
    ));
    bmc_chassis.aux_power_reset().await?;

    bmc.expect(Expect::action(
        format!("{}/Actions/Chassis.ForceOff", ids.powershelf_chassis_id),
        json!({ "ForceOffType": "ForceOff" }),
        json!(null),
    ));
    powershelf.powershelf_force_off().await?;

    bmc.expect(Expect::action(
        format!("{}/Actions/Chassis.On", ids.powershelf_chassis_id),
        json!({ "OnType": "On" }),
        json!(null),
    ));
    powershelf.powershelf_on().await?;

    Ok(())
}

#[test]
async fn task_polling_reads_status_progress_and_messages() -> Result<(), Box<dyn StdError>> {
    let bmc = Arc::new(Bmc::default());
    let task_id = "42";
    let task_uri = format!("/redfish/v1/TaskService/Tasks/{task_id}");
    let root = expect_service_root(bmc.clone(), json!({})).await?;

    bmc.expect(Expect::get(
        &task_uri,
        json!({
            ODATA_ID: &task_uri,
            ODATA_TYPE: TASK_DATA_TYPE,
            "Id": task_id,
            "Name": "Task 42",
            "TaskState": "Running",
            "TaskStatus": "OK",
            "PercentComplete": 75,
            "Messages": [
                {
                    "MessageId": "Base.1.0.Success",
                    "Message": "Update is running"
                }
            ]
        }),
    ));

    let task = root.task(task_id).await?;

    assert_eq!(task.task_state(), Some(TaskState::Running));
    assert_eq!(task.task_status(), Some(Health::Ok));
    assert_eq!(task.percent_complete(), Some(75));
    assert_eq!(
        task.messages().collect::<Vec<_>>(),
        vec!["Update is running"]
    );

    Ok(())
}

async fn expect_service_root(
    bmc: Arc<Bmc>,
    fields: Value,
) -> Result<ServiceRoot<Bmc>, Box<dyn StdError>> {
    bmc.expect(Expect::get(
        ODataId::service_root(),
        merge_json(
            json!({
                ODATA_ID: ODataId::service_root(),
                ODATA_TYPE: SERVICE_ROOT_DATA_TYPE,
                "Id": "RootService",
                "Name": "RootService",
                "Links": {
                    "Sessions": {
                        ODATA_ID: "/redfish/v1/SessionService/Sessions"
                    }
                }
            }),
            fields,
        ),
    ));

    ServiceRoot::new(bmc).await.map_err(Into::into)
}

fn collection_payload(id: &str, odata_type: &str, members: Vec<Value>) -> Value {
    json!({
        ODATA_ID: id,
        ODATA_TYPE: odata_type,
        "Id": resource_name(id),
        "Name": resource_name(id),
        "Members": members
    })
}

fn system_payload(ids: &ProcessorIds) -> Value {
    json!({
        ODATA_ID: &ids.system_id,
        ODATA_TYPE: SYSTEM_DATA_TYPE,
        "Id": resource_name(&ids.system_id),
        "Name": resource_name(&ids.system_id),
        "Processors": {
            ODATA_ID: &ids.processors_id
        },
        "Status": {
            "Health": "OK",
            "State": "Enabled"
        }
    })
}

fn processor_payload(ids: &ProcessorIds) -> Value {
    json!({
        ODATA_ID: &ids.processor_id,
        ODATA_TYPE: PROCESSOR_DATA_TYPE,
        "Id": resource_name(&ids.processor_id),
        "Name": resource_name(&ids.processor_id),
        "EnvironmentMetrics": {
            ODATA_ID: &ids.environment_metrics_id
        },
        "Oem": {
            "Nvidia": {
                "MNNVLinkTopology": {
                    "ChassisSerialNumber": "CHASSIS123",
                    "TraySlotNumber": 7,
                    "TraySlotIndex": 3
                }
            }
        }
    })
}

fn environment_metrics_payload(id: &str, set_point: u32) -> Value {
    json!({
        ODATA_ID: id,
        ODATA_TYPE: ENVIRONMENT_METRICS_DATA_TYPE,
        "Id": resource_name(id),
        "Name": resource_name(id),
        "PowerLimitWatts": {
            "AllowableMin": 300,
            "AllowableMax": 700,
            "SetPoint": set_point
        }
    })
}

fn workload_power_profile_payload(ids: &ProcessorIds) -> Value {
    json!({
        ODATA_ID: &ids.workload_power_profile_id,
        ODATA_TYPE: WORKLOAD_POWER_PROFILE_DATA_TYPE,
        "Id": resource_name(&ids.workload_power_profile_id),
        "Name": resource_name(&ids.workload_power_profile_id),
        "Actions": {
            "#NvidiaWorkloadPower.EnableProfiles": {
                "target": &ids.enable_profiles_id
            },
            "#NvidiaWorkloadPower.DisableProfiles": {
                "target": &ids.disable_profiles_id
            }
        }
    })
}

fn chassis_payload(id: &str) -> Value {
    json!({
        ODATA_ID: id,
        ODATA_TYPE: CHASSIS_DATA_TYPE,
        "Id": resource_name(id),
        "Name": resource_name(id),
        "ChassisType": "Component",
        "Status": {
            "Health": "OK",
            "State": "Enabled"
        }
    })
}

fn merge_json(mut base: Value, fields: Value) -> Value {
    if let (Some(base), Some(fields)) = (base.as_object_mut(), fields.as_object()) {
        for (key, value) in fields {
            base.insert(key.clone(), value.clone());
        }
    }

    base
}

fn resource_name(id: &str) -> &str {
    id.rsplit('/').next().map_or(id, |name| name)
}

fn test_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message)
}

struct ProcessorIds {
    systems_id: String,
    system_id: String,
    processors_id: String,
    processor_id: String,
    environment_metrics_id: String,
    workload_power_profile_id: String,
    enable_profiles_id: String,
    disable_profiles_id: String,
}

impl ProcessorIds {
    fn new() -> Self {
        let systems_id = String::from("/redfish/v1/Systems");
        let system_id = format!("{systems_id}/HGX_Baseboard_0");
        let processors_id = format!("{system_id}/Processors");
        let processor_id = format!("{processors_id}/GPU_0");
        let environment_metrics_id = format!("{processor_id}/EnvironmentMetrics");
        let workload_power_profile_id = format!("{processor_id}/Oem/Nvidia/WorkloadPowerProfile");
        let enable_profiles_id =
            format!("{workload_power_profile_id}/Actions/NvidiaWorkloadPower.EnableProfiles");
        let disable_profiles_id =
            format!("{workload_power_profile_id}/Actions/NvidiaWorkloadPower.DisableProfiles");

        Self {
            systems_id,
            system_id,
            processors_id,
            processor_id,
            environment_metrics_id,
            workload_power_profile_id,
            enable_profiles_id,
            disable_profiles_id,
        }
    }
}

struct ChassisIds {
    chassis_collection_id: String,
    bmc_chassis_id: String,
    powershelf_chassis_id: String,
}

impl ChassisIds {
    fn new() -> Self {
        let chassis_collection_id = String::from("/redfish/v1/Chassis");
        let bmc_chassis_id = format!("{chassis_collection_id}/BMC_0");
        let powershelf_chassis_id = format!("{chassis_collection_id}/powershelf");

        Self {
            chassis_collection_id,
            bmc_chassis_id,
            powershelf_chassis_id,
        }
    }
}
