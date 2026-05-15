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

mod common;

#[cfg(feature = "reqwest")]
mod reqwest_client_tests {
    use nv_redfish_bmc_http::reqwest::BmcError;
    use nv_redfish_bmc_http::reqwest::Client;
    use nv_redfish_bmc_http::BmcCredentials;
    use nv_redfish_bmc_http::CacheSettings;
    use nv_redfish_bmc_http::HttpBmc;
    use nv_redfish_bmc_http::RedfishUriError;
    use nv_redfish_core::{
        query::{ExpandQuery, FilterQuery},
        Bmc, ModificationResponse,
    };
    use serde::Serialize;
    use serde_json::json;
    use std::error::Error as StdError;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use url::Url;
    use wiremock::{
        matchers::{body_json, header, method, path, query_param},
        Mock, MockServer, Request, ResponseTemplate,
    };

    use crate::common::test_utils::*;

    const MULTIPART_UPDATE_PATH: &str = "/redfish/v1/UpdateService/update-multipart";
    const OCTET_STREAM_MIME: &str = "application/octet-stream";
    const RAW_UPDATE_PATH: &str = "/redfish/v1/UpdateService/update";
    const UPLOAD_MODE_HEADER: &str = "X-Upload-Mode";

    #[derive(Serialize)]
    struct TestUpdateParameters {
        #[serde(rename = "ForceUpdate")]
        force_update: bool,
        #[serde(rename = "Targets")]
        targets: Vec<String>,
    }

    #[tokio::test]
    async fn test_get_request_success() {
        let mock_server = MockServer::start().await;
        let resource_path = paths::SYSTEMS_1;

        let test_resource =
            create_test_resource(resource_path, Some("123"), names::TEST_SYSTEM, 42);

        Mock::given(method("GET"))
            .and(path(resource_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(&test_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let result = bmc.get::<TestResource>(&resource_id).await;

        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.name, names::TEST_SYSTEM);
        assert_eq!(retrieved.value, 42);
    }

    #[tokio::test]
    async fn test_set_credentials() {
        let mock_server = MockServer::start().await;
        let first_resource_path = paths::SYSTEMS_1;
        let second_resource_path = paths::MANAGERS_1;

        let first_resource =
            create_test_resource(first_resource_path, Some("123"), names::TEST_SYSTEM, 42);
        let second_resource =
            create_test_resource(second_resource_path, Some("456"), names::TEST_MANAGER, 7);

        Mock::given(method("GET"))
            .and(path(first_resource_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(&first_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(second_resource_path))
            .and(header("X-Auth-Token", "new-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&second_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let first_id = create_odata_id(first_resource_path);
        let first = bmc.get::<TestResource>(&first_id).await.unwrap();
        assert_eq!(first.value, 42);

        bmc.set_credentials(BmcCredentials::token("new-token".to_string()));

        let second_id = create_odata_id(second_resource_path);
        let second = bmc.get::<TestResource>(&second_id).await.unwrap();
        assert_eq!(second.value, 7);
    }

    #[tokio::test]
    async fn test_get_request_with_expand() {
        let mock_server = MockServer::start().await;
        let resource_path = paths::SYSTEMS_1;

        let test_resource =
            create_test_resource(resource_path, Some("456"), names::TEST_SYSTEM, 100);

        Mock::given(method("GET"))
            .and(path(resource_path))
            .and(query_param("$expand", ".($levels=2)"))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(&test_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let expand_query = ExpandQuery::current().levels(2);
        let result = bmc.expand::<TestResource>(&resource_id, expand_query).await;

        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.name, names::TEST_SYSTEM);
        assert_eq!(retrieved.value, 100);
    }

    #[tokio::test]
    async fn test_get_request_with_filter() {
        let mock_server = MockServer::start().await;
        let resource_path = paths::SYSTEMS_1;

        let test_resource =
            create_test_resource(resource_path, Some("789"), names::TEST_SYSTEM, 50);

        Mock::given(method("GET"))
            .and(path(resource_path))
            .and(query_param("$filter", "value gt 10"))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(&test_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let filter_query = FilterQuery::gt(&"value", 10);
        let result = bmc.filter::<TestResource>(&resource_id, filter_query).await;

        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.name, names::TEST_SYSTEM);
        assert_eq!(retrieved.value, 50);
    }

    #[tokio::test]
    async fn test_post_create_request() {
        let mock_server = MockServer::start().await;
        let collection_path = paths::SYSTEMS_1;

        let create_request = CreateRequest {
            name: names::TEST_SYSTEM.to_string(),
            value: 999,
        };

        let created_resource =
            create_test_resource("/redfish/v1/systems/new", None, names::TEST_SYSTEM, 999);

        Mock::given(method("POST"))
            .and(path(collection_path))
            .and(body_json(&create_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(201).set_body_json(&created_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let collection_id = create_odata_id(collection_path);
        let result = bmc
            .create::<CreateRequest, TestResource>(&collection_id, &create_request)
            .await;

        assert!(result.is_ok());
        let created = match result.unwrap() {
            ModificationResponse::Entity(created) => created,
            _ => panic!("expected entity response"),
        };
        assert_eq!(created.name, names::TEST_SYSTEM);
        assert_eq!(created.value, 999);
    }

    #[tokio::test]
    async fn test_create_session_response() {
        let mock_server = MockServer::start().await;
        let collection_path = "/redfish/v1/SessionService/Sessions";
        let session_path = "/redfish/v1/SessionService/Sessions/1";

        let create_request = CreateRequest {
            name: names::TEST_SYSTEM.to_string(),
            value: 999,
        };
        let created_resource = create_test_resource(session_path, None, names::TEST_SYSTEM, 999);

        Mock::given(method("POST"))
            .and(path(collection_path))
            .and(body_json(&create_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("X-Auth-Token", "session-token-123")
                    .insert_header("Location", format!("https://bmc.example{session_path}"))
                    .set_body_json(&created_resource),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let collection_id = create_odata_id(collection_path);
        let response = bmc
            .create_session::<CreateRequest, TestResource>(&collection_id, &create_request)
            .await
            .unwrap();

        assert_eq!(response.auth_token, "session-token-123");
        assert_eq!(response.location.to_string(), session_path);
        assert_eq!(response.entity.name, names::TEST_SYSTEM);
        assert_eq!(response.entity.value, 999);
    }

    #[tokio::test]
    async fn test_create_session_missing_token_is_error() {
        let mock_server = MockServer::start().await;
        let collection_path = "/redfish/v1/SessionService/Sessions";
        let session_path = "/redfish/v1/SessionService/Sessions/1";

        let create_request = CreateRequest {
            name: names::TEST_SYSTEM.to_string(),
            value: 999,
        };
        let created_resource = create_test_resource(session_path, None, names::TEST_SYSTEM, 999);

        Mock::given(method("POST"))
            .and(path(collection_path))
            .and(body_json(&create_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("Location", session_path)
                    .set_body_json(&created_resource),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let collection_id = create_odata_id(collection_path);
        let error = bmc
            .create_session::<CreateRequest, TestResource>(&collection_id, &create_request)
            .await
            .unwrap_err();

        assert!(matches!(error, BmcError::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn test_create_session_missing_location_is_error() {
        let mock_server = MockServer::start().await;
        let collection_path = "/redfish/v1/SessionService/Sessions";
        let session_path = "/redfish/v1/SessionService/Sessions/1";

        let create_request = CreateRequest {
            name: names::TEST_SYSTEM.to_string(),
            value: 999,
        };
        let created_resource = create_test_resource(session_path, None, names::TEST_SYSTEM, 999);

        Mock::given(method("POST"))
            .and(path(collection_path))
            .and(body_json(&create_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("X-Auth-Token", "session-token-123")
                    .set_body_json(&created_resource),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let collection_id = create_odata_id(collection_path);
        let error = bmc
            .create_session::<CreateRequest, TestResource>(&collection_id, &create_request)
            .await
            .unwrap_err();

        assert!(matches!(error, BmcError::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn test_patch_update_request() {
        let mock_server = MockServer::start().await;
        let resource_path = "/redfish/v1/systems/1";

        let update_request = UpdateRequest {
            name: Some("Updated System".to_string()),
            value: None,
        };

        let etag = create_odata_etag("abc123");

        let updated_resource = TestResource {
            id: create_odata_id(resource_path),
            etag: None,
            name: "Updated System".to_string(),
            value: 42,
        };

        Mock::given(method("PATCH"))
            .and(path(resource_path))
            .and(body_json(&update_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("If-Match", "abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&updated_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(resource_path))
            .and(body_json(&update_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("If-Match", "*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&updated_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let result = bmc
            .update::<UpdateRequest, TestResource>(&resource_id, Some(&etag), &update_request)
            .await;

        assert!(result.is_ok());
        let updated = match result.unwrap() {
            ModificationResponse::Entity(updated) => updated,
            _ => panic!("expected entity response"),
        };
        assert_eq!(updated.name, "Updated System");
        assert_eq!(updated.value, 42);

        let no_etag = bmc
            .update::<UpdateRequest, TestResource>(&resource_id, None, &update_request)
            .await;

        assert!(no_etag.is_ok());
    }

    #[tokio::test]
    async fn test_delete_request() {
        let mock_server = MockServer::start().await;
        let resource_path = "/redfish/v1/systems/1";

        Mock::given(method("DELETE"))
            .and(path(resource_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let result = bmc.delete::<TestResource>(&resource_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_action_request() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let action_path = "/redfish/v1/systems/1/Actions/ComputerSystem.Reset";

        let action_request = ActionRequest {
            parameter: "ForceRestart".to_string(),
        };

        let action_response = ActionResponse {
            result: "Reset initiated".to_string(),
            success: true,
        };

        Mock::given(method("POST"))
            .and(path(action_path))
            .and(body_json(&action_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(&action_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let action = create_test_action(action_path);
        let result = bmc.action(&action, &action_request).await;

        assert!(result.is_ok());

        assert!(matches!(result?, ModificationResponse::Empty));

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_json_get_no_auth_query_headers_and_empty_body(
    ) -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let relative_path = "/redfish/v1/Managers/BMC/Oem/Passthrough";
        let absolute_path = "/redfish/v1/Managers/BMC/Oem/Absolute";
        let response_body = json!({
            "Result": "Ready",
            "State": "Enabled"
        });

        Mock::given(method("GET"))
            .and(path(relative_path))
            .and(query_param("expand", "all"))
            .and(header("X-Passthrough", "rms"))
            .and(|request: &Request| missing_header(request, "authorization"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("ETag", "ignored-etag")
                    .set_body_json(&response_body),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(absolute_path))
            .and(query_param("mode", "absolute"))
            .and(header("X-Passthrough", "rms"))
            .and(|request: &Request| missing_header(request, "authorization"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert("X-Passthrough", http::HeaderValue::from_static("rms"));

        let credentials = BmcCredentials::username_password(String::new(), None);
        let bmc = HttpBmc::with_custom_headers(
            Client::new()?,
            Url::parse(&mock_server.uri())?,
            credentials,
            CacheSettings::default(),
            custom_headers,
        );

        let response = bmc.get_json(format!("{relative_path}?expand=all")).await?;
        assert_eq!(response, response_body);

        let absolute_uri = format!("{}{absolute_path}?mode=absolute", mock_server.uri());
        let response = bmc.get_json(absolute_uri).await?;
        assert_eq!(response, json!({}));

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_json_rejects_unsafe_uri() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let bmc = create_test_bmc(&mock_server);

        let error = bmc
            .get_json("https://example.com/redfish/v1/Managers")
            .await;
        assert_origin_mismatch(error, "https://example.com/redfish/v1/Managers")?;

        let error = bmc.get_json("/not-redfish").await;
        assert_non_redfish_path(error, "/not-redfish")?;

        let error = bmc.get_json("/redfish/../not-redfish").await;
        assert_dot_segment(error, "/redfish/../not-redfish")?;

        let error = bmc.get_json("/redfish/%2e%2e/not-redfish").await;
        assert_dot_segment(error, "/redfish/%2e%2e/not-redfish")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_json_get_error_body_and_invalid_json() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let error_path = "/redfish/v1/Managers/BMC/Oem/PassthroughError";
        let invalid_json_path = "/redfish/v1/Managers/BMC/Oem/InvalidJson";

        Mock::given(method("GET"))
            .and(path(error_path))
            .respond_with(ResponseTemplate::new(400).set_body_string("redfish error body"))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(invalid_json_path))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);
        let error = bmc.get_json(error_path).await;

        let Err(BmcError::InvalidResponse { status, text, .. }) = error else {
            return Err(String::from("expected invalid response error").into());
        };

        assert_eq!(status.as_u16(), 400);
        assert_eq!(text, "redfish error body");

        let error = bmc.get_json(invalid_json_path).await;

        let Err(BmcError::DecodeError(_)) = error else {
            return Err(String::from("expected decode error").into());
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_json_post_no_auth_response_and_error() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let post_path = "/redfish/v1/Managers/BMC/Oem/Passthrough";
        let error_path = "/redfish/v1/Managers/BMC/Oem/PassthroughError";
        let request_body = json!({ "Action": "Start" });
        let response_body = json!({
            "Result": "Accepted",
            "TaskState": "Running"
        });

        Mock::given(method("POST"))
            .and(path(post_path))
            .and(body_json(&request_body))
            .and(|request: &Request| missing_header(request, "authorization"))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("ETag", "ignored-etag")
                    .set_body_json(&response_body),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path(error_path))
            .and(body_json(&request_body))
            .and(|request: &Request| missing_header(request, "authorization"))
            .respond_with(ResponseTemplate::new(400).set_body_string("redfish error body"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let credentials = BmcCredentials::username_password(String::new(), None);
        let bmc = create_test_bmc_with_credentials(&mock_server, credentials);

        let response = bmc.post_json(post_path, &request_body).await?;
        assert_eq!(response, response_body);

        let error = bmc.post_json(error_path, &request_body).await;

        let Err(BmcError::InvalidResponse { status, text, .. }) = error else {
            return Err(String::from("expected invalid response error").into());
        };

        assert_eq!(status.as_u16(), 400);
        assert_eq!(text, "redfish error body");

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_json_patch_if_match_policy_and_empty_body() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let patch_path = "/redfish/v1/Managers/BMC/Oem/Passthrough";
        let request_body = json!({ "Enabled": true });
        let response_body = json!({ "Applied": true });

        Mock::given(method("PATCH"))
            .and(path(patch_path))
            .and(body_json(&request_body))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(|request: &Request| missing_header(request, "if-match"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(patch_path))
            .and(body_json(&request_body))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("If-Match", "abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let response = bmc.patch_json(patch_path, None, &request_body).await?;
        assert_eq!(response, json!({}));

        let etag = create_odata_etag("abc123");
        let response = bmc
            .patch_json(patch_path, Some(&etag), &request_body)
            .await?;
        assert_eq!(response, response_body);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_request_4xx_error() {
        let mock_server = MockServer::start().await;
        let resource_path = "/redfish/v1/nonexistent";

        Mock::given(method("GET"))
            .and(path(resource_path))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let resource_id = create_odata_id(resource_path);
        let result = bmc.get::<TestResource>(&resource_id).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, BmcError::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn test_action_request_5xx_server_error() {
        let mock_server = MockServer::start().await;
        let action_path = "/redfish/v1/systems/1/Actions/ComputerSystem.Reset";

        let action_request = ActionRequest {
            parameter: "InvalidParameter".to_string(),
        };

        Mock::given(method("POST"))
            .and(path(action_path))
            .and(body_json(&action_request))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let action = create_test_action(action_path);
        let result = bmc.action(&action, &action_request).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, BmcError::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn test_custom_headers_in_get_request() {
        let mock_server = MockServer::start().await;
        let resource_path = paths::SYSTEMS_1;

        let test_resource =
            create_test_resource(resource_path, Some("123"), names::TEST_SYSTEM, 42);

        Mock::given(method("GET"))
            .and(path(resource_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("X-Custom-Header", "custom-value"))
            .and(header("X-Auth-Token", "test-token-12345"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&test_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert("X-Custom-Header", "custom-value".parse().unwrap());
        custom_headers.insert("X-Auth-Token", "test-token-12345".parse().unwrap());

        let bmc = create_test_bmc_with_custom_headers(&mock_server, custom_headers);

        let resource_id = create_odata_id(resource_path);
        let result = bmc.get::<TestResource>(&resource_id).await;

        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.name, names::TEST_SYSTEM);
        assert_eq!(retrieved.value, 42);
    }

    #[tokio::test]
    async fn test_custom_headers_in_post_request() {
        let mock_server = MockServer::start().await;
        let collection_path = paths::SYSTEMS_1;

        let create_request = CreateRequest {
            name: names::TEST_SYSTEM.to_string(),
            value: 999,
        };

        let created_resource =
            create_test_resource("/redfish/v1/systems/new", None, names::TEST_SYSTEM, 999);

        Mock::given(method("POST"))
            .and(path(collection_path))
            .and(body_json(&create_request))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("X-Vendor-Specific", "vendor-value"))
            .and(header("X-Request-Id", "req-123"))
            .respond_with(ResponseTemplate::new(201).set_body_json(&created_resource))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert("X-Vendor-Specific", "vendor-value".parse().unwrap());
        custom_headers.insert("X-Request-Id", "req-123".parse().unwrap());

        let bmc = create_test_bmc_with_custom_headers(&mock_server, custom_headers);

        let collection_id = create_odata_id(collection_path);
        let result = bmc
            .create::<CreateRequest, TestResource>(&collection_id, &create_request)
            .await;

        assert!(result.is_ok());
        let created = match result.unwrap() {
            ModificationResponse::Entity(created) => created,
            _ => panic!("expected entity response"),
        };
        assert_eq!(created.name, names::TEST_SYSTEM);
        assert_eq!(created.value, 999);
    }

    #[tokio::test]
    async fn test_custom_headers_in_delete_request() {
        let mock_server = MockServer::start().await;
        let resource_path = "/redfish/v1/systems/1";

        Mock::given(method("DELETE"))
            .and(path(resource_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header("X-Delete-Reason", "decommissioned"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert("X-Delete-Reason", "decommissioned".parse().unwrap());

        let bmc = create_test_bmc_with_custom_headers(&mock_server, custom_headers);

        let resource_id = create_odata_id(resource_path);
        let result = bmc.delete::<TestResource>(&resource_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_update_multipart_path_uses_location() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let upload_path = MULTIPART_UPDATE_PATH;
        let task_uri = "/redfish/v1/TaskService/Tasks/42";

        let file_path = temp_file_path("firmware");
        let file_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| "update.bin".to_string(), ToString::to_string);

        tokio::fs::write(&file_path, b"firmware-bytes").await?;

        Mock::given(method("POST"))
            .and(path(upload_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header(UPLOAD_MODE_HEADER, "stream"))
            .and(move |request: &Request| {
                multipart_body_contains(request, &file_name, "firmware-bytes")
            })
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("https://bmc.example{task_uri}"))
                    .insert_header("Retry-After", "15"),
            )
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert(UPLOAD_MODE_HEADER, http::HeaderValue::from_static("stream"));

        let bmc = create_test_bmc_with_custom_headers(&mock_server, custom_headers);
        let params = TestUpdateParameters {
            force_update: true,
            targets: vec!["/redfish/v1/Systems/1".to_string()],
        };

        let response = bmc
            .post_update_multipart_from_path(
                upload_path,
                &params,
                &file_path,
                Duration::from_secs(600),
            )
            .await;

        let _ = tokio::fs::remove_file(&file_path).await;
        let response = response?;

        assert_eq!(response.status, 202);
        assert_eq!(
            response.location.as_ref().map(ToString::to_string),
            Some(task_uri.to_string())
        );
        assert_eq!(response.retry_after_secs, Some(15));
        assert_eq!(response.task_id(), Some("42"));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_multipart_reader_uses_body_odata_id() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let upload_path = MULTIPART_UPDATE_PATH;
        let task_uri = "/redfish/v1/TaskService/Tasks/99";

        Mock::given(method("POST"))
            .and(path(upload_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(|request: &Request| {
                multipart_body_contains(request, "reader.bin", "reader-firmware")
            })
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "@odata.id": task_uri,
                "Id": "99"
            })))
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let params = TestUpdateParameters {
            force_update: false,
            targets: vec!["/redfish/v1/Managers/BMC".to_string()],
        };

        let reader = tokio_test::io::Builder::new()
            .read(b"reader-firmware")
            .build();

        let response = bmc
            .post_update_multipart_from_reader(
                upload_path,
                &params,
                "reader.bin",
                reader,
                Duration::from_secs(600),
            )
            .await?;

        assert_eq!(response.status, 200);
        assert_eq!(
            response.odata_id.as_ref().map(ToString::to_string),
            Some(task_uri.to_string())
        );
        assert_eq!(
            response.task_uri().map(ToString::to_string),
            Some(task_uri.to_string())
        );
        assert_eq!(response.task_id(), Some("99"));

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_update_path_sets_length_and_location() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let upload_path = RAW_UPDATE_PATH;
        let task_uri = "/redfish/v1/TaskService/Tasks/123";
        let file_path = temp_file_path("raw");

        tokio::fs::write(&file_path, b"raw-firmware").await?;

        Mock::given(method("PUT"))
            .and(path(upload_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(header(UPLOAD_MODE_HEADER, "raw"))
            .and(|request: &Request| raw_body_matches(request, b"raw-firmware", Some(12)))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("https://bmc.example{task_uri}")),
            )
            .mount(&mock_server)
            .await;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert(UPLOAD_MODE_HEADER, http::HeaderValue::from_static("raw"));

        let bmc = create_test_bmc_with_custom_headers(&mock_server, custom_headers);
        let response = bmc
            .put_update_file_from_path(upload_path, &file_path, Duration::from_secs(600))
            .await;

        let _ = tokio::fs::remove_file(&file_path).await;
        let response = response?;

        assert_eq!(response.status, 202);
        assert_eq!(
            response.location.as_ref().map(ToString::to_string),
            Some(task_uri.to_string())
        );
        assert_eq!(response.task_id(), Some("123"));

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_update_rejects_external_absolute_url() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let bmc = create_test_bmc(&mock_server);
        let reader = tokio_test::io::Builder::new().build();

        let error = bmc
            .put_update_file_from_reader(
                "https://example.com/redfish/v1/UpdateService/update",
                reader,
                Duration::from_secs(600),
            )
            .await;
        assert_origin_mismatch(error, "https://example.com/redfish/v1/UpdateService/update")?;

        Ok(())
    }

    fn assert_origin_mismatch<T>(
        result: Result<T, BmcError>,
        expected_uri: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let Err(BmcError::InvalidRedfishUri(RedfishUriError::OriginMismatch { uri })) = result
        else {
            return Err(String::from("expected URI validation error").into());
        };

        assert_eq!(uri, expected_uri);

        Ok(())
    }

    fn assert_non_redfish_path<T>(
        result: Result<T, BmcError>,
        expected_path: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let Err(BmcError::InvalidRedfishUri(RedfishUriError::NonRedfishPath { path })) = result
        else {
            return Err(String::from("expected URI validation error").into());
        };

        assert_eq!(path, expected_path);

        Ok(())
    }

    fn assert_dot_segment<T>(
        result: Result<T, BmcError>,
        expected_path: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let Err(BmcError::InvalidRedfishUri(RedfishUriError::DotSegment { path })) = result else {
            return Err(String::from("expected URI validation error").into());
        };

        assert_eq!(path, expected_path);

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_update_reader_uses_body_odata_id() -> Result<(), Box<dyn StdError>> {
        let mock_server = MockServer::start().await;
        let upload_path = RAW_UPDATE_PATH;
        let task_uri = "/redfish/v1/TaskService/Tasks/124";

        Mock::given(method("PUT"))
            .and(path(upload_path))
            .and(header("authorization", "Basic cm9vdDpwYXNzd29yZA=="))
            .and(|request: &Request| raw_body_matches(request, b"raw-reader", None))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "@odata.id": task_uri,
                "Id": "124"
            })))
            .mount(&mock_server)
            .await;

        let bmc = create_test_bmc(&mock_server);

        let reader = tokio_test::io::Builder::new().read(b"raw-reader").build();

        let response = bmc
            .put_update_file_from_reader(upload_path, reader, Duration::from_secs(600))
            .await?;

        assert_eq!(response.status, 200);
        assert_eq!(
            response.task_uri().map(ToString::to_string),
            Some(task_uri.to_string())
        );
        assert_eq!(response.task_id(), Some("124"));

        Ok(())
    }

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());

        std::env::temp_dir().join(format!("nv-redfish-{name}-{nanos}.bin"))
    }

    fn multipart_body_contains(request: &Request, file_name: &str, file_body: &str) -> bool {
        let Some(content_type) = request
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };

        let body = String::from_utf8_lossy(&request.body);

        content_type.starts_with("multipart/form-data; boundary=")
            && body.contains("name=\"UpdateParameters\"")
            && body.contains("\"ForceUpdate\":")
            && body.contains("\"Targets\":")
            && body.contains("name=\"UpdateFile\"")
            && body.contains(&format!("filename=\"{file_name}\""))
            && body.contains(file_body)
    }

    fn raw_body_matches(
        request: &Request,
        expected_body: &[u8],
        content_length: Option<u64>,
    ) -> bool {
        let content_type_matches = request
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == OCTET_STREAM_MIME);

        let content_length_matches = content_length.is_none_or(|content_length| {
            request
                .headers
                .get("content-length")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == content_length.to_string())
        });

        content_type_matches && content_length_matches && request.body == expected_body
    }

    fn missing_header(request: &Request, name: &str) -> bool {
        !request.headers.contains_key(name)
    }
}
