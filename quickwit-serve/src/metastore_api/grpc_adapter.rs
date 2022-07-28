// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use async_trait::async_trait;
use quickwit_control_plane::MetastoreService;
use quickwit_proto::metastore_api::metastore_api_service_server::{self as grpc};
use quickwit_proto::metastore_api::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest,
    DeleteIndexResponse, DeleteSourceRequest, DeleteSplitsRequest, IndexMetadataRequest,
    IndexMetadataResponse, ListAllSplitsRequest, ListIndexesMetadatasRequest,
    ListIndexesMetadatasResponse, ListSplitsRequest, ListSplitsResponse,
    MarkSplitsForDeletionRequest, PublishSplitsRequest, SourceResponse, SplitResponse,
    StageSplitRequest,
};
use quickwit_proto::tonic;

#[derive(Clone)]
pub struct GrpcMetastoreServiceAdapter(MetastoreService);

impl From<MetastoreService> for GrpcMetastoreServiceAdapter {
    fn from(metastore_service: MetastoreService) -> Self {
        assert!(
            metastore_service.is_local(),
            "The gRPC adapter must use a local metastore service."
        );
        Self(metastore_service)
    }
}

#[async_trait]
impl grpc::MetastoreApiService for GrpcMetastoreServiceAdapter {
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        self.0
            .clone()
            .create_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        self.0
            .clone()
            .index_metadata(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn list_indexes_metadatas(
        &self,
        request: tonic::Request<ListIndexesMetadatasRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadatasResponse>, tonic::Status> {
        self.0
            .clone()
            .list_indexes_metadatas(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<DeleteIndexResponse>, tonic::Status> {
        self.0
            .clone()
            .delete_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn list_all_splits(
        &self,
        request: tonic::Request<ListAllSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.0
            .clone()
            .list_all_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.0
            .clone()
            .list_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn stage_split(
        &self,
        request: tonic::Request<StageSplitRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        self.0
            .clone()
            .stage_split(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        self.0
            .clone()
            .publish_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        self.0
            .clone()
            .mark_splits_for_deletion(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn delete_splits(
        &self,
        request: tonic::Request<DeleteSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        self.0
            .clone()
            .delete_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn add_source(
        &self,
        request: tonic::Request<AddSourceRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        self.0
            .clone()
            .add_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }

    async fn delete_source(
        &self,
        request: tonic::Request<DeleteSourceRequest>,
    ) -> Result<tonic::Response<SourceResponse>, tonic::Status> {
        self.0
            .clone()
            .delete_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(convert_error)
    }
}

// TODO: process errors correctly.
pub(crate) fn convert_error<E: ToString>(error: E) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}
