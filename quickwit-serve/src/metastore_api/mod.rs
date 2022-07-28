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

mod grpc_adapter;
mod rest_handler;

pub use grpc_adapter::GrpcMetastoreServiceAdapter;

pub use self::rest_handler::metastore_api_handlers;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_control_plane::MetastoreService;
    use quickwit_metastore::{IndexMetadata, Metastore, MockMetastore};
    use quickwit_proto::metastore_api::metastore_api_service_server::MetastoreApiServiceServer;
    use quickwit_proto::metastore_api::IndexMetadataRequest;
    use quickwit_proto::tonic::transport::Server;

    use super::GrpcMetastoreServiceAdapter;

    // Creates an [`MetastoreService`] and use a gRPC server with the adapter so
    // that it sends requests to the [`MetastoreService`].
    async fn create_metastore_service_client(
        mock_metastore: Arc<dyn Metastore>,
    ) -> anyhow::Result<MetastoreService> {
        let (client, server) = tokio::io::duplex(1024);
        let metastore_service_local = MetastoreService::from_metastore(mock_metastore);
        let grpc_adapter = GrpcMetastoreServiceAdapter::from(metastore_service_local);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adapter))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        let metastore_service_client = MetastoreService::from_duplex_stream(client).await?;
        Ok(metastore_service_client)
    }

    #[tokio::test]
    async fn test_grpc_metastore_service_with_fake_server() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadata::for_test(
                "test-index",
                "ram:///indexes/test-index",
            ))
        });
        let mut service_client = create_metastore_service_client(Arc::new(mock_metastore)).await?;
        let response = service_client
            .index_metadata(IndexMetadataRequest {
                index_id: "my-index".to_string(),
            })
            .await;
        assert!(response.is_ok());
        // TODO: complete with test on all metastore service calls.
        Ok(())
    }
}
