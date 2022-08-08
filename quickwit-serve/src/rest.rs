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

use std::net::SocketAddr;

use hyper::http;
use quickwit_common::metrics;
use tracing::{error, info};
use warp::{redirect, Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::error::ServiceErrorCode;
use crate::format::FormatError;
use crate::health_check_api::liveness_check_handler;
use crate::indexing_api::indexing_get_handler;
use crate::ingest_api::{elastic_bulk_handler, ingest_handler, tail_handler};
use crate::metastore_api::metastore_api_handlers;
use crate::node_info_handler::node_info_handler;
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::ui_handler::ui_handler;
use crate::{Format, QuickwitServices};

/// Starts REST service given a HTTP address and a search service.
pub(crate) async fn start_rest_server(
    rest_listen_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(rest_listen_addr = %rest_listen_addr, "Starting REST server.");
    let request_counter = warp::log::custom(|_| {
        crate::SERVE_METRICS.http_requests_total.inc();
    });
    let metrics_service = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    let api_v1_routes = cluster_handler(quickwit_services.cluster.clone())
        .or(node_info_handler(
            quickwit_services.build_info.clone(),
            quickwit_services.config.clone(),
        ))
        .or(indexing_get_handler(
            quickwit_services.indexer_service.clone(),
        ))
        .or(search_get_handler(quickwit_services.search_service.clone()))
        .or(search_post_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(search_stream_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(ingest_handler(quickwit_services.ingest_api_service.clone()))
        .or(tail_handler(quickwit_services.ingest_api_service.clone()))
        .or(elastic_bulk_handler(
            quickwit_services.ingest_api_service.clone(),
        ))
        .or(metastore_api_handlers(
            quickwit_services.metastore_service_local.clone(),
        ));
    let api_v1_root_route = api_v1_root_url.and(api_v1_routes);
    let redirect_root_to_ui_route =
        warp::path::end().map(|| redirect(http::Uri::from_static("/ui/search")));
    let rest_routes = api_v1_root_route
        .or(redirect_root_to_ui_route)
        .or(ui_handler())
        .or(liveness_check_handler())
        .or(metrics_service)
        .with(request_counter)
        .recover(recover_fn);

    info!("Searcher ready to accept requests at http://{rest_listen_addr}/");
    warp::serve(rest_routes).run(rest_listen_addr).await;
    Ok(())
}

/// This function returns a formatted error based on the given rejection reason.
/// The ordering of rejection processing is very important, we need to start
/// with the most specific rejections and end with the most generic. If not, Quickwit
/// will return useless errors to the user.
// TODO: we may want in the future revamp rejections as our usage does not exactly
// match rejection behaviour. When a filter returns a rejection, it means that it
// did not match, but maybe another filter can. Consequently warp will continue
// to try to match other filters. Once a filter is matched, we can enter into
// our own logic and return a proper reply.
// More on this here: https://github.com/seanmonstar/warp/issues/388.
// We may use this work on the PR is merged: https://github.com/seanmonstar/warp/pull/909.
pub async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    let err = get_status_with_error(rejection);
    Ok(Format::PrettyJson.make_reply_for_err(err))
}

fn get_status_with_error(rejection: Rejection) -> FormatError {
    if rejection.is_not_found() {
        FormatError {
            code: ServiceErrorCode::NotFound,
            error: "Route not found".to_string(),
        }
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::UnsupportedMediaType>() {
        FormatError {
            code: ServiceErrorCode::UnsupportedMediaType,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidQuery>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::LengthRequired>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MissingHeader>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidHeader>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MethodNotAllowed>() {
        FormatError {
            code: ServiceErrorCode::MethodNotAllowed,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::PayloadTooLarge>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<crate::ingest_api::BulkApiError>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else {
        error!("REST server error: {:?}", rejection);
        FormatError {
            code: ServiceErrorCode::Internal,
            error: "Internal server error.".to_string(),
        }
    }
}
