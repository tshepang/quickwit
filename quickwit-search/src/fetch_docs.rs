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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use futures::stream::{StreamExt, TryStreamExt};
use itertools::Itertools;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::{FetchDocsResponse, PartialHit, SearchRequest, SplitIdAndFooterOffsets};
use quickwit_storage::Storage;
use tantivy::query::QueryParserError;
use tantivy::schema::Value;
use tantivy::{Document, IndexReader, ReloadPolicy, Searcher, SnippetGenerator};
use tracing::error;

use crate::leaf::open_index_with_cache;
use crate::GlobalDocAddress;

/// Given a list of global doc address, fetches all the documents and
/// returns them as a hashmap.
#[allow(clippy::needless_lifetimes)]
async fn fetch_docs_to_map(
    mut global_doc_addrs: Vec<GlobalDocAddress>,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<dyn DocMapper>,
    search_request: &SearchRequest,
) -> anyhow::Result<HashMap<GlobalDocAddress, String>> {
    let mut split_fetch_docs_futures = Vec::new();

    let split_offsets_map: HashMap<&str, &SplitIdAndFooterOffsets> = splits
        .iter()
        .map(|split| (split.split_id.as_str(), split))
        .collect();

    // We sort global hit addrs in order to allow for the grouby.
    global_doc_addrs.sort_by(|a, b| a.split.cmp(&b.split));
    for (split_id, global_doc_addrs) in global_doc_addrs
        .iter()
        .group_by(|global_doc_addr| global_doc_addr.split.as_str())
        .into_iter()
    {
        let global_doc_addrs: Vec<GlobalDocAddress> =
            global_doc_addrs.into_iter().cloned().collect();
        let split_and_offset = split_offsets_map
            .get(split_id)
            .ok_or_else(|| anyhow::anyhow!("Failed to find offset for split {}", split_id))?;
        split_fetch_docs_futures.push(fetch_docs_in_split(
            global_doc_addrs,
            index_storage.clone(),
            *split_and_offset,
            doc_mapper.clone(),
            search_request,
        ));
    }

    let split_fetch_docs: Vec<Vec<(GlobalDocAddress, String)>> = futures::future::try_join_all(
        split_fetch_docs_futures,
    )
    .await
    .map_err(|error| {
        let split_ids = splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec();
        error!(split_ids = ?split_ids, error = ?error, "Error when fetching docs in splits.");
        anyhow::anyhow!(
            "Error when fetching docs for splits {:?}: {:?}.",
            split_ids,
            error
        )
    })?;

    let global_doc_addr_to_doc_json: HashMap<GlobalDocAddress, String> = split_fetch_docs
        .into_iter()
        .flat_map(|docs| docs.into_iter())
        .collect();

    Ok(global_doc_addr_to_doc_json)
}

/// `fetch_docs` step of search.
///
/// This function takes a list of partial hits (possibly from different splits)
/// and the storage associated to an index, fetches the document from
/// the split document stores, and returns the full hits.
pub async fn fetch_docs(
    partial_hits: Vec<PartialHit>,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<dyn DocMapper>,
    search_request: &SearchRequest,
) -> anyhow::Result<FetchDocsResponse> {
    let global_doc_addrs: Vec<GlobalDocAddress> = partial_hits
        .iter()
        .map(GlobalDocAddress::from_partial_hit)
        .collect();

    let mut global_doc_addr_to_doc_json = fetch_docs_to_map(
        global_doc_addrs,
        index_storage,
        splits,
        doc_mapper,
        search_request,
    )
    .await?;

    let hits: Vec<quickwit_proto::LeafHit> = partial_hits
        .iter()
        .flat_map(|partial_hit| {
            let global_doc_addr = GlobalDocAddress::from_partial_hit(partial_hit);
            if let Some((_, leaf_json)) = global_doc_addr_to_doc_json.remove_entry(&global_doc_addr)
            {
                Some(quickwit_proto::LeafHit {
                    leaf_json,
                    partial_hit: Some(partial_hit.clone()),
                })
            } else {
                None
            }
        })
        .collect();
    Ok(FetchDocsResponse { hits })
}

const NUM_CONCURRENT_REQUESTS: usize = 10;

async fn get_searcher_for_split_without_cache(
    index_storage: Arc<dyn Storage>,
    split: &SplitIdAndFooterOffsets,
) -> anyhow::Result<IndexReader> {
    let index = open_index_with_cache(index_storage, split, false)
        .await
        .with_context(|| "open-index-for-split")?;
    let reader = index
        .reader_builder()
        // the docs are presorted so a cache size of NUM_CONCURRENT_REQUESTS is fine
        .doc_store_cache_size(NUM_CONCURRENT_REQUESTS)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    Ok(reader)
}

/// Fetching docs from a specific split.
#[tracing::instrument(skip(global_doc_addrs, index_storage, split))]
#[allow(clippy::needless_lifetimes)]
async fn fetch_docs_in_split(
    mut global_doc_addrs: Vec<GlobalDocAddress>,
    index_storage: Arc<dyn Storage>,
    split: &SplitIdAndFooterOffsets,
    doc_mapper: Arc<dyn DocMapper>,
    search_request: &SearchRequest,
) -> anyhow::Result<Vec<(GlobalDocAddress, String)>> {
    global_doc_addrs.sort_by_key(|doc| doc.doc_addr);

    let index_reader = get_searcher_for_split_without_cache(index_storage, split).await?;
    let searcher = index_reader.searcher();
    let snippet_generators = Arc::new(create_snippet_generators(
        &searcher,
        doc_mapper,
        search_request,
    )?);

    let doc_futures = global_doc_addrs.into_iter().map(|global_doc_addr| {
        let searcher = searcher.clone();
        let moved_snippet_generators = snippet_generators.clone();
        async move {
            let doc = searcher
                .doc_async(global_doc_addr.doc_addr)
                .await
                .context("searcher-doc-async")?;
            if moved_snippet_generators.is_empty() {
                let doc_json = searcher.schema().to_json(&doc);
                return Ok((global_doc_addr, doc_json));
            }

            let mut doc_with_snippet = Document::new();
            for (field, field_values) in doc.get_sorted_field_values() {
                let field_name = searcher.schema().get_field_name(field);
                // TODO:  Extract snippets & serialize
                let values: Vec<Value> =
                    if let Some(snippet_generator) = moved_snippet_generators.get(field_name) {
                        field_values
                            .into_iter()
                            .map(|value| {
                                let snippet = snippet_generator
                                    .snippet(value.as_text().expect("must be a bug"));
                                Value::Str(snippet.to_html())
                            })
                            .collect()
                    } else {
                        field_values.into_iter().cloned().collect()
                    };
                for typed_val in values {
                    doc_with_snippet.add_field_value(field, typed_val)
                }
            }
            let doc_json = searcher.schema().to_json(&doc_with_snippet);
            Ok((global_doc_addr, doc_json))
        }
    });

    let stream = futures::stream::iter(doc_futures).buffer_unordered(NUM_CONCURRENT_REQUESTS);
    stream.try_collect::<Vec<_>>().await
}

pub fn create_snippet_generators(
    searcher: &Searcher,
    doc_mapper: Arc<dyn DocMapper>,
    search_request: &SearchRequest,
) -> anyhow::Result<HashMap<String, SnippetGenerator>> {
    let schema = searcher.schema();
    let query = doc_mapper.query(schema.clone(), search_request)?;

    let mut snippet_generators = HashMap::new();
    for field_name in &search_request.snippet_fields {
        let field = schema
            .get_field(field_name)
            .ok_or_else(|| QueryParserError::FieldDoesNotExist(field_name.clone()))?;
        let snippet_generator = SnippetGenerator::create(searcher, &*query, field)?;
        snippet_generators.insert(field_name.clone(), snippet_generator);
    }
    Ok(snippet_generators)
}
