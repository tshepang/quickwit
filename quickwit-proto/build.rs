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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/search_api.proto");
    println!("cargo:rerun-if-changed=proto/ingest_api.proto");
    println!("cargo:rerun-if-changed=proto/index_management_api.proto");

    let mut prost_config = prost_build::Config::default();
    // prost_config.type_attribute("LeafSearchResponse", "#[derive(Default)]");
    prost_config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure()
        .type_attribute(".", "#[derive(Serialize, Deserialize)]")
        .type_attribute("OutputFormat", "#[serde(rename_all = \"snake_case\")]")
        .out_dir("src/")
        .compile_with_config(
            prost_config,
            &[
                "./proto/search_api.proto",
                "./proto/ingest_api.proto",
                "./proto/metastore_api.proto",
            ],
            &["./proto"],
        )?;
    Ok(())
}
