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

use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};

use quickwit_common::Byte;

use crate::OwnedBytes;

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct SliceAddress {
    pub path: PathBuf,
    pub byte_range: Range<usize>,
}

/// A simple in-resident memory slice cache.
pub struct SliceCache {
    cache: moka::sync::Cache<SliceAddress, OwnedBytes>,
    capacity: Option<Byte>,
}

impl SliceCache {
    /// Creates an slice cache with the given capacity.
    pub fn with_capacity(capacity: Byte) -> Self {
        let cache = moka::sync::Cache::builder()
            .max_capacity(capacity.get_bytes())
            .weigher(|_key, payload: &OwnedBytes| payload.len() as u32)
            .build();
        SliceCache { cache, capacity: Some(capacity) }
    }

    /// Creates a slice cache that nevers removes any entry.
    pub fn with_infinite_capacity() -> Self {
        let cache = moka::sync::Cache::builder()
            .weigher(|_key, payload: &OwnedBytes| payload.len() as u32)
            .build();
        SliceCache { cache, capacity: None }
    }

    pub fn size_in_cache(&self) -> Byte {
        use moka::sync::ConcurrentCacheExt;
        self.cache.sync();
        Byte::from_bytes(self.cache.weighted_size())
    }

    /// If available, returns the cached view of the slice.
    pub fn get(&self, path: &Path, bytes_range: Range<usize>) -> Option<OwnedBytes> {
        let slice_addr = SliceAddress {
            path: path.to_path_buf(),
            byte_range: bytes_range,
        };
        self.cache.get(&slice_addr)
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    pub fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        // use moka::sync::ConcurrentCacheExt;
        if let Some(capacity) = self.capacity {
            if byte_range.len() as u64 > capacity.get_bytes() {
                return;
            }
        }
        let slice_addr = SliceAddress { path, byte_range };
        self.cache.insert(slice_addr, bytes);
        // self.cache.sync();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_cache_edge_condition() {
        let cache = SliceCache::with_capacity(Byte::from_bytes(5));
        {
            let data = OwnedBytes::new(&b"abc"[..]);
            cache.put(PathBuf::from("3"), 0..3, data);
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
        }
        {
            let data = OwnedBytes::new(&b"de"[..]);
            cache.put(PathBuf::from("2"), 0..2, data);
            // our first entry should still be here.
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(Path::new("2"), 0..2).unwrap(), &b"de"[..]);
        }
        assert_eq!(cache.size_in_cache().get_bytes(), 5);

        {
            let data = OwnedBytes::new(&b"fghij"[..]);
            assert_eq!(cache.size_in_cache().get_bytes(), 5);
            cache.put(PathBuf::from("5"), 0..5, data);
            // cache.debug();
            assert_eq!(cache.get(Path::new("5"), 0..5).unwrap(), &b"fghij"[..]);
            // our two first entries should have be removed from the cache
            assert_eq!(cache.size_in_cache().get_bytes(), 5);
            assert!(cache.get(Path::new("2"), 0..2).is_none());
            assert!(cache.get(Path::new("3"), 0..3).is_none());
        }
        {
            let data = OwnedBytes::new(&b"klmnop"[..]);
            cache.put(PathBuf::from("6"), 0..6, data);
            // The entry put should have been dismissed as it is too large for the cache
            assert!(cache.get(Path::new("6"), 0..6).is_none());
            // The previous entry should however be remaining.
            assert_eq!(cache.get(Path::new("5"), 0..5).unwrap(), &b"fghij"[..]);
        }
    }

    #[test]
    fn test_cache_edge_unlimited_capacity() {
        let cache = SliceCache::with_infinite_capacity();
        {
            let data = OwnedBytes::new(&b"abc"[..]);
            cache.put(PathBuf::from("3"), 0..3, data);
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
        }
        {
            let data = OwnedBytes::new(&b"de"[..]);
            cache.put(PathBuf::from("2"), 0..2, data);
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(Path::new("2"), 0..2).unwrap(), &b"de"[..]);
        }
    }

    #[test]
    fn test_cache() {
        let cache = SliceCache::with_capacity(Byte::from_bytes(10_000));
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        cache.put(PathBuf::from("hello.seg"), 1..3, data);
        assert_eq!(
            cache.get(Path::new("hello.seg"), 1..3).unwrap(),
            &b"werwer"[..]
        );
    }

    #[test]
    fn test_cache_different_slice() {
        let cache = SliceCache::with_capacity(Byte::from_bytes(10_000));
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        // We could actually have a cache hit here, but this is not useful for Quickwit.
        cache.put(PathBuf::from("hello.seg"), 1..3, data);
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_some());
        assert!(cache.get(Path::new("hello.seg"), 2..3).is_none());
    }
}
