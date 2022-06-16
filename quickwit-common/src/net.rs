// Copyright (C) 2021 Quickwit, Inc.
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

use std::fmt::Display;
use std::net::{IpAddr, SocketAddr, TcpListener};

use anyhow::Context;
use tokio::net::{lookup_host, ToSocketAddrs};

/// Represents a host, i.e. an IP address (`127.0.0.1`) or a hostname (`localhost`).
#[derive(Clone, Debug)]
pub enum Host {
    Hostname(String),
    IpAddr(IpAddr),
}

impl Host {
    /// Returns a resolved host, i.e. an IP address.
    pub async fn resolve(&self) -> anyhow::Result<IpAddr> {
        match self {
            Host::IpAddr(ip_addr) => Ok(ip_addr.clone()),
            Host::Hostname(hostname) => lookup_host(hostname.as_str())
                .await
                .with_context(|| format!("Failed to resolve hostname `{}`.", hostname))?
                .next()
                .map(|socket_addr| socket_addr.ip())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "DNS resolution did not yield any record for hostname `{}`.",
                        hostname
                    )
                }),
        }
    }
}

impl Display for Host {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Host::Hostname(hostname) => hostname.fmt(formatter),
            Host::IpAddr(ip_addr) => ip_addr.fmt(formatter),
        }
    }
}

/// Represents an address `<host>:<port>` where `host` can be an IP address or a hostname.
#[derive(Clone, Debug)]
pub struct HostAddr {
    host: Host,
    port: u16,
}

impl HostAddr {
    /// Attempts to parse a `host_addr`.
    /// If no port is defined, it just accepts the host and uses the given default port.
    ///
    /// This function supports:
    /// - IPv4
    /// - IPv4:port
    /// - IPv6
    /// - \[IPv6\]:port -- IpV6 contains colon. It is customary to require bracket for this reason.
    /// - hostname
    /// - hostname:port
    pub fn parse_with_default_port(host_addr: &str, default_port: u16) -> anyhow::Result<Self> {
        if let Ok(socket_addr) = host_addr.parse::<SocketAddr>() {
            return Ok(Self {
                host: Host::IpAddr(socket_addr.ip()),
                port: socket_addr.port(),
            });
        }
        if let Ok(ip_addr) = host_addr.parse::<IpAddr>() {
            return Ok(Self {
                host: Host::IpAddr(ip_addr),
                port: default_port,
            });
        }
        if let Some((hostname_str, port_str)) = host_addr.split_once(':') {
            let port = port_str
                .parse::<u16>()
                .with_context(|| format!("Failed to parse host address: `{}`.", host_addr))?;
            return Ok(Self {
                host: Host::Hostname(hostname_str.to_string()),
                port,
            });
        }
        Ok(Self {
            host: Host::Hostname(host_addr.to_string()),
            port: default_port,
        })
    }

    pub async fn to_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        self.host
            .resolve()
            .await
            .map(|ip_addr| SocketAddr::new(ip_addr, self.port))
    }
}

impl Display for HostAddr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.host, self.port)
    }
}

impl PartialEq<&str> for HostAddr {
    fn eq(&self, other: &&str) -> bool {
        self.to_string() == other
    }
}

impl PartialEq<String> for HostAddr {
    fn eq(&self, other: &String) -> bool {
        self.to_string() == other
    }
}

impl Serialize for HostAddr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

/// Finds a random available TCP port.
pub fn find_available_tcp_port() -> anyhow::Result<u16> {
    let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Converts an object into a resolved `SocketAddr`.
pub async fn get_socket_addr<T: ToSocketAddrs + std::fmt::Debug>(
    addr: &T,
) -> anyhow::Result<SocketAddr> {
    lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("Failed to resolve address `{:?}`.", addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_addr() {
        HostAddr::parse_with_default_port("127.0..1", 1337).unwrap_err();
        assert_eq!(
            HostAddr::parse_with_default_port("127.0.0.1", 1337).unwrap(),
            "127.0.0.1:1337"
        );
        assert_eq!(
            HostAddr::parse_with_default_port("127.0.0.1:100", 1337).unwrap(),
            "127.0.0.1:100"
        );
        test_parse_host_addr_helper("127.0.0.1", Some("127.0.0.1:1337"));
        test_parse_host_addr_helper("127.0.0.1:100", Some("127.0.0.1:100"));
        test_parse_host_addr_helper("127.0..1:100", None);
        test_parse_host_addr_helper(
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            Some("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1337"),
        );
        test_parse_host_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1000", None);
        test_parse_host_addr_helper(
            "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000",
            Some("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000"),
        );
        test_parse_host_addr_helper("[2001:0db8:1000", None);
        test_parse_host_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000", None);

        test_parse_host_addr_helper("google.com", Some("google.com:1337"));
        test_parse_host_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000", None);
    }
}
