use std::net::SocketAddr;

use url::Url;

use crate::engine::local::{Db, TiKv};
use crate::opt::endpoint::into_endpoint;
use crate::opt::{Config, Endpoint, IntoEndpoint};
use crate::{Error, Result};

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<TiKv> for $name {}
			impl into_endpoint::Sealed<TiKv> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("tikv://{self}");
					let mut endpoint = Endpoint::new(Url::parse(&url).map_err(|_| Error::internal(format!("Invalid URL: {}", url)))?);
					endpoint.path = url;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<TiKv> for ($name, Config) {}
			impl into_endpoint::Sealed<TiKv> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<TiKv>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn typed_endpoints_preserve_the_pd_address() {
		let addr: SocketAddr = "127.0.0.1:2379".parse().unwrap();
		let endpoints = [
			into_endpoint::Sealed::<TiKv>::into_endpoint("127.0.0.1:2379").unwrap(),
			into_endpoint::Sealed::<TiKv>::into_endpoint(addr).unwrap(),
			into_endpoint::Sealed::<TiKv>::into_endpoint((addr, Config::default())).unwrap(),
		];
		for endpoint in endpoints {
			assert_eq!(endpoint.path, "tikv://127.0.0.1:2379");
			assert_eq!(endpoint.url.scheme(), "tikv");
			assert_eq!(endpoint.url.port(), Some(2379));
		}
	}
}
