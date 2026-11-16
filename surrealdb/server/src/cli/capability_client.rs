//! Matching native quota capability preflight for remote CLI operations.

use anyhow::{Context, Result, bail};
use reqwest::{Client, Url};

use crate::capability::CapabilityDocument;

pub(crate) async fn require_matching_remote(endpoint: &str) -> Result<CapabilityDocument> {
	let document = fetch_capabilities(endpoint).await?;
	document.require_matching_cli()?;
	Ok(document)
}

pub(crate) async fn require_remote_ready(
	endpoint: &str,
	required: &[String],
) -> Result<CapabilityDocument> {
	let document = fetch_capabilities(endpoint).await?;
	document.require(required)?;
	let mut url = endpoint_url(endpoint, "/ready")?;
	if !required.is_empty() {
		url.query_pairs_mut().append_pair("require", &required.join(","));
	}
	let response = Client::new().get(url).send().await?;
	if !response.status().is_success() {
		bail!("remote server does not satisfy required native quota readiness");
	}
	Ok(document)
}

async fn fetch_capabilities(endpoint: &str) -> Result<CapabilityDocument> {
	let response = Client::new().get(endpoint_url(endpoint, "/capabilities")?).send().await?;
	if !response.status().is_success() {
		bail!(
			"remote server does not expose a compatible native quota capability document ({})",
			response.status()
		);
	}
	let document = response
		.json::<CapabilityDocument>()
		.await
		.context("remote native quota capability document is malformed")?;
	Ok(document)
}

fn endpoint_url(endpoint: &str, path: &str) -> Result<Url> {
	let mut parsed = Url::parse(endpoint).context("remote endpoint is not a valid URL")?;
	// The server multiplexes its transports on one listener, so the public
	// HTTP routes the preflight needs sit behind the websocket and gRPC
	// endpoint schemes as well.
	let replacement = match parsed.scheme() {
		"ws" | "grpc" => Some("http"),
		"wss" | "grpcs" => Some("https"),
		"http" | "https" => None,
		scheme => bail!("endpoint scheme '{scheme}' does not support remote capability preflight"),
	};
	let mut url = match replacement {
		// `Url::set_scheme` refuses to move a URL between a non-special
		// scheme (`grpc`, `grpcs`) and a special one (`http`, `https`), so
		// the scheme is swapped textually, as the SDK's gRPC connector does.
		Some(scheme) if parsed.scheme() != "ws" && parsed.scheme() != "wss" => {
			let rest = endpoint
				.split_once("://")
				.map(|(_, rest)| rest)
				.context("remote endpoint is not a valid URL")?;
			Url::parse(&format!("{scheme}://{rest}"))
				.context("remote endpoint is not a valid URL")?
		}
		Some(scheme) => {
			parsed
				.set_scheme(scheme)
				.map_err(|_| anyhow::anyhow!("unable to map websocket endpoint to HTTP"))?;
			parsed
		}
		None => parsed,
	};
	url.set_path(path);
	url.set_query(None);
	url.set_fragment(None);
	Ok(url)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn websocket_endpoint_maps_to_public_capability_route() {
		assert_eq!(
			endpoint_url("ws://localhost:8000/rpc", "/capabilities").unwrap().as_str(),
			"http://localhost:8000/capabilities"
		);
		assert_eq!(
			endpoint_url("wss://db.example.com/rpc?x=1", "/ready").unwrap().as_str(),
			"https://db.example.com/ready"
		);
		assert_eq!(
			endpoint_url("grpc://localhost:8000", "/capabilities").unwrap().as_str(),
			"http://localhost:8000/capabilities"
		);
		assert_eq!(
			endpoint_url("grpcs://db.example.com", "/ready").unwrap().as_str(),
			"https://db.example.com/ready"
		);
	}

	#[test]
	fn local_and_unknown_schemes_fail_closed() {
		assert!(endpoint_url("memory", "/capabilities").is_err());
		assert!(endpoint_url("surrealkv://data", "/capabilities").is_err());
	}
}
