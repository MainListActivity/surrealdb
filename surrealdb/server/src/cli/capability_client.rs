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
	let mut url = Url::parse(endpoint).context("remote endpoint is not a valid URL")?;
	let replacement = match url.scheme() {
		"ws" => Some("http"),
		"wss" => Some("https"),
		"http" | "https" => None,
		scheme => bail!("endpoint scheme '{scheme}' does not support remote capability preflight"),
	};
	if let Some(replacement) = replacement {
		url.set_scheme(replacement)
			.map_err(|_| anyhow::anyhow!("unable to map websocket endpoint to HTTP"))?;
	}
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
	}

	#[test]
	fn local_and_unknown_schemes_fail_closed() {
		assert!(endpoint_url("memory", "/capabilities").is_err());
		assert!(endpoint_url("surrealkv://data", "/capabilities").is_err());
	}
}
