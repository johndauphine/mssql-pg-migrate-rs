//! Unified TLS configuration for PostgreSQL connections.
//!
//! This module consolidates TLS setup previously duplicated across source and target
//! PostgreSQL implementations.

use std::sync::Arc;

use rustls::ClientConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{info, warn};

use crate::error::{MigrateError, Result};

/// SSL verification modes for PostgreSQL connections.
///
/// These modes match PostgreSQL's standard `sslmode` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// No SSL/TLS (plain TCP connection).
    #[default]
    Disable,
    /// Use SSL but don't verify server certificate.
    /// **Security Warning**: Vulnerable to man-in-the-middle attacks.
    Require,
    /// Verify server certificate against CA but not hostname.
    VerifyCa,
    /// Full certificate and hostname verification.
    VerifyFull,
}

impl SslMode {
    /// Parse an SSL mode from a string.
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "disable" | "" => Ok(SslMode::Disable),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            other => Err(MigrateError::Config(format!(
                "Invalid ssl_mode '{}'. Valid values: disable, require, verify-ca, verify-full",
                other
            ))),
        }
    }

    /// Check if this mode requires TLS.
    pub fn requires_tls(&self) -> bool {
        !matches!(self, SslMode::Disable)
    }
}

/// Builder for PostgreSQL TLS connections.
///
/// Centralizes TLS configuration to avoid duplication between source and target
/// PostgreSQL pools.
pub struct TlsBuilder {
    ssl_mode: SslMode,
}

impl TlsBuilder {
    /// Create a new TLS builder with the given SSL mode.
    pub fn new(ssl_mode: SslMode) -> Self {
        Self { ssl_mode }
    }

    /// Create a TLS builder from an ssl_mode string.
    pub fn from_str(ssl_mode: &str) -> Result<Self> {
        Ok(Self::new(SslMode::from_str(ssl_mode)?))
    }

    /// Build a MakeRustlsConnect instance for use with deadpool-postgres.
    ///
    /// Returns None if TLS is disabled.
    pub fn build(&self) -> Result<Option<MakeRustlsConnect>> {
        if !self.ssl_mode.requires_tls() {
            return Ok(None);
        }

        let config = self.build_client_config()?;
        Ok(Some(MakeRustlsConnect::new(config)))
    }

    /// Build the underlying rustls ClientConfig.
    pub fn build_client_config(&self) -> Result<ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = match self.ssl_mode {
            SslMode::Disable => {
                // Shouldn't be called for disable mode, but return a sensible default
                return Err(MigrateError::Config(
                    "Cannot build TLS config for ssl_mode=disable".into(),
                ));
            }
            SslMode::Require => {
                // TLS enabled but no certificate verification
                // SECURITY WARNING: Vulnerable to MITM attacks
                warn!(
                    "ssl_mode=require: TLS enabled but server certificate is not verified. \
                     Consider using 'verify-full' for production."
                );
                ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth()
            }
            SslMode::VerifyCa => {
                // Certificate verification without hostname check
                // Note: Our implementation does check hostname due to rustls defaults
                warn!(
                    "ssl_mode=verify-ca: certificate and hostname verification enabled \
                     (same behavior as verify-full in this implementation)"
                );
                ClientConfig::builder()
                    .with_root_certificates(root_store.clone())
                    .with_no_client_auth()
            }
            SslMode::VerifyFull => {
                // Full certificate and hostname verification
                info!("ssl_mode=verify-full: full certificate and hostname verification enabled");
                ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            }
        };

        Ok(config)
    }
}

/// Custom certificate verifier that accepts any certificate.
///
/// **SECURITY WARNING**: This bypasses all certificate validation.
/// Only use for `ssl_mode=require` where TLS encryption is desired
/// but certificate validation is not required.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_mode_parsing() {
        assert_eq!(SslMode::from_str("disable").unwrap(), SslMode::Disable);
        assert_eq!(SslMode::from_str("require").unwrap(), SslMode::Require);
        assert_eq!(SslMode::from_str("verify-ca").unwrap(), SslMode::VerifyCa);
        assert_eq!(SslMode::from_str("verify-full").unwrap(), SslMode::VerifyFull);
        assert_eq!(SslMode::from_str("").unwrap(), SslMode::Disable);
        assert!(SslMode::from_str("invalid").is_err());
    }

    #[test]
    fn test_ssl_mode_requires_tls() {
        assert!(!SslMode::Disable.requires_tls());
        assert!(SslMode::Require.requires_tls());
        assert!(SslMode::VerifyCa.requires_tls());
        assert!(SslMode::VerifyFull.requires_tls());
    }

    #[test]
    fn test_tls_builder_disable_returns_none() {
        let builder = TlsBuilder::new(SslMode::Disable);
        assert!(builder.build().unwrap().is_none());
    }

    #[test]
    fn test_tls_builder_require_returns_some() {
        let builder = TlsBuilder::new(SslMode::Require);
        assert!(builder.build().unwrap().is_some());
    }

    #[test]
    fn test_tls_builder_verify_full_returns_some() {
        let builder = TlsBuilder::new(SslMode::VerifyFull);
        assert!(builder.build().unwrap().is_some());
    }
}
