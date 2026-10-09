//! A throwaway PKI for tests and local deployments.
//!
//! This generates a self-signed certificate authority and issues leaf
//! certificates from it, so a cluster can be brought up with file-backed
//! mutual TLS without any external tooling. The material is written as PEM
//! files which are consumed by the gRPC TLS stack via
//! `--grpc-tls-*-file` arguments.
//!
//! Issued certificates carry both the server and client authentication
//! extended key usages so the same identity can serve a gRPC endpoint and
//! authenticate to another.

use std::{io, path::Path};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

/// A self-signed certificate authority.
pub struct CertificateAuthority {
    certificate: rcgen::Certificate,
    key_pair: KeyPair,
}

/// A certificate issued by a [`CertificateAuthority`], with its private key.
pub struct Certificate {
    certificate_pem: String,
    private_key_pem: String,
}

impl CertificateAuthority {
    /// Generate a new self-signed certificate authority.
    pub fn generate(common_name: &str) -> Result<Self, String> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];

        let key_pair = KeyPair::generate()
            .map_err(|error| format!("failed to generate the CA key pair: {error}"))?;
        let certificate = params
            .self_signed(&key_pair)
            .map_err(|error| format!("failed to self-sign the CA certificate: {error}"))?;

        Ok(Self {
            certificate,
            key_pair,
        })
    }

    /// Write the CA certificate (PEM) to `path`.
    pub fn write_certificate(&self, path: &Path) -> io::Result<()> {
        std::fs::write(path, self.certificate.pem())
    }

    /// Issue a leaf certificate for the given subject alternative names.
    ///
    /// Each name is parsed as an IP address when possible and otherwise used
    /// as a DNS name. The certificate is valid for both server and client
    /// authentication.
    pub fn issue(
        &self,
        common_name: &str,
        subject_alt_names: &[String],
    ) -> Result<Certificate, String> {
        let mut params = CertificateParams::new(subject_alt_names.to_vec())
            .map_err(|error| format!("invalid subject alternative names: {error}"))?;
        params.is_ca = IsCa::NoCa;
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];

        let key_pair = KeyPair::generate()
            .map_err(|error| format!("failed to generate the key pair: {error}"))?;
        let certificate = params
            .signed_by(&key_pair, &self.certificate, &self.key_pair)
            .map_err(|error| format!("failed to sign the certificate: {error}"))?;

        Ok(Certificate {
            certificate_pem: certificate.pem(),
            private_key_pem: key_pair.serialize_pem(),
        })
    }
}

impl Certificate {
    /// Write the certificate and private key (PEM) to the given paths.
    pub fn write(&self, certificate: &Path, private_key: &Path) -> io::Result<()> {
        std::fs::write(certificate, &self.certificate_pem)?;
        std::fs::write(private_key, &self.private_key_pem)
    }
}
