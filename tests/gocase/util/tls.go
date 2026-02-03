/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func DefaultTLSConfig() (*tls.Config, error) {
	dir := filepath.Join(*workspace, "..", "tls", "cert")

	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"))
	if err != nil {
		return nil, err
	}

	rootCAs := x509.NewCertPool()
	if ca, err := os.ReadFile(filepath.Join(dir, "ca.crt")); err != nil {
		return nil, err
	} else {
		rootCAs.AppendCertsFromPEM(ca)
	}

	return &tls.Config{
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}, nil
}

// TLSConfigWithSNI creates a TLS config that sends a specific SNI
func TLSConfigWithSNI(sni string, caCertPath string) (*tls.Config, error) {
	rootCAs := x509.NewCertPool()
	if ca, err := os.ReadFile(caCertPath); err != nil {
		return nil, err
	} else {
		rootCAs.AppendCertsFromPEM(ca)
	}

	return &tls.Config{
		ServerName: sni,
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
	}, nil
}

// GenerateTLSCerts generates a CA and server certificate with the given DNS names (SNIs)
// and writes them to the specified directory.
// Returns paths: caCertPath, serverCertPath, serverKeyPath
func GenerateTLSCerts(dir string, dnsNames []string) (caCert, serverCert, serverKey string, err error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", "", "", err
	}

	caCert = filepath.Join(dir, "ca.crt")
	serverCert = filepath.Join(dir, "server.crt")
	serverKey = filepath.Join(dir, "server.key")

	// Generate CA
	caPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Kvrocks Test CA"},
			CommonName:   "Kvrocks Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return "", "", "", err
	}

	caCertParsed, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return "", "", "", err
	}

	// Write CA cert
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	if err := os.WriteFile(caCert, caCertPEM, 0644); err != nil {
		return "", "", "", err
	}

	// Generate server certificate
	serverPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Kvrocks Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCertParsed, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return "", "", "", err
	}

	// Write server cert
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	if err := os.WriteFile(serverCert, serverCertPEM, 0644); err != nil {
		return "", "", "", err
	}

	// Write server key
	serverKeyBytes, err := x509.MarshalECPrivateKey(serverPrivKey)
	if err != nil {
		return "", "", "", err
	}
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyBytes})
	if err := os.WriteFile(serverKey, serverKeyPEM, 0600); err != nil {
		return "", "", "", err
	}

	return caCert, serverCert, serverKey, nil
}
