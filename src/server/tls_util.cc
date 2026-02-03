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
 *
 */

#ifdef ENABLE_OPENSSL

#include "tls_util.h"

#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <pthread.h>

#include <bitset>
#include <mutex>
#include <string>

#include "config.h"

#if OPENSSL_VERSION_NUMBER < 0x10100000L
std::unique_ptr<std::mutex[]> ssl_mutexes;
#endif

void InitSSL() {
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  SSL_load_error_strings();
  ERR_load_crypto_strings();

  if (!RAND_poll()) {
    error("OpenSSL failed to generate random seed");
    exit(1);
  }

  // OpenSSL do not provide builtin thread safety until 1.1.0
  // so we need to use CRYPTO_set_locking_callback

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  ssl_mutexes = std::unique_ptr<std::mutex[]>(new std::mutex[CRYPTO_num_locks()]);

  CRYPTO_set_locking_callback([](int mode, int n, const char *, int) {
    if (mode & CRYPTO_LOCK) {
      ssl_mutexes[n].lock();
    } else {
      ssl_mutexes[n].unlock();
    }
  });

  CRYPTO_set_id_callback([] { return (unsigned long)pthread_self(); });  // NOLINT
#endif
}

StatusOr<unsigned long> ParseSSLProtocols(const std::string &protocols) {  // NOLINT
  unsigned long ctx_options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;           // NOLINT

  if (protocols.empty()) {
    return ctx_options | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
  }

  auto protos = util::Split(util::ToLower(protocols), " ");

  std::bitset<4> has_protocol;
  for (const auto &proto : protos) {
    if (proto == "tlsv1") {
      has_protocol[0] = true;
    } else if (proto == "tlsv1.1") {
      has_protocol[1] = true;
    } else if (proto == "tlsv1.2") {
      has_protocol[2] = true;
#ifdef SSL_OP_NO_TLSv1_3
    } else if (proto == "tlsv1.3") {
      has_protocol[3] = true;
#endif
    } else {
      return {Status::NotOK, "Failed to set SSL protocols: " + proto + " is not a valid protocol"};
    }
  }

  if (!has_protocol[0]) {
    ctx_options |= SSL_OP_NO_TLSv1;
  }
  if (!has_protocol[1]) {
    ctx_options |= SSL_OP_NO_TLSv1_1;
  }
  if (!has_protocol[2]) {
    ctx_options |= SSL_OP_NO_TLSv1_2;
  }
#ifdef SSL_OP_NO_TLSv1_3
  if (!has_protocol[3]) {
    ctx_options |= SSL_OP_NO_TLSv1_3;
  }
#endif

  if (has_protocol.none()) {
    return {Status::NotOK, "Failed to set SSL protocols: no protocol is enabled"};
  }

  return ctx_options;
}

UniqueSSLContext CreateSSLContext(const Config *config, const SSL_METHOD *method) {
  if (config->tls_cert_file.empty() || config->tls_key_file.empty()) {
    error("Both tls-cert-file and tls-key-file must be specified while TLS is enabled");
    return nullptr;
  }

  auto ssl_ctx = UniqueSSLContext(method);
  if (!ssl_ctx) {
    error("Failed to construct SSL context: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }

  auto proto_status = ParseSSLProtocols(config->tls_protocols);
  if (!proto_status) {
    error("{}", proto_status.Msg());
    return nullptr;
  }

  auto ctx_options = *proto_status;

#ifdef SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS
  ctx_options |= SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS;
#endif
#ifdef SSL_OP_NO_COMPRESSION
  ctx_options |= SSL_OP_NO_COMPRESSION;
#endif
#ifdef SSL_OP_NO_CLIENT_RENEGOTIATION
  ctx_options |= SSL_OP_NO_CLIENT_RENEGOTIATION;
#endif

  if (config->tls_prefer_server_ciphers) {
    ctx_options |= SSL_OP_CIPHER_SERVER_PREFERENCE;
  }

  SSL_CTX_set_options(ssl_ctx.get(), ctx_options);

  SSL_CTX_set_mode(ssl_ctx.get(), SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  if (config->tls_session_caching) {
    SSL_CTX_set_session_cache_mode(ssl_ctx.get(), SSL_SESS_CACHE_SERVER);
    SSL_CTX_set_timeout(ssl_ctx.get(), config->tls_session_cache_timeout);
    SSL_CTX_sess_set_cache_size(ssl_ctx.get(), config->tls_session_cache_size);

    const char *session_id = "kvrocks";
    SSL_CTX_set_session_id_context(ssl_ctx.get(), (const unsigned char *)session_id, strlen(session_id));
  } else {
    SSL_CTX_set_session_cache_mode(ssl_ctx.get(), SSL_SESS_CACHE_OFF);
  }

  if (config->tls_auth_clients == TLS_AUTH_CLIENTS_NO) {
    SSL_CTX_set_verify(ssl_ctx.get(), SSL_VERIFY_NONE, nullptr);
  } else if (config->tls_auth_clients == TLS_AUTH_CLIENTS_OPTIONAL) {
    SSL_CTX_set_verify(ssl_ctx.get(), SSL_VERIFY_PEER, nullptr);
  } else {
    SSL_CTX_set_verify(ssl_ctx.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
  }

  auto ca_file = config->tls_ca_cert_file.empty() ? nullptr : config->tls_ca_cert_file.c_str();
  auto ca_dir = config->tls_ca_cert_dir.empty() ? nullptr : config->tls_ca_cert_dir.c_str();
  if (ca_file || ca_dir) {
    if (SSL_CTX_load_verify_locations(ssl_ctx.get(), ca_file, ca_dir) != 1) {
      error("Failed to load CA certificates: {}", fmt::streamed(SSLErrors{}));
      return nullptr;
    }
  } else if (config->tls_auth_clients != TLS_AUTH_CLIENTS_NO) {
    error("Either tls-ca-cert-file or tls-ca-cert-dir must be specified while tls-auth-clients is enabled");
    return nullptr;
  }

  if (SSL_CTX_use_certificate_chain_file(ssl_ctx.get(), config->tls_cert_file.c_str()) != 1) {
    error("Failed to load SSL certificate file: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }

  if (!config->tls_key_file_pass.empty()) {
    SSL_CTX_set_default_passwd_cb_userdata(ssl_ctx.get(), static_cast<void *>(const_cast<Config *>(config)));
    SSL_CTX_set_default_passwd_cb(ssl_ctx.get(), [](char *buf, int size, int, void *cfg) -> int {
      strncpy(buf, static_cast<const Config *>(cfg)->tls_key_file_pass.c_str(), size);
      buf[size - 1] = '\0';
      return static_cast<int>(strlen(buf));
    });
  }

  if (SSL_CTX_use_PrivateKey_file(ssl_ctx.get(), config->tls_key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
    error("Failed to load SSL private key file: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }

  if (SSL_CTX_check_private_key(ssl_ctx.get()) != 1) {
    error("Failed to check the loaded private key: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }

  if (!config->tls_ciphers.empty() && !SSL_CTX_set_cipher_list(ssl_ctx.get(), config->tls_ciphers.c_str())) {
    error("Failed to set SSL ciphers: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }

#ifdef SSL_OP_NO_TLSv1_3
  if (!config->tls_ciphersuites.empty() && !SSL_CTX_set_ciphersuites(ssl_ctx.get(), config->tls_ciphersuites.c_str())) {
    error("Failed to set SSL ciphersuites: {}", fmt::streamed(SSLErrors{}));
    return nullptr;
  }
#endif

  return ssl_ctx;
}

std::ostream &operator<<(std::ostream &os, SSLErrors) {
  ERR_print_errors_cb(
      [](const char *str, size_t len, void *pos) {
        *reinterpret_cast<std::ostream *>(pos) << std::string(str, len) << ";";
        return 0;
      },
      reinterpret_cast<void *>(&os));

  return os;
}

std::ostream &operator<<(std::ostream &os, SSLError e) {
  char msg[1024] = {};
  ERR_error_string_n(e.err, msg, sizeof(msg) - 1);
  return os << msg;
}

// Parse SNI extension from TLS ClientHello
// TLS record format: type(1) + version(2) + length(2) + handshake
// Handshake: type(1) + length(3) + version(2) + random(32) + session_id_len(1) + ...
// Extensions are after cipher suites and compression methods
static std::string ParseSNIFromClientHello(const unsigned char *data, size_t len) {
  // Minimum TLS record header + handshake header
  if (len < 5 + 4) return "";

  // Check TLS record type (0x16 = Handshake)
  if (data[0] != 0x16) return "";

  // Skip TLS record header (5 bytes)
  size_t pos = 5;

  // Check handshake type (0x01 = ClientHello)
  if (data[pos] != 0x01) return "";
  pos += 4;  // Skip handshake type + length

  // Skip version (2) + random (32)
  pos += 2 + 32;
  if (pos >= len) return "";

  // Skip session ID
  uint8_t session_id_len = data[pos++];
  pos += session_id_len;
  if (pos + 2 >= len) return "";

  // Skip cipher suites
  uint16_t cipher_len = (data[pos] << 8) | data[pos + 1];
  pos += 2 + cipher_len;
  if (pos + 1 >= len) return "";

  // Skip compression methods
  uint8_t comp_len = data[pos++];
  pos += comp_len;
  if (pos + 2 >= len) return "";

  // Extensions length
  uint16_t ext_len = (data[pos] << 8) | data[pos + 1];
  pos += 2;
  size_t ext_end = pos + ext_len;
  if (ext_end > len) ext_end = len;

  // Parse extensions
  while (pos + 4 <= ext_end) {
    uint16_t ext_type = (data[pos] << 8) | data[pos + 1];
    uint16_t ext_data_len = (data[pos + 2] << 8) | data[pos + 3];
    pos += 4;

    if (pos + ext_data_len > ext_end) break;

    // SNI extension type = 0x0000
    if (ext_type == 0x0000 && ext_data_len >= 5) {
      // SNI list length (2) + name type (1) + name length (2) + name
      size_t sni_pos = pos + 2;  // Skip list length
      if (sni_pos + 3 > pos + ext_data_len) break;

      uint8_t name_type = data[sni_pos++];
      if (name_type != 0) {  // 0 = hostname
        pos += ext_data_len;
        continue;
      }

      uint16_t name_len = (data[sni_pos] << 8) | data[sni_pos + 1];
      sni_pos += 2;

      if (sni_pos + name_len <= pos + ext_data_len && name_len > 0 && name_len < 256) {
        return std::string(reinterpret_cast<const char *>(data + sni_pos), name_len);
      }
    }

    pos += ext_data_len;
  }

  return "";
}

std::string ExtractSNIFromClientHello(int fd) {
  unsigned char buf[1500];  // ClientHello typically fits in one packet

  // MSG_PEEK: Read without consuming from buffer
  ssize_t n = recv(fd, buf, sizeof(buf), MSG_PEEK);
  if (n < 10) return "";

  return ParseSNIFromClientHello(buf, static_cast<size_t>(n));
}

#endif

// Functions available regardless of TLS support

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstring>
#include <string>

std::string GetPeerIP(int fd) {
  struct sockaddr_storage addr;
  socklen_t addr_len = sizeof(addr);

  if (getpeername(fd, reinterpret_cast<struct sockaddr *>(&addr), &addr_len) != 0) {
    return "";
  }

  char ip_str[INET6_ADDRSTRLEN] = {};

  if (addr.ss_family == AF_INET) {
    auto *sin = reinterpret_cast<struct sockaddr_in *>(&addr);
    inet_ntop(AF_INET, &sin->sin_addr, ip_str, sizeof(ip_str));
  } else if (addr.ss_family == AF_INET6) {
    auto *sin6 = reinterpret_cast<struct sockaddr_in6 *>(&addr);
    inet_ntop(AF_INET6, &sin6->sin6_addr, ip_str, sizeof(ip_str));
  } else {
    return "";
  }

  return ip_str;
}

std::string GetSchedulingKey(int fd, bool is_tls) {
#ifdef ENABLE_OPENSSL
  if (is_tls) {
    std::string sni = ExtractSNIFromClientHello(fd);
    if (!sni.empty()) {
      return sni;
    }
  }
#else
  (void)fd;
  (void)is_tls;
#endif
  // Fallback: use a fixed default domain for non-TLS or missing SNI
  return "default.domain";
}
