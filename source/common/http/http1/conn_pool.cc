#include "common/http/http1/conn_pool.h"

#include <cstdint>
#include <list>
#include <memory>

#include "envoy/event/dispatcher.h"
#include "envoy/event/schedulable_cb.h"
#include "envoy/event/timer.h"
#include "envoy/http/codec.h"
#include "envoy/http/header_map.h"
#include "envoy/upstream/upstream.h"

#include "common/http/codec_client.h"
#include "common/http/codes.h"
#include "common/http/header_utility.h"
#include "common/http/headers.h"
#include "common/runtime/runtime_features.h"

#include "absl/strings/match.h"

namespace Envoy {
namespace Http {
namespace Http1 {

ActiveClient::StreamWrapper::StreamWrapper(ResponseDecoder& response_decoder, ActiveClient& parent)
    : RequestEncoderWrapper(parent.codec_client_->newStream(*this)),
      ResponseDecoderWrapper(response_decoder), parent_(parent) {
  RequestEncoderWrapper::inner_.getStream().addCallbacks(*this);
}

ActiveClient::StreamWrapper::~StreamWrapper() {
  // Upstream connection might be closed right after response is complete. Setting delay=true
  // here to attach pending requests in next dispatcher loop to handle that case.
  // https://github.com/envoyproxy/envoy/issues/2715
  parent_.parent().onStreamClosed(parent_, true);
}

void ActiveClient::StreamWrapper::onEncodeComplete() { encode_complete_ = true; }

void ActiveClient::StreamWrapper::decodeHeaders(ResponseHeaderMapPtr&& headers, bool end_stream) {
  if (Runtime::runtimeFeatureEnabled("envoy.reloadable_features.fixed_connection_close")) {
    close_connection_ =
        HeaderUtility::shouldCloseConnection(parent_.codec_client_->protocol(), *headers);
    if (close_connection_) {
      parent_.parent().host()->cluster().stats().upstream_cx_close_notify_.inc();
    }
  } else {
    // If Connection: close OR
    //    Http/1.0 and not Connection: keep-alive OR
    //    Proxy-Connection: close
    if ((absl::EqualsIgnoreCase(headers->getConnectionValue(),
                                Headers::get().ConnectionValues.Close)) ||
        (parent_.codec_client_->protocol() == Protocol::Http10 &&
         !absl::EqualsIgnoreCase(headers->getConnectionValue(),
                                 Headers::get().ConnectionValues.KeepAlive)) ||
        (absl::EqualsIgnoreCase(headers->getProxyConnectionValue(),
                                Headers::get().ConnectionValues.Close))) {
      parent_.parent().host()->cluster().stats().upstream_cx_close_notify_.inc();
      close_connection_ = true;
    }
  }
  ResponseDecoderWrapper::decodeHeaders(std::move(headers), end_stream);
}

void ActiveClient::StreamWrapper::onDecodeComplete() {
  ASSERT(!decode_complete_);
  decode_complete_ = encode_complete_;
  ENVOY_CONN_LOG(debug, "response complete", *parent_.codec_client_);

  if (!parent_.stream_wrapper_->encode_complete_) {
    ENVOY_CONN_LOG(debug, "response before request complete", *parent_.codec_client_);
    parent_.codec_client_->close();
  } else if (parent_.stream_wrapper_->close_connection_ || parent_.codec_client_->remoteClosed()) {
    ENVOY_CONN_LOG(debug, "saw upstream close connection", *parent_.codec_client_);
    parent_.codec_client_->close();
  } else {
    auto* pool = &parent_.parent();
    pool->dispatcher().post([pool]() -> void { pool->onUpstreamReady(); });
    parent_.stream_wrapper_.reset();

    pool->checkForDrained();
  }
}

void ActiveClient::StreamWrapper::onResetStream(StreamResetReason, absl::string_view) {
  parent_.codec_client_->close();
}

ActiveClient::ActiveClient(HttpConnPoolImplBase& parent)
    : Envoy::Http::ActiveClient(
          parent, parent.host()->cluster().maxRequestsPerConnection(),
          1 // HTTP1 always has a concurrent-request-limit of 1 per connection.
      ) {
  parent.host()->cluster().stats().upstream_cx_http1_total_.inc();
}

bool ActiveClient::closingWithIncompleteStream() const {
  return (stream_wrapper_ != nullptr) && (!stream_wrapper_->decode_complete_);
}

RequestEncoder& ActiveClient::newStreamEncoder(ResponseDecoder& response_decoder) {
  ASSERT(!stream_wrapper_);
  stream_wrapper_ = std::make_unique<StreamWrapper>(response_decoder, *this);
  return *stream_wrapper_;
}

ConnectionPool::InstancePtr
allocateConnPool(Event::Dispatcher& dispatcher, Random::RandomGenerator& random_generator,
                 Upstream::HostConstSharedPtr host, Upstream::ResourcePriority priority,
                 const Network::ConnectionSocket::OptionsSharedPtr& options,
                 const Network::TransportSocketOptionsSharedPtr& transport_socket_options,
                 Upstream::ClusterConnectivityState& state) {
  return std::make_unique<FixedHttpConnPoolImpl>(
      std::move(host), std::move(priority), dispatcher, options, transport_socket_options,
      random_generator, state,
      [](HttpConnPoolImplBase* pool) { return std::make_unique<ActiveClient>(*pool); },
      [](Upstream::Host::CreateConnectionData& data, HttpConnPoolImplBase* pool) {
        CodecClientPtr codec{new CodecClientProd(
            CodecClient::Type::HTTP1, std::move(data.connection_), data.host_description_,
            pool->dispatcher(), pool->randomGenerator())};
        return codec;
      },
      std::vector<Protocol>{Protocol::Http11});
}

} // namespace Http1
} // namespace Http
} // namespace Envoy
