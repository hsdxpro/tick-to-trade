// The C++ engine: same three busy-polling threads, same rings, same stages
// as the Rust engine, so the same harness clocks both languages with one
// methodology. Sockets are the only platform-touched code, kept in a shim
// small enough to read in one breath.
//
//   engine [feed-port] [orders-host] [orders-port]   (defaults 9701 127.0.0.1 9702)

#include "../spsc.hpp"
#include "pipeline.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using SocketHandle = SOCKET;
/// What this platform's `send` and `recv` take as a buffer length. Winsock
/// says `int`, POSIX says `size_t`, and casting at the call site would be
/// right on one of them and a sign conversion on the other.
using SocketLen = int;
constexpr SocketHandle kBadSocket = INVALID_SOCKET;
static void socket_init() {
    WSADATA data;
    WSAStartup(MAKEWORD(2, 2), &data);
}
static void set_nonblocking(SocketHandle s) {
    u_long on = 1;
    ioctlsocket(s, FIONBIO, &on);
}
static bool would_block() { return WSAGetLastError() == WSAEWOULDBLOCK; }
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketHandle = int;
using SocketLen = std::size_t;
constexpr SocketHandle kBadSocket = -1;
static void socket_init() {}
static void set_nonblocking(SocketHandle s) {
    fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK);
}
static bool would_block() { return errno == EWOULDBLOCK || errno == EAGAIN; }
#endif

namespace {

using namespace t2t;
using namespace t2t::pipeline;

using Clock = std::chrono::steady_clock;

} // namespace

int main(int argc, char** argv) {
    socket_init();
    const auto feed_port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9701);
    const char* orders_host = argc > 2 ? argv[2] : "127.0.0.1";
    const auto orders_port = static_cast<std::uint16_t>(argc > 3 ? std::atoi(argv[3]) : 9702);

    const SocketHandle feed = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in feed_address{};
    feed_address.sin_family = AF_INET;
    feed_address.sin_port = htons(feed_port);
    feed_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (feed == kBadSocket
        || bind(feed, reinterpret_cast<const sockaddr*>(&feed_address), sizeof feed_address)
               != 0) {
        std::printf("engine: cannot bind the feed port %u\n", feed_port);
        return 1;
    }
    set_nonblocking(feed);

    const SocketHandle orders = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in orders_address{};
    orders_address.sin_family = AF_INET;
    orders_address.sin_port = htons(orders_port);
    inet_pton(AF_INET, orders_host, &orders_address.sin_addr);
    if (orders == kBadSocket
        || connect(orders, reinterpret_cast<const sockaddr*>(&orders_address),
                   sizeof orders_address)
               != 0) {
        std::printf("engine: cannot reach the order listener\n");
        return 1;
    }
    const int one = 1;
    setsockopt(orders, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one),
               sizeof one);
    std::printf("engine (C++): feed :%u, orders %s:%u\n", feed_port, orders_host, orders_port);

    SpscQueue<BboUpdate> to_strategy(1024);
    SpscQueue<OrderCommand> to_gateway(1024);

    std::thread feed_thread([&] {
        feed::Itch<FeedStage> parser{feed::synth::kTradfi};
        FeedStage stage(4, kBand);
        std::byte datagram[2048];
        for (;;) {
            const auto received = recv(feed, reinterpret_cast<char*>(datagram),
                                       static_cast<SocketLen>(sizeof datagram), 0);
            if (received < 0) {
                if (would_block()) {
                    continue;
                }
                std::printf("engine: feed socket failed\n");
                std::exit(1);
            }
            const auto outcome = parser.parse(
                feed::Bytes{datagram, static_cast<std::size_t>(received)}, stage);
            if (!outcome.ok()) {
                std::printf("engine: undecodable datagram dropped\n");
                continue;
            }
            if (const auto update = stage.take_moved()) {
                auto item = *update;
                while (!to_strategy.try_push(std::move(item))) {
                }
            }
        }
    });

    std::thread strategy_thread([&] {
        Strategy strategy;
        for (;;) {
            std::optional<BboUpdate> update;
            while (!(update = to_strategy.try_pop())) {
            }
            if (auto order = strategy.decide(*update)) {
                while (!to_gateway.try_push(std::move(*order))) {
                }
            }
        }
    });

    for (;;) {
        std::optional<OrderCommand> order;
        while (!(order = to_gateway.try_pop())) {
        }
        const auto encoded = order->encode();
        std::size_t written = 0;
        while (written < encoded.size()) {
            const auto sent = send(orders, reinterpret_cast<const char*>(encoded.data()) + written,
                                   static_cast<SocketLen>(encoded.size() - written), 0);
            if (sent <= 0) {
                std::printf("engine: order connection lost\n");
                return 1;
            }
            written += static_cast<std::size_t>(sent);
        }
    }
}
