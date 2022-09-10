#ifndef O2_DISOCKET_HPP
#define O2_DISOCKET_HPP

#include <cstring>
#include "boost/asio.hpp"
#include "boost/endian/conversion.hpp"
#include <sstream>

struct DIMessage {
  struct Header {
    enum class Type : uint32_t {
      DATA = 1,
      DEVICE_ON = 2,
      DEVICE_OFF = 3,
      INSPECT_ON = 4,
      INSPECT_OFF = 5
    };

    Header(Type type, uint64_t payloadSize) : type(type), payloadSize(payloadSize) {}

    Type type;
    uint64_t payloadSize;
  };

  DIMessage(Header::Type type, const std::string& payload) : header(type, payload.size()), payload(payload) {}
  DIMessage(Header::Type type) : header(type, 0), payload() {}

  Header header;
  std::string payload;
};

static boost::asio::io_context ioContext;

#define ASIO_CATCH(customMessage) catch(boost::system::system_error& err){ auto msg = std::string{err.what()}; auto code = std::to_string(err.code().value()); throw std::runtime_error{"Exception in DataInspector (boost_code=" + code + ", boost_msg=" + msg + ") - " + customMessage}; }

class DISocket {
 public:
  DISocket(std::unique_ptr<boost::asio::ip::tcp::socket> socket) : socket(std::move(socket)) {}
  DISocket(DISocket&& diSocket)  noexcept : socket(std::move(diSocket.socket)) {}

  DISocket operator=(const DISocket& diSocket) = delete;

  static DISocket connect(const std::string& address, int port) {
    try {
      auto socket = std::make_unique<boost::asio::ip::tcp::socket>(ioContext);
      socket->connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(address), port));
      return DISocket{std::move(socket)};
    }
    ASIO_CATCH("DISocket::connect")
  }

  void send(DIMessage&& message)
  {
    try {
      char header[12];
      uint32_t type_LE = boost::endian::native_to_little(static_cast<uint32_t>(message.header.type));
      uint64_t payloadSize_LE = boost::endian::native_to_little(message.header.payloadSize);
      memcpy(header, &type_LE, 4);
      memcpy(header + 4, &payloadSize_LE, 8);

      // send header
      socket->send(boost::asio::buffer(header, 12));

      // ignore payload
      if (message.header.payloadSize == 0)
        return;

      // send payload
      socket->send(boost::asio::buffer(message.payload, message.header.payloadSize));
    }
    ASIO_CATCH("DISocket::send")
  }

  DIMessage receive()
  {
    try
    {
      // read header
      char header[12];
      socket->read_some(boost::asio::buffer(header, 12));

      uint32_t type = boost::endian::little_to_native(*((uint32_t*)header));
      uint64_t payloadSize = boost::endian::little_to_native(*((uint64_t*)(header + 4)));

      // read payload
      if (payloadSize > 0) {
        char* payload = new char[payloadSize];
        uint64_t read = 0;
        while (read < payloadSize) {
          read += socket->read_some(boost::asio::buffer(payload + read, payloadSize - read));
        }

        return DIMessage{static_cast<DIMessage::Header::Type>(type), std::string{payload, payloadSize}};
      } else {
        return DIMessage{static_cast<DIMessage::Header::Type>(type)};
      }
    }
    ASIO_CATCH("DISocket::receive")
  }

  bool isReadyToReceive()
  {
    try {
      return socket->available() >= 12;
    }
    ASIO_CATCH("DISocket::isReadyToReceive")
  }

  void close()
  {
    socket->close();
  }

 private:
  std::unique_ptr<boost::asio::ip::tcp::socket> socket;
};

#endif //O2_DISOCKET_HPP
