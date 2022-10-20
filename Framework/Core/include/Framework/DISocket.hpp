#ifndef O2_DISOCKET_HPP
#define O2_DISOCKET_HPP

#include <cstring>
#include "Framework/TypeTraits.h"
#include "boost/asio.hpp"
#include "boost/endian/conversion.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/binary_iarchive.hpp"
#include <sstream>

template <typename T>
std::tuple<char*, uint64_t> boostSerialize(const T& obj)
{
  std::ostringstream buffer;
  boost::archive::binary_oarchive outputArchive(buffer);
  outputArchive << obj;

  auto str = buffer.str();
  auto size = str.length();

  char* serialized = new char[size];
  std::memcpy(serialized, str.c_str(), size);

  return {serialized, size};
}

template <typename T>
T boostDeserialize(char* payload, uint64_t size)
{
  T t{};

  std::istringstream buffer({payload, size});
  boost::archive::binary_iarchive inputArchive(buffer);
  inputArchive >> t;

  return t;
}

struct DIMessage {
  struct __attribute__ ((packed)) Header {
    enum class Type : uint32_t {
      INVALID = 0,
      DATA = 1,
      DEVICE_ON = 2,
      DEVICE_OFF = 3,
      INSPECT_ON = 4,
      INSPECT_OFF = 5,
      TERMINATE = 6
    };

    Header(Type type, uint64_t payloadSize) : typeLE(boost::endian::native_to_little(static_cast<uint32_t>(type))), payloadSizeLE(boost::endian::native_to_little(payloadSize)) {}
    Header(Type type) : Header(type, 0) {}
    Header() : Header(Type::INVALID, 0) {}

    Type type() const;
    uint64_t payloadSize() const;

   private:
    uint32_t typeLE;
    uint64_t payloadSizeLE;
  };

  template<typename T>
  DIMessage(Header::Type type, const T& payload)
  {
    uint64_t payloadSize = 0;
    if constexpr (std::is_base_of_v<std::string, T>) {
      payloadSize = payload.size();
      this->payload = new char[payloadSize];
      std::memcpy(this->payload, payload.data(), payloadSize);
    } else if constexpr (std::is_integral_v<T>) {
      payloadSize = sizeof(T);
      payload = boost::endian::native_to_little(payload);
      this->payload = new char[payloadSize];
      std::memcpy(this->payload, &payload, payloadSize);
    } else if constexpr (o2::framework::is_boost_serializable<T>::value) {
      auto [serialized, size] = boostSerialize(payload);
      payloadSize = size;
      this->payload = serialized;
    } else {
      static_assert(o2::framework::always_static_assert_v<T>, "DISocket: Cannot create message of this type.");
    }

    header = Header{type, payloadSize};
  }
  DIMessage() : header(Header::Type::INVALID), payload(nullptr) {}

  DIMessage(const DIMessage& other) noexcept;
  DIMessage& operator=(const DIMessage& other) noexcept;

  DIMessage(DIMessage&& other) noexcept;
  DIMessage& operator=(DIMessage&& other) noexcept;

  ~DIMessage();

  template<typename T>
  T get() const
  {
    if constexpr (std::is_same_v<std::string, T>) {
      return std::string{payload, header.payloadSize()};
    } else if constexpr (std::is_integral_v<T>) {
      return boost::endian::little_to_native(*((T*) payload));
    } else if constexpr (o2::framework::is_boost_serializable<T>::value) {
      return boostDeserialize<T>(payload, header.payloadSize());
    } else {
      static_assert(o2::framework::always_static_assert_v<T>, "DISocket: Cannot create object of this type.");
    }
  }

  Header header;
  char* payload;
};

class DISocket {
 public:
  DISocket(const std::string& address, int port);
  ~DISocket();

  bool isMessageAvailable();
  void send(const DIMessage& message);
  DIMessage receive();

 private:
  boost::asio::io_context ioContext;
  boost::asio::ip::tcp::socket socket;
};

#endif //O2_DISOCKET_HPP
