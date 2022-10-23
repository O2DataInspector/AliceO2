#include "Framework/DataInspector.h"
#include "Framework/DataProcessorSpec.h"
#include "Framework/DeviceSpec.h"
#include "Framework/OutputObjHeader.h"
#include "Framework/RawDeviceService.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <fcntl.h>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include "boost/asio.hpp"
#include <TBufferJSON.h>
#include <boost/algorithm/string/join.hpp>
#include "Framework/DISocket.hpp"
#include "Framework/DataInspectorService.h"
#include <arrow/table.h>
#include "Framework/TableConsumer.h"
#include "Framework/AlgorithmSpec.h"
#include "boost/archive/iterators/base64_from_binary.hpp"
#include "boost/archive/iterators/transform_width.hpp"
#include "boost/predef/other/endian.h"

using namespace rapidjson;

namespace o2::framework::DataInspector
{
  inline size_t base64PaddingSize(uint64_t dataSize)
  {
    return (3 - dataSize % 3) % 3;
  }

  std::string encode64(const char* data, uint64_t size)
  {
    auto* begin = data;
    auto* end = data + size;

    using namespace boost::archive::iterators;
    using EncodingIt = base64_from_binary<transform_width<const char*, 6, 8>>;
    return std::string(EncodingIt(begin), EncodingIt(end)).append(base64PaddingSize(size), '=');
  }

  void addPayload(Document& message,
                  const header::DataHeader* header,
                  const DataRef& ref,
                  Document::AllocatorType& alloc)
  {
    if (header->payloadSerializationMethod == header::gSerializationMethodROOT) {
      std::unique_ptr<TObject> object = DataRefUtils::as<TObject>(ref);
      TString json = TBufferJSON::ToJSON(object.get());

      // This object will contain values in json generated by ROOT
      Value payloadValue;
      payloadValue.SetObject();

      // Parse generated json string and fill payloadValue
      Document payloadDocument;
      payloadDocument.Parse(json.Data());
      for(auto it = payloadDocument.MemberBegin(); it != payloadDocument.MemberEnd(); it++) {
        Value name;
        name.CopyFrom(it->name, alloc);
        Value val;
        val.CopyFrom(it->value, alloc);

        payloadValue.AddMember(name, val, alloc);
      }

      // Add ROOT payload
      message.AddMember("payload", payloadValue, alloc);
    }
    else if(header->payloadSerializationMethod == header::gSerializationMethodArrow) {
      TableConsumer consumer = TableConsumer(reinterpret_cast<const uint8_t*>(ref.payload), header->payloadSize);
      auto table = consumer.asArrowTable();
      message.AddMember("payload", Value(table->ToString().c_str(), alloc), alloc);
    } else {
      message.AddMember("payload", Value(encode64(ref.payload, header->payloadSize).c_str(), alloc), alloc);

      #if BOOST_ENDIAN_BIG_BYTE
      auto endianness = "BIG";
      #elif BOOST_ENDIAN_LITTLE_BYTE
      auto endianness = "LITTLE";
      #else
      auto endianness = "UNKNOWN";
      #endif
      message.AddMember("payloadEndianness", Value(endianness, alloc), alloc);
    }
  }

  void addBasicHeaderInfo(Document& message, const header::DataHeader* header, Document::AllocatorType& alloc)
  {
    std::string origin = header->dataOrigin.as<std::string>();
    std::string description = header->dataDescription.as<std::string>();
    std::string method = header->payloadSerializationMethod.as<std::string>();

    message.AddMember("origin", Value(origin.c_str(), alloc), alloc);
    message.AddMember("description", Value(description.c_str(), alloc), alloc);
    message.AddMember("subSpecification", Value(header->subSpecification), alloc);
    message.AddMember("firstTForbit", Value(header->firstTForbit), alloc);
    message.AddMember("tfCounter", Value(header->tfCounter), alloc);
    message.AddMember("runNumber", Value(header->runNumber), alloc);
    message.AddMember("payloadSize", Value(header->payloadSize), alloc);
    message.AddMember("splitPayloadParts", Value(header->splitPayloadParts), alloc);
    message.AddMember("payloadSerialization", Value(method.c_str(), alloc), alloc);
    message.AddMember("payloadSplitIndex", Value(header->splitPayloadIndex), alloc);
  }

  void buildDocument(Document& message, std::string sender, const DataRef& ref)
  {
    message.SetObject();
    Document::AllocatorType& alloc = message.GetAllocator();
    message.AddMember("sender", Value(sender.c_str(), alloc), alloc);

    const header::BaseHeader* baseHeader = header::BaseHeader::get(reinterpret_cast<const std::byte*>(ref.header));
    for (; baseHeader != nullptr; baseHeader = baseHeader->next()) {
      if (baseHeader->description == header::DataHeader::sHeaderType) {
        const auto* header = header::get<header::DataHeader*>(baseHeader->data());
        addBasicHeaderInfo(message, header, alloc);
        addPayload(message, header, ref, alloc);
      }
      else if (baseHeader->description == DataProcessingHeader::sHeaderType) {
        const auto* header = header::get<DataProcessingHeader*>(baseHeader->data());

        message.AddMember("startTime", Value(header->startTime), alloc);
        message.AddMember("duration", Value(header->duration), alloc);
        message.AddMember("creationTimer", Value(header->creation), alloc);
      }
      else if (baseHeader->description == OutputObjHeader::sHeaderType) {
        const auto* header = header::get<OutputObjHeader*>(baseHeader->data());
        message.AddMember("taskHash", Value(header->mTaskHash), alloc);
      }
    }
  }

  /* Callback which transforms each `DataRef` in `context` to a JSON object and
  sends it on the `socket`. The messages are sent separately. */
  void sendToProxy(DataInspectorProxyService& diProxyService, const std::vector<DataRef>& refs, const std::string& deviceName)
  {
    for(auto& ref : refs) {
      Document message;
      StringBuffer buffer;
      Writer<StringBuffer> writer(buffer);
      buildDocument(message, deviceName, ref);
      message.Accept(writer);

      diProxyService.send(DIMessage{DIMessage::Header::Type::DATA, std::string{buffer.GetString(), buffer.GetSize()}});
    }
  }
}
