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

using namespace rapidjson;

namespace o2::framework::DataInspector
{
  void addPayload(Document& message,
                  const header::DataHeader* header,
                  const DataRef& ref,
                  Document::AllocatorType& alloc){
    if (header->payloadSerializationMethod == header::gSerializationMethodROOT) {
      std::unique_ptr<TObject> object = DataRefUtils::as<TObject>(ref);
      TString json = TBufferJSON::ToJSON(object.get());

      Value payloadValue;
      payloadValue.SetObject();

      Document payloadDocument;
      payloadDocument.Parse(json.Data());
      for(auto it = payloadDocument.MemberBegin(); it != payloadDocument.MemberEnd(); it++) {
        Value name;
        name.CopyFrom(it->name, alloc);
        Value val;
        val.CopyFrom(it->value, alloc);

        payloadValue.AddMember(name, val, alloc);
      }

      message.AddMember("payload", payloadValue, alloc);
    }
    else if(header->payloadSerializationMethod == header::gSerializationMethodArrow) {
        TableConsumer consumer = TableConsumer(reinterpret_cast<const uint8_t*>(ref.payload), header->payloadSize);
        auto table = consumer.asArrowTable();
        message.AddMember("payload", Value(table->ToString().c_str(), alloc), alloc);
    }
  }

  void addBasicHeaderInfo(Document& message, const header::DataHeader* header, Document::AllocatorType& alloc){
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

  void buildDocument(Document& message, std::string sender, const DataRef& ref){
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
  //TODO: ??? move deserialization to proxy ???
  void sendToProxy(DataInspectorProxyService& diProxyService, const std::vector<DataRef>& refs, const std::string& deviceName)
  {
    std::string sender = deviceName;

    for(auto& ref : refs) {
      Document message;
      StringBuffer buffer;
      Writer<StringBuffer> writer(buffer);
      buildDocument(message, sender, ref);
      message.Accept(writer);

      diProxyService.send(DIMessage{DIMessage::Header::Type::DATA, std::string{buffer.GetString(), buffer.GetSize()}});
    }
  }

  inline bool isNonInternalDevice(const DataProcessorSpec &device)
  {
    return device.name.find("internal") == std::string::npos;
  }

  /* Inject interceptor to check for messages from proxy before running onProcess. */
  void injectOnProcessInterceptor(DataProcessorSpec& spec)
  {
    auto& oldProcAlg = spec.algorithm.onProcess;
    spec.algorithm.onProcess = [oldProcAlg](ProcessingContext& context) -> void {
      context.services().get<DataInspectorProxyService>().receive();
      oldProcAlg(context);
    };
  }

  void injectInterceptors(WorkflowSpec &workflow) {
    for (DataProcessorSpec &device: workflow) {
      if (isNonInternalDevice(device)) {

        injectOnProcessInterceptor(device);
      }
    }
  }
}
