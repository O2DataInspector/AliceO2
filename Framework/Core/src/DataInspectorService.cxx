#include "Framework/DataInspectorService.h"
#include "Framework/ServiceSpec.h"
#include "Framework/ServiceRegistry.h"
#include "Framework/DeviceSpec.h"

namespace o2::framework
{
DataInspectorProxyService::DataInspectorProxyService(const std::string& deviceName) : deviceName(deviceName), socket(DISocket::connect("127.0.0.1", 8081))
{
  socket.send(DIMessage{DIMessage::Header::Type::DEVICE_ON, deviceName});
}

DataInspectorProxyService::~DataInspectorProxyService()
{
  socket.send(DIMessage{DIMessage::Header::Type::DEVICE_OFF, deviceName});
  socket.close();
}

ServiceSpec DataInspectorProxyService::spec()
{
  return ServiceSpec{
    .name = "data-inspector-proxy-service",
    .init = [](ServiceRegistry& registry, DeviceState& state, fair::mq::ProgOptions& options) -> ServiceHandle {
      const auto& deviceName = registry.get<DeviceSpec const>().name;
      return ServiceHandle{TypeIdHelpers::uniqueId<DataInspectorProxyService>(), new DataInspectorProxyService(deviceName)};
    },
    .kind = ServiceKind::Global
  };
}

void DataInspectorProxyService::receive()
{
  if(socket.isReadyToReceive()) {
    DIMessage msg = socket.receive();
    handleMessage(msg);
  }
}

void DataInspectorProxyService::send(DIMessage&& msg)
{
  socket.send(std::move(msg));
}

void DataInspectorProxyService::handleMessage(DIMessage &msg)
{
  switch (msg.header.type) {
    case DIMessage::Header::Type::INSPECT_ON: {
      LOG(info) << "DIService - INSPECT ON";
      _isInspected = true;
      break;
    }
    case DIMessage::Header::Type::INSPECT_OFF: {
      LOG(info) << "DIService - INSPECT OFF";
      _isInspected = false;
      break;
    }
    default: {
      LOG(info) << "DIService - Wrong msg type: " << static_cast<uint32_t>(msg.header.type);
    }
  }
}

FairMQParts DataInspectorService::copyMessage(FairMQParts &parts)
{
  FairMQParts partsCopy;
  for (auto &part: parts) {
    FairMQTransportFactory *transport = part->GetTransport();
    FairMQMessagePtr message(transport->CreateMessage());
    message->Copy(*part);
    partsCopy.AddPart(std::move(message));
  }
  return partsCopy;
}

ServiceSpec DataInspectorService::spec()
{
  return ServiceSpec{
    .name = "data-inspector-service",
    .init = [](ServiceRegistry& registry, DeviceState& state, fair::mq::ProgOptions& options) -> ServiceHandle {
      const auto& outputs = registry.get<DeviceSpec const>().outputs;

      int channelIndex = 0;
      for(;channelIndex<outputs.size(); channelIndex++){
        if(outputs[channelIndex].channel.find("to_DataInspector") != std::string::npos)
          break;
      }

      return ServiceHandle{TypeIdHelpers::uniqueId<DataInspectorService>(), new DataInspectorService(ChannelIndex{channelIndex})};
    },
    .kind = ServiceKind::Global
  };
}

void DataInspectorService::sendCopyToDataInspectorDevice(FairMQDeviceProxy& proxy, FairMQParts& parts)
{
  auto copy = copyMessage(parts);
  proxy.getOutputChannel(dataInspectorChannelIndex)->Send(copy);
}
}