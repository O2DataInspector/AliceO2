#ifndef O2_DATAINSPECTORSERVICE_H
#define O2_DATAINSPECTORSERVICE_H

#include <fairlogger/Logger.h>
#include "DISocket.hpp"
#include "Framework/RoutingIndices.h"
#include "Framework/FairMQDeviceProxy.h"
#include "Framework/ServiceSpec.h"
#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQParts.h>

namespace o2::framework
{
class DataInspectorProxyService {
 public:
  DataInspectorProxyService(const std::string& deviceName);
  ~DataInspectorProxyService();

  static ServiceSpec spec();

  void receive();
  void send(DIMessage&& message);
  bool isInspected() { return _isInspected; }

 private:
  void handleMessage(DIMessage& msg);

  const std::string deviceName;
  bool _isInspected = false;
  DISocket socket;
};

class DataInspectorService {
 public:
  DataInspectorService(ChannelIndex dataInspectorChannelIndex) : dataInspectorChannelIndex(dataInspectorChannelIndex) {}

  static ServiceSpec spec();

  void sendCopyToDataInspectorDevice(FairMQDeviceProxy& proxy, FairMQParts& parts);

 private:
  FairMQParts copyMessage(FairMQParts &parts);

  ChannelIndex dataInspectorChannelIndex;
};
}

#endif //O2_DATAINSPECTORSERVICE_H
