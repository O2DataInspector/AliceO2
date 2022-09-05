#ifndef O2_DATAINSPECTOR_H
#define O2_DATAINSPECTOR_H

#include "Framework/DeviceSpec.h"
#include "Framework/WorkflowSpec.h"
#include "Framework/DataInspectorService.h"

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQParts.h>

#include <cstring>

namespace o2::framework::DataInspector
{
/* Checks if a command line argument relates to the data inspector. */
inline bool isInspectorArgument(const char* argument)
{
  return std::strstr(argument, "--inspector") != nullptr;
}

/* Checks if device is used by the data inspector */
inline bool isInspectorDevice(const DataProcessorSpec& spec)
{
  return spec.name == "DataInspector";
}

inline bool isInspectorDevice(const DeviceSpec& spec)
{
  return spec.name == "DataInspector";
}

inline bool isNonInternalDevice(const DeviceSpec& spec)
{
  return spec.name.find("internal") == std::string::npos;
}

/* Copies `parts` and sends it to the `device`. The copy is necessary because of
 * memory management. */
void sendCopy(FairMQDeviceProxy* proxy, FairMQParts& parts, ChannelIndex channelIndex);

/* Creates an O2 Device for the DataInspector and adds it to `workflow`. */
void addDataInspector(WorkflowSpec& workflow);

/* Inject interceptor to send copy of each message to DataInspector. */
inline void injectSendingPolicyInterceptor(DeviceSpec& device)
{
  auto& oldPolicy = device.sendingPolicy;
  device.sendingPolicy = SendingPolicy{
    "data-inspector-policy",
    nullptr,
    [oldPolicy](FairMQDeviceProxy& proxy, FairMQParts& parts, ChannelIndex channelIndex, ServiceRegistry& registry) -> void{
      auto& diService = registry.get<DataInspectorService>();
      if(diService.isInspected())
        sendCopy(&proxy, parts, diService.getDataInspectorChannelIndex());

      oldPolicy.send(proxy, parts, channelIndex, registry);
    }
  };
}

/* Inject interceptor to check for messages from proxy before running onProcess. */
inline void injectOnProcessInterceptor(DataProcessorSpec& spec)
{
  auto& oldProcAlg = spec.algorithm.onProcess;
  spec.algorithm.onProcess = [oldProcAlg](ProcessingContext& context) -> void {
    context.services().get<DataInspectorService>().receive();
    oldProcAlg(context);
  };
}

/* Change completion policy of DataInspector to run on each input. */
inline void changeInspectorPolicies(DeviceSpec& spec)
{
  spec.completionPolicy = CompletionPolicy{
    "data-inspector-completion",
    [](DeviceSpec const& device) { return device.name == "DataInspector"; },
    [](InputSpan const& span) { return CompletionPolicy::CompletionOp::Consume; }};
}

/* Decide which modifications to apply for given device when DataInspector is present. */
inline void modifyPolicies(DeviceSpec& spec)
{
  if (isInspectorDevice(spec)) {
    changeInspectorPolicies(spec);
  } else {
    injectSendingPolicyInterceptor(spec);
  }
}
}

#endif //O2_DATAINSPECTOR_H
