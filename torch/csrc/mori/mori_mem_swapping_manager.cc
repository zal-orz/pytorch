#include <torch/csrc/autograd/variable.h>
#include <torch/csrc/mori/mori_mem_swapping_manager.hpp>
// #include "runtime/hardware/device_context_manager.h"

namespace torch {

MoriMemSwappingManager& MoriMemSwappingManager::GetInstance() {
  static MoriMemSwappingManager instance{};
  return instance;
}

void MoriMemSwappingManager::ClearMoriMemSwappingManager() {
  for (auto& iter : mori_mem_swapping_managers_) {
    // MS_LOG(INFO) << "Release mori memory manager for " << iter.first;
    // MS_EXCEPTION_IF_NULL(iter.second);
    if (iter.second->isInited())
      iter.second->terminate();
  }
  mori_mem_swapping_managers_.clear();
  mori_mem_managers_.clear();

  tensors_.clear();
}

mori::MemorySwappingManager* MoriMemSwappingManager::
    GetOrCreateMoriMemSwappingManager(const at::Device& device_context_key) {
  std::string mori_mem_swapping_manager_key_str = device_context_key.str();

  // Get mori memory swapping manager
  auto mori_mem_swapping_manager_iter =
      mori_mem_swapping_managers_.find(mori_mem_swapping_manager_key_str);
  if (mori_mem_swapping_manager_iter != mori_mem_swapping_managers_.end()) {
    return mori_mem_swapping_manager_iter->second.get();
  }

  // Create mori memory swapping manager
  mori::Context context;
  context["exporters.events"] = "json";
  context["exporters.events.path"] =
      "/home/zal/mori_json/libmori_exporter_events_json.so";
  context["exporters.events.method"] = "file";
  context["exporters.events.method.filename"] = "events_export.log";
  context["exporters.tensors"] = "json";
  context["exporters.tensors.path"] =
      "/home/zal/mori_json/libmori_exporter_tensors_json.so";
  context["exporters.tensors.method"] = "file";
  context["exporters.tensors.method.filename"] = "tensor_export.log";
  context["exporters.schedule"] = "json";
  context["exporters.schedule.path"] =
      "/home/zal/mori_json/libmori_exporter_schedule_json.so";
  context["exporters.schedule.method"] = "file";
  context["exporters.schedule.method.filename"] = "schedule_export.log";
  context["scheduler"] = "section";
  context["scheduler.dependency.timeaware"] = "false";
  context["scheduler.dependency.threshold"] = "2";

  auto mori_memory_swapping_manager =
      std::make_shared<mori::MemorySwappingManager>(context);
  // 修改为pytorch的内存管理器
  // Create mori memory manager
  at::Allocator* allocator = at::GetAllocator(device_context_key.type());
  /*mindspore::device::MemoryManager* mem_manager =
      device::DeviceContextManager::GetInstance()
          .GetOrCreateDeviceContext(device_context_key)
          ->mem_manager();*/
  auto pytorch_mem_manager = std::make_shared<PytorchMemoryManager>(allocator);

  mori_mem_swapping_managers_.insert(std::make_pair(
      mori_mem_swapping_manager_key_str, mori_memory_swapping_manager));
  mori_mem_managers_.insert(
      std::make_pair(mori_mem_swapping_manager_key_str, pytorch_mem_manager));

  mori_memory_swapping_manager->setMemoryManager(pytorch_mem_manager.get());
  // mori_memory_swapping_manager->setLogger(&logger);

  return mori_memory_swapping_manager.get();
}

void MoriMemSwappingManager::UpdateMoriMemSwappingManagerKey(
    const at::Device& old_key,
    const at::Device& new_key) {
  //   std::string old_key_str = old_key.ToString();
  //   std::string new_key_str = new_key.ToString();

  //   auto handle = mori_mem_swapping_managers_.extract(old_key_str);
  //   if (handle.empty()) {
  //     MS_LOG(EXCEPTION) << "Can not find device context for: " <<
  //     old_key_str;
  //   }

  //   handle.key() = new_key_str;
  //   (void)device_contexts_.insert(std::move(handle));
}

void MoriMemSwappingManager::WaitSwappingTaskFinishOnDevice() const {
  //   for (const auto &item : device_contexts_) {
  //     auto device_context = item.second;
  //     try {
  //       if (device_context != nullptr && !device_context->SyncStream()) {
  //         MS_LOG(ERROR) << "SyncStream failed";
  //         return;
  //       }
  //     } catch (const std::exception &ex) {
  //       MS_LOG(ERROR) << "SyncStream failed, exception:" << ex.what();
  //       return;
  //     }
  //   }
}

} // namespace torch
