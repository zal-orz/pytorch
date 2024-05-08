#pragma once

// #include <torch/csrc/autograd/variable.h>
#include <c10/core/TensorImpl.h>
#include <c10/core/UndefinedTensorImpl.h>
#include <c10/util/intrusive_ptr.h>
#include <atomic>
#include <memory>
#include <unordered_map>
#include "mori_mem_manager.h"

namespace torch {

struct MoriMemSwappingManager {
 public:
  // static MoriMemSwappingManager& GetInstance();

  // void Register(
  //     const std::string& device_name,
  //     MoriMemSwappingManager&& device_context_creator);

  mori::MemorySwappingManager* GetOrCreateMoriMemSwappingManager(
      const at::Device& device_context_key) {
    if (mori_memory_swapping_manager.get() != nullptr)
      return mori_memory_swapping_manager.get();

    // Create mori memory swapping manager
    mori::Context context;
    context["scheduler"] = "section";
    context["scheduler.dependency.timeaware"] = "false";
    context["scheduler.dependency.threshold"] = "2";

    mori_memory_swapping_manager =
        std::make_shared<mori::MemorySwappingManager>(context);
    // 修改为pytorch的内存管理器
    // Create mori memory manager
    at::Allocator* allocator = at::GetAllocator(device_context_key.type());
    /*mindspore::device::MemoryManager* mem_manager =
        device::DeviceContextManager::GetInstance()
            .GetOrCreateDeviceContext(device_context_key)
            ->mem_manager();*/
    pytorch_memory_manager = std::make_shared<PytorchMemoryManager>(allocator);

    mori_memory_swapping_manager->setMemoryManager(
        pytorch_memory_manager.get());
    // pytorch_mem_manager_->setLogger(&logger);

    return mori_memory_swapping_manager.get();
  }

  // void UpdateMoriMemSwappingManagerKey(
  //     const at::Device& old_key,
  //     const at::Device& new_key);
  void ClearMoriMemSwappingManager() {
    if (mori_memory_swapping_manager->isInited())
      mori_memory_swapping_manager->terminate();
    mori_memory_swapping_manager.reset();
    pytorch_memory_manager.reset();

    tensors_.clear();
  }
  // void WaitSwappingTaskFinishOnDevice() const;

  std::pair<
      c10::intrusive_ptr<c10::TensorImpl, c10::UndefinedTensorImpl>,
      size_t>
  GetTensor(const std::string& tensor_name) {
    return tensors_.at(tensor_name);
  }

  void StoreTensor(
      const std::string& tensor_name,
      c10::intrusive_ptr<c10::TensorImpl, c10::UndefinedTensorImpl> tensor_impl,
      size_t size) {
    if (tensors_.find(tensor_name) != tensors_.end()) {
      throw std::runtime_error("Tensor already exists: " + tensor_name);
    }
    tensors_[tensor_name] = std::make_pair(tensor_impl, size);
  }

  mori::status::Operator& GetOperatorStatus(const std::string& op_name) {
    return operators_.at(op_name);
  }

  bool IsOperatorExist(const std::string& op) const {
    return operators_.find(op) != operators_.end();
  }

  mori::status::Operator& StoreOperatorStatus(
      const mori::status::Operator& op) {
    return operators_.emplace(op.getName(), op).first->second;
  }

  std::string GetOutputOperator() const noexcept {
    return output;
  }
  void SetOutputOperator(const std::string& _output) noexcept {
    output = _output;
  }

  std::string CreateAnonymousOperatorName() {
    std::string re = "anomyous";
    re += std::to_string(index++);
    return re;
  }

  void setDatasetQueueSize(size_t _size) {
    size_ = _size;
  }
  size_t GetDatasetQueueSize() {
    return size_;
  }

  MoriMemSwappingManager() = default;
  ~MoriMemSwappingManager() = default;
  C10_DISABLE_COPY_AND_ASSIGN(MoriMemSwappingManager);

 private:
  std::shared_ptr<mori::MemorySwappingManager> mori_memory_swapping_manager;
  std::shared_ptr<PytorchMemoryManager> pytorch_memory_manager;

  std::unordered_map<
      std::string,
      std::pair<
          c10::intrusive_ptr<c10::TensorImpl, c10::UndefinedTensorImpl>,
          size_t>>
      tensors_;
  std::unordered_map<std::string, mori::status::Operator> operators_;
  std::string output = "";

  std::atomic<int> index{0};

  size_t size_ = 0;

}; // struct DeviceContextManager

extern MoriMemSwappingManager mori_mem_swapping_manager;

} // namespace torch