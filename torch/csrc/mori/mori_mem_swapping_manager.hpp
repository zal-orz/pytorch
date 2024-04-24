#pragma once

// #include <torch/csrc/autograd/variable.h>
#include <torch/csrc/mori/mori_mem_manager.hpp>
#include <atomic>
#include <memory>
#include <unordered_map>

namespace at {
struct Tensor {};
} // namespace at

namespace torch {

struct MoriMemSwappingManager {
 public:
  static MoriMemSwappingManager& GetInstance();
  void Register(
      const std::string& device_name,
      MoriMemSwappingManager&& device_context_creator);
  mori::MemorySwappingManager* GetOrCreateMoriMemSwappingManager(
      const at::Device& device);
  void UpdateMoriMemSwappingManagerKey(
      const at::Device& old_key,
      const at::Device& new_key);
  void ClearMoriMemSwappingManager();
  void WaitSwappingTaskFinishOnDevice() const;

  std::pair<at::Tensor, size_t> GetTensor(const std::string& tensor_name) {
    return tensors_.at(tensor_name);
  }

  void StoreTensor(
      const std::string& tensor_name,
      at::Tensor tensor,
      size_t size) {
    tensors_[tensor_name] = std::make_pair(tensor, size);
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

 private:
  MoriMemSwappingManager() = default;
  ~MoriMemSwappingManager() = default;
  C10_DISABLE_COPY_AND_ASSIGN(MoriMemSwappingManager);

  std::shared_ptr<mori::MemorySwappingManager> mori_memory_swapping_manager;
  std::shared_ptr<PytorchMemoryManager> pytorch_memory_manager;

  std::unordered_map<std::string, std::pair<at::Tensor, size_t>> tensors_;
  std::unordered_map<std::string, mori::status::Operator> operators_;
  std::string output = "";

  std::atomic<int> index{0};

  size_t size_ = 0;

}; // struct DeviceContextManager

} // namespace torch