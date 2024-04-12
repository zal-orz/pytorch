#pragma once

#include <map>
#include <memory>
#include <shared_mutex>

#include <c10/cuda/CUDACachingAllocator.h>
#include <cuda_runtime_api.h>
#include <stdio.h>
#include "libmori.hpp"
// #include "plugin/device/gpu/hal/device/cuda_driver.h"
// #include "plugin/device/gpu/hal/device/gpu_device_manager.h"
// #include "runtime/device/memory_manager.h"

#define MORI_HOST_MEM_ALIGN_SIZE 512

namespace torch {

struct PytorchMemoryManager : public mori::MemoryManager {
 private:
  struct MemoryStatus {
    size_t size = 0;
    bool allocated = false;
  }; // inner struct MemoryStatus

 private:
  // CUDACachingAllocator
  at::Allocator* memory_manager;

  std::map<void*, MemoryStatus> hosts;
  std::multimap<size_t, void*> hosts_idle;

  bool pinned_mem_enabled = false;

  std::shared_mutex hm;

  void* stream;
  void* event;

 protected:
  std::multimap<size_t, void*>::iterator locateIdleAddress(
      void* address,
      size_t size) {
    auto iter = hosts_idle.lower_bound(size);
    if (iter == hosts_idle.end())
      return hosts_idle.end();
    if (iter->first < size)
      return hosts_idle.end();

    while (iter != hosts_idle.end() && iter->first == size) {
      if (iter->second == address)
        return iter;
      ++iter;
    }
    return hosts_idle.end();
  }

 public:
  PytorchMemoryManager(at::Allocator* _memory_manager)
      : memory_manager(_memory_manager) {
    void* host_address = nullptr;
    // size_t size = device::gpu::CudaDriver::total_mem_size() << 2;
    size_t size;
    if (cudaMemGetInfo(_, &size) != cudaSuccess) {
      std::cout << "Getting CUDA memory information failed." << std::endl;
      return;
    }
    size = size << 2;
    // if (device::gpu::CudaDriver::AllocHostPinnedMem(size, &host_address)==0)
    //   return;
    if (cudaMallocHost(&host_address, size, cudaHostAllocDefault) !=
        cudaSuccess) {
      std::cout << "Allocating host memory failed." << std::endl;
      return;
    }

    pinned_mem_enabled = true;
    hosts.emplace(host_address, MemoryStatus{size, false});
    hosts_idle.emplace(size, host_address);

    // if (!device::gpu::GPUDeviceManager::GetInstance().CreateStream(&stream))
    // { MS_EXCEPTION(DeviceProcessError)
    //  << "Creating of data transferring CUDA stream failed.";
    //}
    if (cudaStreamCreate(&stream) != cudaSuccess) {
      std::cout << "Creating of data transferring CUDA stream failed."
                << std::endl;
      return;
    }
  }

  virtual void* allocateDevice(size_t size) override {
    // return memory_manager->MallocMemFromMemPool(size);
    return memory_manager->raw_alloc(size);
  }

  virtual void* allocateHost(size_t size) override {
    size_t aligned_size =
        mori::utils::get_memory_aligned_size(size, MORI_HOST_MEM_ALIGN_SIZE);
    if (aligned_size == 0)
      aligned_size = MORI_HOST_MEM_ALIGN_SIZE;

    std::unique_lock<std::shared_mutex> l{hm};

    auto iter = hosts_idle.lower_bound(aligned_size);
    if (iter == hosts_idle.end())
      return nullptr;
    auto p = hosts.find(iter->second);
    assert(p != hosts.end());

    hosts_idle.emplace(
        iter->first - aligned_size, AddressOffset(iter->second, aligned_size));
    hosts_idle.erase(iter);

    hosts.emplace(
        AddressOffset(p->first, aligned_size),
        MemoryStatus{p->second.size - aligned_size, false});
    p->second.allocated = true;
    p->second.size = aligned_size;
    return p->first;
  }

  virtual void copyIn(void* host_address, void* device_address, size_t size)
      override {
    if (size == 0)
      return;
    std::shared_lock<std::shared_mutex> l{hm};
    /*if
    (!device::gpu::GPUDeviceManager::GetInstance().CopyHostMemToDeviceAsync(
            device_address, host_address, size, stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy in CUDA memcpy failed: submitting asynchronious memory
             copying failed.";
    }
    if (!device::gpu::GPUDeviceManager::GetInstance().SyncStream(stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy in CUDA memcpy failed: Stream synchronization failed.";
    }*/
    cudaError_t err = cudaMemcpyAsync(
        device_address, host_address, size, cudaMemcpyHostToDevice, stream);
    if (err != cudaSuccess) {
      std::cout << "Copy in CUDA memcpy failed" << std::endl;
      return;
    }
    err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
      std::cout << "Stream synchronization failed" << std::endl;
      return;
    }
  }

  virtual void copyOut(void* device_address, void* host_address, size_t size)
      override {
    if (size == 0)
      return;
    std::shared_lock<std::shared_mutex> l{hm};
    /*if
    (!device::gpu::GPUDeviceManager::GetInstance().CopyDeviceMemToHostAsync(
            host_address, device_address, size, stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy out CUDA memcpy failed: submitting asynchronious memory
             copying failed.";
    }
    if (!device::gpu::GPUDeviceManager::GetInstance().SyncStream(stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy in CUDA memcpy failed: Stream synchronization failed.";
    }*/
    cudaError_t err = cudaMemcpyAsync(
        host_address, device_address, size, cudaMemcpyDeviceToHost, stream);
    if (err != cudaSuccess) {
      std::cout << "Copy out CUDA memcpy failed" << std::endl;
      return;
    }
    err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
      std::cout << "Stream synchronization failed" << std::endl;
      return;
    }
  }

  virtual void freeDevice(void* address) override {
    memory_manager->raw_delete(address);
  }

  virtual void freeHost(void* address) {
    std::unique_lock<std::shared_mutex> l{hm};

    auto p = hosts.find(address);
    if (p == hosts.end() || !p->second.allocated)
      MS_LOG(EXCEPTION) << "Find the host memory is not used, address["
                        << address << "].";
    p->second.allocated = false;

    void* free_mem_address = address;
    size_t free_mem_size = p->second.size;

    auto prev = p;
    auto post = p;
    ++post;
    --prev;
    if (post != hosts.end() && !post->second.allocated) {
      auto poe = locateIdleAddress(post->first, post->second.size);
      assert(poe != hosts_idle.end());
      hosts_idle.erase(poe);
      p->second.size += post->second.size;
      free_mem_size = p->second.size;
      hosts.erase(post);
    }

    if (p != hosts.begin() && !prev->second.allocated) {
      auto pre = locateIdleAddress(prev->first, prev->second.size);
      assert(pre != hosts_idle.end());
      hosts_idle.erase(pre);
      prev->second.size += p->second.size;
      free_mem_size = prev->second.size;
      hosts.erase(p);
      free_mem_address = prev->first;
    }

    hosts_idle.emplace(free_mem_size, free_mem_address);
  }

  virtual bool isMemorySectionSupported() const override {
    return false;
  }

  virtual void copyDevice(void* src, void* dst, size_t size) override {
    /*if (!device::gpu::GPUDeviceManager::GetInstance()
             .CopyDeviceMemToDeviceAsync(dst, src, size, stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy device CUDA memcpy failed: submitting asynchronious memory
             copying failed.";
    }
    if (!device::gpu::GPUDeviceManager::GetInstance().SyncStream(stream)) {
      MS_EXCEPTION(DeviceProcessError)
          << "Copy in CUDA memcpy failed: Stream synchronization failed.";
    }*/
    cudaError_t err =
        cudaMemcpyAsync(dst, src, size, cudaMemcpyDeviceToDevice, stream);
    if (err != cudaSuccess) {
      std::cout << "Copy device CUDA memcpy failed" << std::endl;
      return;
    }
    err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
      std::cout << "Stream synchronization failed" << std::endl;
      return;
    }
  }

  virtual void* split(void* address, size_t size) override {
    // return memory_manager->Split(address, size);
    return void * ();
  }
  virtual void* salloc(void* address, size_t size) override {
    // return memory_manager->Salloc(address, size);
    return void * ();
  }
  virtual bool merge(void* left, void* right) override {
    // return memory_manager->Merge(left, right);
    return void * ();
  }

  virtual mori::MemoryInfo getMemoryInfo() const override {
    size_t device_size = 1024 * 8;
    size_t host_size = 32768;
    device_size *= 1048576;
    host_size *= 1048576;

    auto re = mori::create_default_memory_info(device_size, host_size);
    auto common = memory_manager->GetCommonMemInfo();
    auto persistent = memory_manager->GetPersistentMemInfo();
    auto transient = memory_manager->GetTransientMemInfo();
    re.device.total_size = re.device.common_block.size +
        re.device.persistent_block.size + re.device.transient_block.size +
        re.device.reserved_size;
    re.device.common_block.address = common.first;
    re.device.common_block.size = common.second;
    re.device.persistent_block.address = persistent.first;
    re.device.persistent_block.size = persistent.second;
    re.device.transient_block.address = transient.first;
    re.device.transient_block.size = transient.second;
    return re;
  }

  virtual ~MindSporeMemoryManager() {
    assert(hosts.begin() != hosts.end());
    // device::gpu::CudaDriver::FreeHostPinnedMem(hosts.begin()->first);
    cudaError_t err = cudaFreeHost(hosts.begin()->first);
    if (err != cudaSuccess) {
      std::cout << "Freeing host memory failed." << std::endl;
      return;
    }
    // device::gpu::GPUDeviceManager::GetInstance().DestroyStream(&stream);
  }

}; // struct MindSporeMemoryManager

} // namespace torch
