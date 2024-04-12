#pragma once

#include <cassert>
#include <chrono>
#include <sstream>
#include <string>

namespace mori {

enum class ApplicationStage {
  all,
  forward,
  backward
}; // enum class ApplicationStage

namespace util {

static std::string get_application_stage_str(ApplicationStage stage) {
  switch (stage) {
    case ApplicationStage::all:
      return "all";
    case ApplicationStage::forward:
      return "forward";
    case ApplicationStage::backward:
      return "backward";
  }
  return "";
}

} // namespace util
} // namespace mori

namespace mori {
namespace events {

enum class MemoryEventType {
  allocate,
  write,
  read,
  access,
  swapin,
  swapout,
  free,
  reshape
}; // enum MemoryEventType

namespace util {
static std::string get_event_type_str(MemoryEventType type) {
  switch (type) {
    case MemoryEventType::allocate:
      return "allocate";
    case MemoryEventType::write:
      return "write";
    case MemoryEventType::read:
      return "read";
    case MemoryEventType::access:
      return "access";
    case MemoryEventType::swapin:
      return "swapin";
    case MemoryEventType::swapout:
      return "swapout";
    case MemoryEventType::free:
      return "free";
    case MemoryEventType::reshape:
      return "reshape";
  }

  assert(0);
  return "";
}

static long get_timestamp_val(
    const std::chrono::steady_clock::time_point& timestamp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             timestamp.time_since_epoch())
      .count();
}
} // namespace util

struct MemoryEvent final {
  std::string op;
  std::string tensor;
  size_t size;
  MemoryEventType type;
  ApplicationStage stage;
  std::chrono::steady_clock::time_point timestamp;

  MemoryEvent() {
    op = "";
    tensor = "";
    size = 0;
    type = MemoryEventType::access;
    stage = ApplicationStage::all;
    timestamp = std::chrono::steady_clock::now();
  }

  MemoryEvent(
      const std::string& _op,
      const std::string& _tensor,
      size_t _size,
      MemoryEventType _type,
      ApplicationStage _stage,
      const std::chrono::steady_clock::time_point& _timestamp) {
    op = _op;
    tensor = _tensor;
    size = _size;
    type = _type;
    stage = _stage;
    timestamp = _timestamp;
  }

  MemoryEvent(
      const std::string& _op,
      const std::string& _tensor,
      size_t _size,
      MemoryEventType _type,
      ApplicationStage _stage) {
    op = _op;
    tensor = _tensor;
    size = _size;
    type = _type;
    stage = _stage;
    timestamp = std::chrono::steady_clock::now();
  }

  MemoryEvent(const MemoryEvent& event) = default;
  MemoryEvent& operator=(const MemoryEvent& event) = default;

  bool operator<(const MemoryEvent& event) const {
    return timestamp < event.timestamp;
  }

  operator std::string() const {
    std::stringstream ss;
    ss << "Timestamp: " << util::get_timestamp_val(timestamp)
       << " operator: " << op << " tensor: " << tensor << " size: " << size
       << " type: " << util::get_event_type_str(type)
       << " stage: " << mori::util::get_application_stage_str(stage);
    return ss.str();
  }

}; // struct MemoryEvents

} // namespace events
} // namespace mori

#include <string>
#include <unordered_map>
#include <vector>

namespace mori {

struct backend_exception : public std::exception {
  backend_exception() = default;
  backend_exception(const backend_exception& exception) = default;

  backend_exception& operator=(const backend_exception& exception) = default;
  virtual const char* what() const throw() {
    return "Backend exception.";
  }

  virtual ~backend_exception() = default;
}; // struct backend_exception

struct dynamic_library_exception : public backend_exception {
 protected:
  std::string reason;

 public:
  dynamic_library_exception(const std::string _reason) : reason(_reason) {}
  dynamic_library_exception(const dynamic_library_exception& exception) =
      default;

  dynamic_library_exception& operator=(
      const dynamic_library_exception& exception) = default;
  virtual const char* what() const throw() {
    return reason.c_str();
  }

  virtual ~dynamic_library_exception() = default;
}; // struct dynamic_library_exception

} // namespace mori

#include <string>

#include <cmath>
#include <sstream>

namespace mori {
namespace utils {

inline static void* address_offset(void* address, size_t size) {
  return (uint8_t*)address + size;
}

// inline static size_t get_memory_block(void* address, size_t size) {
//     return reinterpret_cast<size_t>(address) / size;
// }

// inline static void* get_block_base_address(size_t block, size_t size) {
//     return reinterpret_cast<void*>(block * size);
// }

inline static size_t get_memory_aligned_size(size_t size, size_t alignment) {
  if (size == 0)
    return 0;
  return ((size - 1) / alignment + 1) * alignment;
}

inline static bool memory_address_aligned(void* address, size_t alignment) {
  return (reinterpret_cast<size_t>(address) % alignment) == 0;
}

inline static std::string make_pointer_string_hex(void* address) {
  std::stringstream ss;
  ss << std::hex << reinterpret_cast<size_t>(address);
  return ss.str();
}

} // namespace utils
} // namespace mori

namespace mori {

struct memory_exception : public std::exception {
 protected:
  std::string reason = "Memory exception.";

 public:
  memory_exception() = default;
  memory_exception(const std::string& _reason) : reason(_reason) {}
  memory_exception(const memory_exception& exception) = default;

  memory_exception(void* address) {
    reason = utils::make_pointer_string_hex(address);
    reason += ": Memory exception.";
  }
  memory_exception(void* address, const std::string& _reason) {
    reason = utils::make_pointer_string_hex(address);
    reason += _reason;
  }

  memory_exception& operator=(const memory_exception& exception) = default;
  virtual const char* what() const throw() override {
    return reason.c_str();
  }

  virtual ~memory_exception() = default;
}; // struct memory_exception

struct memory_insufficience : public memory_exception {
 protected:
  size_t size;

 public:
  memory_insufficience(const std::string& _reason, size_t _size)
      : memory_exception(_reason), size(_size) {}
  virtual const char* what() const noexcept override {
    return reason.c_str();
  }
  virtual size_t demand() const {
    return size;
  }
}; // struct memory_insufficience

struct memory_host_insufficience : public memory_insufficience {
  memory_host_insufficience(const std::string& _reason, size_t _size)
      : memory_insufficience(_reason, _size) {}

  virtual ~memory_host_insufficience() = default;
}; // struct memory_host_insufficience

struct memory_device_insufficience : public memory_insufficience {
  memory_device_insufficience(const std::string& _reason, size_t _size)
      : memory_insufficience(_reason, _size) {}

  virtual ~memory_device_insufficience() = default;
}; // struct memory_device_insufficience

struct memory_allocated : public memory_exception {
  memory_allocated() {
    reason = "Memory already allocated.";
  }
  memory_allocated(const std::string& _reason) : memory_exception(_reason) {}
  memory_allocated(void* address)
      : memory_exception(address, ": Memory already allocated.") {}
  memory_allocated(void* address, const std::string& _reason)
      : memory_exception(address, _reason) {}
  virtual ~memory_allocated() = default;
}; // struct memory_allocated

struct memory_not_allocated : public memory_exception {
  memory_not_allocated() {
    reason = "Memory not allocated.";
  }
  memory_not_allocated(const std::string& _reason)
      : memory_exception(_reason) {}
  memory_not_allocated(void* address)
      : memory_exception(address, ": Memory not allocated.") {}
  memory_not_allocated(void* address, const std::string& _reason)
      : memory_exception(address, _reason) {}
  virtual ~memory_not_allocated() = default;
}; // struct memory_not_allocated

struct memory_operation_invalid : public memory_exception {
  memory_operation_invalid() {
    reason = "Memory operation invalid.";
  }
  memory_operation_invalid(const std::string& _reason)
      : memory_exception(_reason) {}
  memory_operation_invalid(void* address)
      : memory_exception(address, ": Memory operation invalid.") {}
  memory_operation_invalid(void* address, const std::string& _reason)
      : memory_exception(address, _reason) {}
  virtual ~memory_operation_invalid() = default;
}; // struct memory_operation_invalid

struct memory_unmanaged : public memory_exception {}; // struct memory_unmanaged

} // namespace mori

namespace mori {

struct status_exception : public std::exception {
 protected:
  std::string reason = "Status exception.";

 public:
  status_exception() = default;
  status_exception(const std::string& _reason) : reason(_reason) {}
  status_exception(const status_exception&) = default;
  status_exception& operator=(const status_exception&) = default;

  virtual const char* what() const noexcept override {
    return reason.c_str();
  }

  virtual ~status_exception() = default;
}; // struct status_exception

struct uninited_exception : public status_exception {
  uninited_exception() {
    reason = "Not inited.";
  }
  uninited_exception(const std::string& _reason) : status_exception(_reason) {}
}; // struct uninited_exception

struct inited_exception : public status_exception {
  inited_exception() {
    reason = "Already inited.";
  }
  inited_exception(const std::string& _reason) : status_exception(_reason) {}
}; // struct inited_exception

} // namespace mori

namespace mori {

struct context_exception : public std::exception {
  context_exception() = default;
  context_exception(const context_exception& exception) = default;

  context_exception& operator=(const context_exception& exception) = default;
  virtual const char* what() const throw() {
    return "Context exception.";
  }
}; // struct status_exception

struct context_missing : public context_exception {
 protected:
  std::string reason = "Context missing: ";

 public:
  context_missing(const std::string& parameter) {
    reason += parameter;
  }
  context_missing(const context_missing& exception) = default;

  context_missing& operator=(const context_missing& exception) = default;
  virtual const char* what() const throw() {
    return reason.c_str();
  }

  ~context_missing() = default;
}; // struct context_missing

struct context_invalid : public context_exception {
 protected:
  std::string reason = "Context invalid: ";

 public:
  context_invalid(const std::string& parameter) {
    reason += parameter;
  }
  context_invalid(const context_invalid& exception) = default;

  context_invalid& operator=(const context_invalid& exception) = default;
  virtual const char* what() const throw() {
    return reason.c_str();
  }

  ~context_invalid() = default;
}; // struct context_invalid

} // namespace mori

namespace mori {
namespace status {

struct memory_status_exception : public std::exception {
 protected:
  std::string reason = "Memory status exception.";

 public:
  memory_status_exception() = default;
  memory_status_exception(const std::string& _reason) : reason(_reason) {}
  memory_status_exception(const memory_status_exception&) = default;
  memory_status_exception& operator=(const memory_status_exception&) = default;

  virtual const char* what() const noexcept override {
    return reason.c_str();
  }

  virtual ~memory_status_exception() = default;
}; // struct memory_status_exception

struct tensor_invalid : public memory_status_exception {
  tensor_invalid() {
    reason = "Tensor status invalid.";
  }
  tensor_invalid(const std::string& _reason)
      : memory_status_exception(_reason) {}
}; // struct tensor_invalid

struct memory_section_invalid : public memory_status_exception {
  memory_section_invalid() {
    reason = "Memory section status invalid.";
  }
  memory_section_invalid(const std::string& _reason)
      : memory_status_exception(_reason) {}
}; // struct memory_section_invalid

struct memory_section_nonexist : public memory_status_exception {
  memory_section_nonexist() {
    reason = "Memory section not exist.";
  }
  memory_section_nonexist(const std::string& _reason)
      : memory_status_exception(_reason) {}
}; // struct memory_section_nonexist

} // namespace status
} // namespace mori

namespace mori {

struct event_exception : public std::exception {
  event_exception() = default;
  event_exception(const event_exception& exception) = default;

  event_exception& operator=(const event_exception& exception) = default;
  virtual const char* what() const throw() {
    return "Event exception.";
  }

  virtual ~event_exception() = default;
}; // struct event_exception

struct event_conflict : public event_exception {
 protected:
  std::string reason;

 public:
  event_conflict(const std::string _reason) : reason(_reason) {}
  event_conflict(const event_conflict& exception) = default;

  event_conflict& operator=(const event_conflict& exception) = default;
  virtual const char* what() const throw() {
    return reason.c_str();
  }

  virtual ~event_conflict() = default;
}; // struct event_conflict

} // namespace mori

#include <map>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <string>

namespace mori {

struct PerfModel {
  size_t read_speed;
  size_t write_speed;
}; // struct PerfModel

// static PerfModel create_gpu_performance_model() {
//     return PerfModel(12288, 12288);
// }
// static PerfModel create_cpu_performance_model() {
//     return PerfModel(65536, 65536);
// }
// static PerfModel create_nvm_performance_model() {
//     // CPU memory is not a limitor of memory swapping. The current spped is
//     set to 64 GB/s. return PerfModel{40960, 20480};
// }
// static PerfModel create_nvme_performance_model() {
//     return PerfModel{2560, 2048};
// }

struct MemoryInfo {
 public:
  struct Device {
    std::string type;
    size_t total_size;

    size_t block_size;

    size_t align_size;
  }; // inner struct Device

  struct Host {
    std::string type;
    size_t total_size;
  }; // innter struct Host

 public:
  Device device;
  Host host;
}; // struct MemoryInfo

static MemoryInfo create_default_memory_info(size_t device, size_t host) {
  MemoryInfo re;
  re.device.type = "gpu";
  re.device.total_size = device;
  re.device.block_size = 1048576;
  re.device.block_size *= 1024; // 1 GB
  re.device.align_size = 512; // 512 B
  re.host.type = "cpu";
  re.host.total_size = host;
  return re;
}

} // namespace mori

namespace mori {
namespace decisions {
struct Model;
} // namespace decisions

struct Region final {
  std::string name;
  size_t size = 0;

  std::vector<size_t> sections;
  size_t fragment_size = 0;

  Region() = default;
  Region(const std::string& _name, size_t _size) : name(_name), size(_size) {}
}; // struct Region

struct Layer final {
 public:
  using iterator = std::vector<std::string>::iterator;
  using const_iterator = std::vector<std::string>::const_iterator;

 public:
  std::vector<std::string> regions;
  size_t size = 0;
  size_t requested_size = 0;

 public:
  Layer() = default;
  Layer(size_t _size) : size(_size) {}

  inline void submit(const std::string& name, size_t size) {
    regions.push_back(name);
    requested_size += size;
  }

  inline bool isAccomodatable() const noexcept {
    return requested_size <= size;
  }

  iterator begin() {
    return regions.begin();
  }
  const_iterator begin() const {
    return regions.begin();
  }

  iterator end() {
    return regions.end();
  }
  const_iterator end() const {
    return regions.end();
  }
}; // struct Layer

/**
 * Describe the layout for all tensors in the memory.
 */
struct MemoryMap final {
 private:
  friend struct decisions::Model;

 private:
  std::unordered_map<std::string, Region> regions;
  std::vector<Layer> layers;

  size_t memory_size = 0;

  int current_layer = 0;

 public:
  MemoryMap() {
    layers.emplace_back();
  }

  inline void setMemorySize(size_t _size) {
    memory_size = _size;
    layers[0].size = _size;
  }
  inline size_t getMemorySize() {
    return memory_size;
  }

  inline void createLayer() {
    layers.emplace_back(memory_size);
    ++current_layer;
  }
  inline Layer& referenceLayer(int _layer) {
    return layers[_layer];
  }
  inline Layer& referenceLayer() {
    return referenceLayer(current_layer);
  }

  inline void submitMemoryRegion(int _layer, const Region& _region) {
    layers[_layer].submit(_region.name, _region.size);
    regions.emplace(_region.name, _region);
  }
  inline void submitMemoryRegion(const Region& _region) {
    submitMemoryRegion(current_layer, _region);
  }

  inline int getLayersCount() const {
    return layers.size();
  }
  inline Layer& getLayer(int layer) {
    return layers[layer];
  }
  inline const Layer& getLayer(int layer) const {
    return layers[layer];
  }
  inline Layer& getCurrentLayer() {
    return getLayer(current_layer);
  }
  inline std::vector<size_t> getSections(const std::string& tensor) const {
    return regions.at(tensor).sections;
  }
  inline size_t getFragmentSize(const std::string& tensor) const {
    return regions.at(tensor).fragment_size;
  }

  std::unordered_map<std::string, size_t> getFragmentInfo() const {
    std::unordered_map<std::string, size_t> re;
    for (auto& x : regions) {
      if (x.second.fragment_size != 0)
        re.emplace(x.first, x.second.fragment_size);
    }
    return re;
  }

  void clear() {
    regions.clear();
    layers.clear();
  }
}; // struct MemoryMap

namespace layout {

struct MemorySection final {
  std::string name = ""; // Tensor information

  void* address = nullptr;
  size_t size = 0;

  bool allocated = false;
}; // struct MemorySection

struct Block final {
  std::map<void*, MemorySection> sections;

  Block(void* address, size_t size) {
    MemorySection s;
    s.address = address;
    s.size = size;
    sections.emplace(s.address, s);
  }
}; // struct Block

struct MemoryLayout final {
 private:
  std::map<void*, Block> blocks;

  std::shared_mutex m;

  size_t device_size;
  size_t block_size;
  size_t align_size;

 protected:
  inline std::map<void*, Block>::iterator locateMemoryBlock(void* address) {
    auto bp = blocks.upper_bound(address);
    if (bp == blocks.begin())
      return blocks.end();
    return --bp;
  }

 public:
  MemoryLayout() = default;

  inline void setMemoryInfo(const MemoryInfo& info) {
    device_size = info.device.total_size;
    block_size = info.device.block_size;
    align_size = info.device.align_size;
  }

  inline bool isSectionExist(void* address) {
    std::shared_lock<std::shared_mutex> l{m};
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      return false;
    auto& sections = bp->second.sections;
    return sections.find(address) != sections.end();
  }
  inline MemorySection getMemorySection(void* address) {
    std::shared_lock<std::shared_mutex> l{m};
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_unmanaged();
    auto& sections = bp->second.sections;
    auto sp = sections.find(address);
    if (sp == sections.end())
      throw memory_unmanaged();
    return sp->second;
  }

  inline void recordMemoryAllocateEvent(
      void* address,
      size_t size,
      const std::string& tensor,
      size_t alignment) {
    // Since MemoryLayout is only a recorder of memory layout information, no
    // need to implement for malloc and salloc seperately.
    if (size == 0) {
      recordMemoryAllocateEvent(address, alignment, tensor, alignment);
      return;
    }

    std::unique_lock<std::shared_mutex> l{m};

    auto bp = blocks.upper_bound(address);
    if (bp == blocks.begin())
      bp = blocks.emplace(address, Block(address, block_size)).first;
    else {
      --bp;
      if (utils::address_offset(bp->first, block_size) <= address)
        bp = blocks.emplace(address, Block(address, block_size)).first;
    }

    auto& sections = bp->second.sections;
    auto p = sections.begin();
    while (p != sections.end() &&
           utils::address_offset(p->first, p->second.size) <= address)
      ++p;
    if (p == sections.end() || p->first > address || p->second.allocated)
      throw memory_allocated(address);
    if (utils::address_offset(p->first, p->second.size) <
        utils::address_offset(address, size))
      throw memory_operation_invalid(
          address,
          "Memory cannot be allocated at specificied address with size.");

    // The original unallocated space should be splited to three parts.
    if (p->first < address) {
      // Left part exists.
      MemorySection s;
      s.address = address;
      s.size = (uint8_t*)p->first - (uint8_t*)address + p->second.size;
      auto q = sections.emplace(address, s);
      assert(q.second);
      p->second.size = (uint8_t*)address - (uint8_t*)p->first;
      p = q.first;
    }
    // Now p->first == address
    if (p->second.size > size) {
      // Right part exists.
      // Create empty section
      MemorySection s;
      s.address = (uint8_t*)address + size;
      s.size = p->second.size - size;
      auto q = sections.emplace(s.address, s);
      assert(q.second);
      p->second.size = size;
    }
    p->second.name = tensor;
    p->second.allocated = true;
  }
  inline void recordMemoryAllocateEvent(
      void* address,
      size_t size,
      const std::string& tensor) {
    if (!utils::memory_address_aligned(address, align_size))
      throw memory_exception(address, "Memory address not aligned.");
    size_t aligned_size = utils::get_memory_aligned_size(size, align_size);
    if (aligned_size == 0)
      aligned_size = align_size;
    recordMemoryAllocateEvent(address, aligned_size, tensor, align_size);
  }
  inline void recordMemoryFreeEvent(
      void* address,
      const std::string& tensor = "") {
    std::unique_lock<std::shared_mutex> l{m};

    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_not_allocated(address);

    auto& sections = bp->second.sections;
    // Check if allocated device memory.
    auto p = sections.find(address);
    // Device memory not allocated.
    if (p == sections.end() || !p->second.allocated)
      throw memory_not_allocated(address);
    p->second.name = "";
    p->second.allocated = false;

    // Merging free sections.
    auto prev = p;
    auto post = p;
    ++post;
    if (post != sections.end() && !post->second.allocated) {
      p->second.size += post->second.size;
      sections.erase(post);
    }

    if (p == sections.begin())
      return;
    --prev;
    if (!prev->second.allocated) {
      prev->second.size += p->second.size;
      sections.erase(p);
    }
  }
  inline void recordMemorySplitEvent(void* address, size_t size) {
    std::unique_lock<std::shared_mutex> l{m};

    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_not_allocated(address);

    auto& sections = bp->second.sections;
    auto p = sections.find(address);
    if (p == sections.end() || !p->second.allocated)
      throw memory_not_allocated(address);
    if (p->second.size <= size)
      throw memory_operation_invalid(
          address, "Memory section equals or be smaller than spliting size.");

    MemorySection s = p->second;
    s.address = utils::address_offset(address, size);
    s.size -= size;
    sections.emplace(s.address, s);
    p->second.size = size;
  }
  inline void recordMemoryMergeEvent(void* left, void* right) {
    std::unique_lock<std::shared_mutex> l{m};

    auto bp = locateMemoryBlock(left);
    if (bp == blocks.end())
      throw memory_not_allocated(left);

    auto& sections = bp->second.sections;
    auto q = sections.find(left);
    if (q == sections.end() || !q->second.allocated)
      throw memory_not_allocated(
          left, "Memory for left section not allocated.");
    auto p = q++;
    if (q == sections.end() || q->first != right || !q->second.allocated)
      throw memory_not_allocated(
          right, "Memory for right section not allocated.");
    if ((uint8_t*)left + p->second.size != (uint8_t*)right)
      throw memory_operation_invalid(left, "Memory sections not continuous.");

    p->second.size += q->second.size;
    sections.erase(q);
  }

}; // struct MemoryLayout

} // namespace layout
} // namespace mori

namespace mori {
namespace events {

enum class ScheduleEventType {
  allocate,
  copyin,
  copyout,
  swapin,
  swapout,
  freedev,
  freehost,
  free
}; // enum class ScheduleEventType

struct ScheduleEvent final {
  std::string operator_name = "";
  std::string tensor_name = "";
  size_t size = 0;

  ScheduleEventType type = ScheduleEventType::allocate;
  std::string postop = ""; // For execution-triggered events, the event should
                           // be executed after executing postop.
  int timepoint = 0; // For timepoing-triggered events, the event should be
                     // executed after specificied timepoint.

  ScheduleEvent() = default;
  ScheduleEvent(
      const std::string& _op_name,
      const std::string& _tensor_name,
      size_t _size)
      : operator_name(_op_name), tensor_name(_tensor_name), size(_size) {}
  ScheduleEvent(
      const std::string& _op_name,
      const std::string& _tensor_name,
      size_t _size,
      ScheduleEventType _event_type,
      const std::string& _postop)
      : operator_name(_op_name),
        tensor_name(_tensor_name),
        size(_size),
        type(_event_type),
        postop(_postop) {}
  ScheduleEvent(
      const std::string& _op_name,
      const std::string& _tensor_name,
      size_t _size,
      ScheduleEventType _event_type,
      int _timepoint)
      : operator_name(_op_name),
        tensor_name(_tensor_name),
        size(_size),
        type(_event_type),
        timepoint(_timepoint) {}
}; // struct ScheduleEvent

struct StageScheduleEvents {
  std::vector<ScheduleEvent> execution;
  std::vector<ScheduleEvent> timepoint;
}; // struct StageScheduleEvents

struct ScheduleEvents {
  MemoryMap memory_map;
  StageScheduleEvents forward_schedule_events;
  StageScheduleEvents backward_schedule_events;
}; // struct ScheduleEvents

} // namespace events
} // namespace mori

#include <algorithm>
#include <cassert>
#include <deque>
#include <map>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mori {
namespace status {

enum MemoryDataType {
  all,
  inout,
  weight,
  workspace,
  constant
}; // enum MemoryType

enum MemoryStatusType {
  none,
  empty,
  device,
  host,
  coexist,
  swapin,
  swapout
}; // enum MemoryDataStatusType

struct Tensor;

/**
 * Describe memory data section on the specific computing acclerating device.
 * A tensor consists of a series of memory data sections.
 */
struct MemorySection final {
 private:
  friend struct Tensor;

 private:
  MemorySection* prev_sect = nullptr;
  MemorySection* post_sect = nullptr;

 public:
  size_t offset = 0;
  size_t size = 0;

  void* host_address = nullptr;
  void* device_address = nullptr;

  MemoryStatusType status = none;

 public:
  MemorySection() = default;
  MemorySection(
      size_t _offset,
      size_t _size,
      void* _host_address,
      void* _device_address,
      MemoryStatusType _status)
      : offset(_offset),
        size(_size),
        host_address(_host_address),
        device_address(_device_address),
        status(_status) {}
  MemorySection(const MemorySection&) = default;
  MemorySection(MemorySection&&) = default;
  MemorySection& operator=(const MemorySection&) = default;
  MemorySection& operator=(MemorySection&&) = default;

  inline bool hasPrev() const {
    return prev_sect != nullptr;
  }
  inline MemorySection* prev() {
    return prev_sect;
  }
  inline const MemorySection* prev() const {
    return prev_sect;
  }

  inline bool hasPost() const {
    return post_sect != nullptr;
  }
  inline MemorySection* next() {
    return post_sect;
  }
  inline const MemorySection* next() const {
    return post_sect;
  }

  ~MemorySection() = default;
};

/**
 * Fragment is a simplified MemorySection
 */
struct Fragment final {
  size_t size = 0;
  void* address = nullptr;
  MemoryStatusType status = none;

  Fragment() = default;
  Fragment(
      size_t _size,
      void* _address,
      MemoryStatusType _status = MemoryStatusType::none)
      : size(_size), address(_address), status(_status) {}
}; // struct Fragment

struct TensorPres;
struct MemoryStatus;

/**
 * TensorStatus
 * Status of a tensor which consists of a series of data sections.
 */
struct Tensor final {
 private:
  friend struct TensorPres;
  friend struct MemoryStatus;

 private:
  std::string name = "";

  // Tensor memory region consists of a series of data sections.
  // Key is the offset of the section, value is the corresponding memory status.
  // When the tensor is fulfilly on device, the sections should be continuous.
  std::map<size_t, MemorySection> sections;
  Fragment fragment;

  // Tensor size
  size_t size = 0;
  // Remaining tensor size in memory
  size_t device_size = 0;
  size_t host_size = 0;
  MemoryDataType type = all;

  // Indicating if the tensor should be considered in swapping.
  bool persistant = false;
  bool transient = false;

  std::string op = "";

 public:
  Tensor() {
    sections.emplace(0, MemorySection());
  }
  Tensor(const std::string& _name) : Tensor() {
    name = _name;
  }
  Tensor(const std::string& _name, size_t _size)
      : name(_name), size(_size), device_size(_size), host_size(0) {
    sections.emplace(0, MemorySection{0, _size, nullptr, nullptr, none});
    // if (_size < 1048576 * 4) transient = true;
  }
  Tensor(const std::string& _name, size_t _size, MemoryDataType _type)
      : Tensor(_name, _size) {
    type = _type;
    if (_type == MemoryDataType::constant || _type == MemoryDataType::weight)
      persistant = true;
    if (_type == MemoryDataType::workspace)
      transient = true;
  }
  Tensor(const Tensor& _status) = default;
  Tensor(Tensor&& _status) = default;

  Tensor& operator=(const Tensor& _status) = default;
  Tensor& operator=(Tensor&& _status) = default;

  inline void setName(const std::string& _name) {
    name = _name;
  }
  inline void setType(MemoryDataType _type) {
    type = _type;
  }
  inline void setSize(size_t _size) {
    size = _size;
    sections[0].size = _size;
  }
  inline void setPersistant(bool _persistant) {
    persistant = _persistant;
  }
  inline void setTransient(bool _transient) {
    transient = _transient;
  }

  inline std::string getName() const noexcept {
    return name;
  }
  inline std::string getOperatorName() const noexcept {
    return op;
  }
  inline size_t getSize() const noexcept {
    return size;
  }
  inline size_t getDeviceSize() const noexcept {
    return device_size;
  }
  inline size_t getHostSize() const noexcept {
    return host_size;
  }
  inline MemoryDataType getType() const noexcept {
    return type;
  }
  inline bool isPersistant() const noexcept {
    return persistant;
  }
  inline bool isTransient() const noexcept {
    return transient;
  }

  inline const MemorySection& getSection(size_t offset) const {
    return sections.at(offset);
  }
  inline int getSectionCount() const noexcept {
    return sections.size();
  }
  inline const MemorySection& getFirstSection() const {
    return sections.begin()->second;
  }
  inline const MemorySection& getLastSection() const {
    return sections.rbegin()->second;
  }

  std::vector<size_t> getSections() const {
    std::vector<size_t> re;
    for (auto& x : sections)
      re.push_back(x.first);
    return re;
  }

  inline bool isSectionExist(size_t offset) const noexcept {
    return sections.find(offset) != sections.end();
  }

  /**
   * If tensor has data located on device.
   */
  bool isDeviceLocated() const noexcept {
    for (auto& x : sections) {
      if (x.second.status == empty || x.second.status == device ||
          x.second.status == coexist)
        return true;
    }
    return false;
  }
  /**
   * If tensor has all data located on device.
   */
  bool isDeviceAllLoacted() const noexcept {
    for (auto& x : sections) {
      if (x.second.status == none || x.second.status == host)
        return false;
    }
    return true;
  }

  void split(size_t offset, size_t size) {
    assert(size != 0);
    MemorySection& memory_section = sections.at(offset);

    if (memory_section.size < size) {
      throw memory_section_invalid("Sectioning size larger than section size.");
    }
    if (memory_section.size == size)
      return;

    MemorySection new_section;
    new_section.offset = memory_section.offset + size;
    new_section.size = memory_section.size - size;
    if (memory_section.host_address != nullptr)
      new_section.host_address = (uint8_t*)memory_section.host_address + size;
    if (memory_section.device_address != nullptr)
      new_section.device_address =
          (uint8_t*)memory_section.device_address + size;
    new_section.status = memory_section.status;
    new_section.prev_sect = &memory_section;
    new_section.post_sect = memory_section.post_sect;

    memory_section.size = size;

    sections.emplace(offset + size, new_section);
    memory_section.post_sect = &(sections.at(offset + size));
  }

  inline bool isMergeable(size_t offset) {
    auto pb = sections.find(offset);
    if (pb == sections.end())
      throw memory_section_invalid("Invalid section offset.");
    MemorySection& memory_section = pb->second;
    auto pe = ++pb;

    if (pe == sections.end())
      return false;
    if (pe->second.status != memory_section.status)
      return false;
    if (memory_section.status == MemoryStatusType::host ||
        memory_section.status == MemoryStatusType::coexist)
      return false;

    assert(
        (uint8_t*)memory_section.device_address + memory_section.size ==
        pe->second.device_address);
    return true;
  }

  MemorySection& merge(size_t offset = 0) {
    if (!isMergeable(offset))
      throw memory_section_invalid("Invalid section merging.");

    MemorySection& memory_section = sections.at(offset);
    MemorySection& post_section = *memory_section.post_sect;
    size_t post_offset = post_section.offset;

    memory_section.size += post_section.size;
    memory_section.post_sect = post_section.post_sect;
    if (memory_section.post_sect != nullptr)
      memory_section.post_sect->prev_sect = &memory_section;
    sections.erase(post_offset);

    return memory_section;
  }

  void setReshaped(size_t _size) {
    // Since the allocation takes place in the beginning of the application
    // procedure, there should be only one memory section.
    if (sections.size() != 1)
      throw status_exception("Set allocated for sectioned tensor.");
    assert(sections.begin()->first == 0);
    assert(sections.begin()->second.offset == 0);
    size = _size;
    sections.begin()->second.size = size;
    if (sections[0].status != MemoryStatusType::none)
      throw status_exception("Set reshaped for allocated tensor.");
    device_size = size;
  }
  void setAllocated(void* device_address) {
    // Since the allocation takes place in the beginning of the application
    // procedure, there should be only one memory section.
    if (sections.size() != 1)
      throw status_exception("Set allocated for sectioned tensor.");
    assert(sections.begin()->first == 0);
    assert(sections.begin()->second.offset == 0);
    assert(sections.begin()->second.size == size);
    if (sections[0].status != MemoryStatusType::none)
      throw status_exception("Set allocated for allocated tensor.");
    sections[0].device_address = device_address;
    sections[0].status = MemoryStatusType::empty;
    device_size = size;
  }
  void setAssigned() {
    for (auto& x : sections) {
      switch (x.second.status) {
        case empty:
          x.second.status = MemoryStatusType::device;
        case device:
          break;
        case coexist:
          throw status_exception("Accessing data not released on host.");
        default:
          throw status_exception("Accessing data not on device.");
      }
    }
  }
  void setAcquired() {
    for (auto& x : sections) {
      switch (x.second.status) {
        case coexist:
        case device:
        case empty:
          break;
        default:
          throw status_exception("Acquiring data not on device.");
      }
    }
  }
  void setAccessed() {
    setAssigned();
  }
  void setCopiedOut(size_t offset, void* host_address) {
    MemorySection& memory_section = sections.at(offset);
    memory_section.host_address = host_address;
    switch (memory_section.status) {
      case device:
        memory_section.status = MemoryStatusType::coexist;
        host_size += memory_section.size;
      case coexist:
      case empty:
        break;
      default: // none host
        throw status_exception(
            "No data on device while copying out memory data.");
    }
  }
  void setCopiedOut(void* host_address) {
    if (sections.size() != 1)
      throw status_exception("Set copied out for sectioned tensor.");
    assert(sections.begin()->first == 0);
    assert(sections.begin()->second.offset == 0);
    assert(sections.begin()->second.size == size);
    setCopiedOut(0, host_address);
  }
  void setCopiedIn(size_t offset, void* device_address) {
    MemorySection& memory_section = sections.at(offset);
    memory_section.device_address = device_address;
    switch (memory_section.status) {
      case none:
        memory_section.status = MemoryStatusType::empty;
        break;
      case host:
        memory_section.status = MemoryStatusType::coexist;
      case coexist:
        break;
      default: // device empty
        throw status_exception("No data on host while copying in memory data.");
        break;
    }
    device_size += memory_section.size;
  }
  void setCopiedIn(void* device_address) {
    if (sections.size() != 1)
      throw status_exception("Set copied in for sectioned tensor.");
    assert(sections.begin()->first == 0);
    assert(sections.begin()->second.offset == 0);
    assert(sections.begin()->second.size == size);
    setCopiedIn(0, device_address);
  }
  void setMoved(size_t offset, void* dst_address) {
    MemorySection& memory_section = sections.at(offset);
    memory_section.device_address = dst_address;
    switch (memory_section.status) {
      case empty:
      case device:
      case coexist:
        break;
      default: // device none
        throw status_exception("No data on device while moving memory data.");
        break;
    }
  }
  void setHostFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case coexist:
        memory_section.status = device;
        break;
      case host:
        memory_section.status = none;
        break;
      default: // none empty device
        throw status_exception("No data on host while freeing host memory.");
    }
    host_size -= memory_section.size;
  }
  void setDeviceFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case coexist:
        memory_section.status = host;
        break;
      case empty:
      case device:
        memory_section.status = none;
        break;
      default: // none host
        throw status_exception("No data on host while freeing host memory.");
    }

    device_size -= memory_section.size;
  }
  void setFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case coexist:
        device_size -= memory_section.size;
        host_size -= memory_section.size;
        break;
      case empty:
      case device:
        device_size -= memory_section.size;
        break;
      case host:
        host_size -= memory_section.size;
        break;
      default: // none
        throw status_exception(
            "No data on host and device while freeing memory.");
    }
    memory_section.status = none;
  }

  inline bool hasFragment() const noexcept {
    return fragment.size != 0;
  }
  inline const Fragment& getFragment() const noexcept {
    return fragment;
  }
  inline void setFragment(size_t _size) {
    if (fragment.status != none)
      throw status_exception("Setting existed fragment size.");
    fragment.size = _size;
  }

  void setFragmentPlaced(void* address) {
    if (fragment.status != none)
      throw status_exception("Placing existed fragment.");
    fragment.status = MemoryStatusType::empty;
    fragment.address = address;
  }

  inline void setFragmentPlaced() {
    setFragmentPlaced((uint8_t*)(sections.at(0).device_address) + size);
  }

  void setFragmentRemoved() {
    if (fragment.status == none)
      throw status_exception("Removing non-exist fragment.");
    fragment.status = MemoryStatusType::none;
  }

  ~Tensor() = default;

}; // struct TensorStatus

struct OperatorPres;

/**
 * Operator
 * Memory status of an operator.
 */
struct Operator final {
 private:
  friend struct OperatorPres;

 private:
  // Operator name.
  std::string name = "";
  // Prev and post dependencies.
  std::unordered_set<std::string> prevs, posts;

  // Tensors consisting of this operator.
  std::unordered_set<std::string> tensors;

  // std::shared_mutex m;

  // Indicate if the operator is a backward propagation operator.
  bool backward_propagation = false;

 public:
  Operator() = default;
  Operator(const std::string& _name) : name(_name) {}
  Operator(const Operator& _op) {
    name = _op.name;
    prevs = _op.prevs;
    posts = _op.posts;
    tensors = _op.tensors;
  }
  Operator(Operator&& _op) {
    name = std::move(_op.name);
    prevs = std::move(_op.prevs);
    posts = std::move(_op.posts);
    tensors = std::move(_op.tensors);
  }

  Operator& operator=(const Operator& _op) {
    name = _op.name;
    prevs = _op.prevs;
    posts = _op.posts;
    tensors = _op.tensors;
    return *this;
  }
  Operator& operator=(Operator&& _op) {
    name = std::move(_op.name);
    prevs = std::move(_op.prevs);
    posts = std::move(_op.posts);
    tensors = std::move(_op.tensors);
    return *this;
  }

  inline bool isBackwardPropagation() const noexcept {
    return backward_propagation;
  }
  inline void setBackwardPropagation(bool _backward_propagation) noexcept {
    backward_propagation = _backward_propagation;
  }

  inline void setPrev(const std::string& op) {
    prevs.insert(op);
  }
  template <typename T>
  inline void setPrevs(const T& ops) {
    prevs.insert(begin(ops), end(ops));
  }
  inline void setPost(const std::string& op) {
    posts.insert(op);
  }
  template <typename T>
  inline void setPosts(const T& ops) {
    posts.insert(begin(ops), end(ops));
  }

  inline bool isPrev(const std::string& op) const {
    return prevs.find(op) != prevs.end();
  }
  inline bool isPost(const std::string& op) const {
    return posts.find(op) != posts.end();
  }

  inline std::unordered_set<std::string> getPrevs() const noexcept {
    return prevs;
  }
  inline std::unordered_set<std::string> getPosts() const noexcept {
    return posts;
  }

  void removePrev(const std::string& op) {
    auto p = prevs.find(op);
    if (p == prevs.end())
      return;
    prevs.erase(op);
  }

  void removePost(const std::string& op) {
    auto p = posts.find(op);
    if (p == posts.end())
      return;
    posts.erase(op);
  }

  inline void clearPrevs() noexcept {
    prevs.clear();
  }
  inline void clearPosts() noexcept {
    posts.clear();
  }

  inline void setTensor(const std::string& tensor) {
    tensors.insert(tensor);
  }
  template <typename T>
  void setTensors(const T& _tensors) {
    for (auto& s : _tensors)
      tensors.insert(s);
  }

  bool isTensorIncluded(const std::string& tensor) const {
    return tensors.find(tensor) != tensors.end();
  }
  const std::unordered_set<std::string> getTensors() const noexcept {
    return tensors;
  }

  void removeTensor(const std::string& tensor) {
    auto p = tensors.find(tensor);
    if (p == tensors.end())
      return;
    tensors.erase(tensor);
  }

  inline void clearTensors() noexcept {
    tensors.clear();
  }

  inline void setName(const std::string& _name) noexcept {
    name = _name;
  }
  inline std::string getName() const noexcept {
    return name;
  }

  ~Operator() = default;

}; // struct Operator

struct MemoryStatus;

struct TensorPres final {
 private:
  friend struct MemoryStatus;

 public:
  using target_type = Tensor;

 private:
  Tensor& status;
  std::unique_lock<std::shared_mutex> l;

  TensorPres(Tensor& _status, std::shared_mutex& m) : status(_status) {
    l = std::unique_lock<std::shared_mutex>{m, std::try_to_lock};
  }

 public:
  TensorPres(TensorPres&& _pres) : status(_pres.status) {
    l = std::move(_pres.l);
  }

  inline bool isReferenced() const noexcept {
    return l.owns_lock();
  }
  inline void reference() {
    l.lock();
  }

 public:
  inline void setReshaped(size_t size) {
    status.setReshaped(size);
  }
  inline void setAllocated(void* device_address) {
    status.setAllocated(device_address);
  }

  inline void setAssigned() {
    status.setAssigned();
  }
  inline void setAcquired() {
    status.setAcquired();
  }
  inline void setAccessed() {
    status.setAccessed();
  }

  inline void setCopiedOut(size_t offset, void* host_address) {
    status.setCopiedOut(offset, host_address);
  }
  inline void setCopiedOut(void* host_address) {
    status.setCopiedOut(host_address);
  }
  inline void setCopiedIn(size_t offset, void* device_address) {
    status.setCopiedIn(offset, device_address);
  }
  inline void setCopiedIn(void* device_address) {
    status.setCopiedIn(device_address);
  }
  inline void setMoved(size_t offset, void* dst_address) {
    status.setMoved(offset, dst_address);
  }
  inline void setHostFreed(size_t offset = 0) {
    status.setHostFreed(offset);
  }
  inline void setDeviceFreed(size_t offset = 0) {
    status.setDeviceFreed(offset);
  }
  inline void setFreed(size_t offset = 0) {
    status.setFreed(offset);
  }

  inline std::string getName() const noexcept {
    return status.getName();
  }
  inline std::string getOperatorName() const noexcept {
    return status.getOperatorName();
  }
  inline size_t getSize() const noexcept {
    return status.getSize();
  }
  inline size_t getDeviceSize() const noexcept {
    return status.getDeviceSize();
  }
  inline size_t getHostSize() const noexcept {
    return status.getHostSize();
  }
  inline MemoryDataType getType() const noexcept {
    return status.getType();
  }
  inline bool isPersistant() const noexcept {
    return status.isPersistant();
  }
  inline bool isTransient() const noexcept {
    return status.isTransient();
  }

  inline const MemorySection& getSection(size_t offset) const {
    return status.getSection(offset);
  }
  inline int getSectionCount() const noexcept {
    return status.getSectionCount();
  }
  inline const MemorySection& getFirstSection() const {
    return status.getFirstSection();
  }
  inline const MemorySection& getLastSection() const {
    return status.getLastSection();
  }

  inline bool isSectionExist(size_t offset) const noexcept {
    return status.isSectionExist(offset);
  }

  inline bool isDeviceLocated() const noexcept {
    return status.isDeviceLocated();
  }
  inline bool isDeviceAllLocated() const noexcept {
    return status.isDeviceAllLoacted();
  }

  inline Tensor& get() noexcept {
    return status;
  }

  inline void split(size_t offset, size_t size) {
    status.split(offset, size);
  }
  inline bool isMergeable(size_t offset) const {
    return status.isMergeable(offset);
  }
  inline MemorySection& merge(size_t offset = 0) {
    return status.merge(offset);
  }

  inline bool hasFragment() const noexcept {
    return status.hasFragment();
  }
  inline const Fragment& getFragment() const noexcept {
    return status.getFragment();
  }
  inline void setFragment(size_t _size) {
    status.setFragment(_size);
  }

  inline void setFragmentPlaced(void* address) {
    status.setFragmentPlaced(address);
  }
  inline void setFragmentPlaced() {
    status.setFragmentPlaced();
  }
  inline void setFragmentRemoved() {
    status.setFragmentRemoved();
  }

  inline void release() {
    l.unlock();
  }

  ~TensorPres() {
    if (l.owns_lock())
      release();
  }
}; // struct TensorPres

struct OperatorPres final {
 private:
  friend struct MemoryStatus;

 public:
  using target_type = Operator;

 private:
  Operator& status;
  // Operator is read-only during DL processing.
  std::unique_lock<std::shared_mutex> l;

  OperatorPres(Operator& _status, std::shared_mutex& m) : status(_status) {
    l = std::unique_lock<std::shared_mutex>(m, std::try_to_lock);
  }

 public:
  OperatorPres(OperatorPres&& _pres) : status(_pres.status) {
    l = std::move(_pres.l);
  }

  inline bool isReferenced() const noexcept {
    return l.owns_lock();
  }
  inline void reference() {
    l.lock();
  }

 public:
  inline std::string getName() const noexcept {
    return status.getName();
  }
  inline std::unordered_set<std::string> getPrevs() const noexcept {
    return status.getPrevs();
  }
  inline std::unordered_set<std::string> getPosts() const noexcept {
    return status.getPosts();
  }
  inline std::unordered_set<std::string> getTensors() const noexcept {
    return status.getTensors();
  }

  inline bool isBackwardPropagation() const noexcept {
    return status.isBackwardPropagation();
  }

  inline Operator& get() noexcept {
    return status;
  }
}; // struct OperatorPres

/**
 * MemoryStatus
 * Storage of tensor status and corresponding operator status.
 */
struct MemoryStatus final {
 private:
  template <typename T>
  struct View final {
    T pres;

    View(typename T::target_type& _target, std::shared_mutex& m)
        : pres(_target, m) {}

    inline bool isReferenced() {
      return pres.isReferenced();
    }
    inline T&& reference() {
      if (!pres.isReferenced())
        pres.reference();
      return std::move(pres);
    }
  }; // inner struct View

  template <typename T>
  struct Hold {
    T target;
    std::shared_mutex m;
    Hold(const T& _target) : target(_target) {}
    Hold(T&& _target) : target(_target) {}
    Hold(const Hold& _hold) : target(_hold.target) {}
    Hold(Hold&&) = default;

    Hold& operator=(const Hold& _hold) {
      target = _hold.target;
      return *this;
    }
    Hold& operator=(Hold&& _hold) = default;
  }; // inner struct Hold

 public:
  using TensorView = View<TensorPres>;
  using OperatorView = View<OperatorPres>;

 private:
  std::unordered_map<std::string, Hold<Tensor>> tensor_statuses;
  std::unordered_map<std::string, Hold<Operator>> operator_statuses;
  std::vector<std::string> execution_order;
  std::string operator_entry = "";

  // Protect the status map.
  // std::shared_mutex tm, om;

  MemoryInfo memory_info;

 public:
  MemoryStatus() = default;
  MemoryStatus(const MemoryStatus& _status) = default;
  MemoryStatus(MemoryStatus&& _status) {
    // std::unique_lock<std::shared_mutex> tl{_status.tm};
    // std::unique_lock<std::shared_mutex> ol{_status.om};
    tensor_statuses = std::move(_status.tensor_statuses);
    operator_statuses = std::move(_status.operator_statuses);
    execution_order = std::move(_status.execution_order);
    operator_entry = std::move(_status.operator_entry);
    memory_info = _status.memory_info;
  }

  MemoryStatus& operator=(const MemoryStatus& _status) = default;
  MemoryStatus& operator=(MemoryStatus&& _status) {
    // std::unique_lock<std::shared_mutex> tl{_status.tm};
    // std::unique_lock<std::shared_mutex> ol{_status.om};
    tensor_statuses = std::move(_status.tensor_statuses);
    operator_statuses = std::move(_status.operator_statuses);
    execution_order = std::move(_status.execution_order);
    operator_entry = std::move(_status.operator_entry);
    memory_info = _status.memory_info;
    return *this;
  }

  void setMemoryInfo(const MemoryInfo& _memory_info) {
    memory_info = _memory_info;
  }
  MemoryInfo getMemoryInfo() const {
    return memory_info;
  }

  /**
   * registerTensor
   * Register a tensor to the storage.
   * Only can be invoked when tensor status storage not inited.
   * @param status tensorStatus
   */
  void registerTensor(const Tensor& status) {
    // std::unique_lock<std::shared_mutex> l{tm};

    auto p = tensor_statuses.find(status.getName());
    if (p != tensor_statuses.end())
      throw status_exception("Tensor already registered.");
    tensor_statuses.emplace(status.getName(), status);
  }

  /**
   * registerTensor
   * Register a tensor to the storage, whose status information is empty.
   * Only can be invoked when tensor status storage not inited.
   * @param tensor tensor name
   */
  void registerTensor(const std::string& tensor) {
    // std::unique_lock<std::shared_mutex> l{tm};

    auto p = tensor_statuses.find(tensor);
    if (p != tensor_statuses.end())
      throw status_exception("Tensor already registered.");
    tensor_statuses.emplace(tensor, Tensor(tensor));
  }

  /**
   * registerOperator
   * Register an operator to the storage.
   * An operator should be always registered later than its tensors,
   * since the status storage would check the validity of the tensors included
   * in the operator.
   * @param status operator status
   */
  void registerOperator(const Operator& status) {
    // std::unique_lock<std::shared_mutex> l{om};

    auto p = operator_statuses.find(status.getName());
    if (p != operator_statuses.end())
      throw status_exception("Operator already registered.");

    for (auto& s : status.getTensors()) {
      if (tensor_statuses.find(s) == tensor_statuses.end())
        // Tensor not registered.
        throw status_exception("Specified tensor not registered.");
      // Set operator information for tensor.
      tensor_statuses.at(s).target.op = status.getName();
    }

    for (auto& s : status.getPrevs())
      if (operator_statuses.find(s) == operator_statuses.end())
        // Operator not registered.
        throw status_exception("Specified prev operator not registered.");

    operator_statuses.emplace(status.getName(), status);
    execution_order.push_back(status.getName());
  }

  void setEntry(const std::string& _op) {
    auto p = operator_statuses.find(_op);
    if (p == operator_statuses.end())
      throw status_exception("Operator not registered.");
    operator_entry = _op;
  }

  inline std::string getEntry() const noexcept {
    return operator_entry;
  }

  inline std::vector<std::string> getExecutionOrder() const noexcept {
    return execution_order;
  }

  inline std::string getExecutionPost(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    if (++p == execution_order.end())
      return "";
    return *p;
  }

  inline std::string getExecutionPrev(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    if (p-- == execution_order.begin())
      return "";
    return *p;
  }

  template <typename T>
  inline void setExecutionOrder(const T& _execution_order) {
    std::vector<std::string> vect(
        begin(_execution_order), end(_execution_order));
    execution_order.swap(vect);
  }

  /**
   * isTensorRegistered
   * Check if the tensor is registered to the storage.
   * @param tensor tensor name
   * @return if the tensor is registered
   */
  bool isTensorRegistered(const std::string& tensor) const {
    return tensor_statuses.find(tensor) != tensor_statuses.end();
  }

  /**
   * isOperatorRegistered
   * Check if the operator is registered to the storage.
   * @param op operator name
   * @return if the operator is registered
   */
  bool isOperatorRegistered(const std::string& op) const {
    return operator_statuses.find(op) != operator_statuses.end();
  }

  TensorView tryReferenceTensor(const std::string& tensor) {
    auto p = tensor_statuses.find(tensor);
    if (p == tensor_statuses.end())
      throw status_exception("Tensor not registered.");
    return TensorView(p->second.target, p->second.m);
  }

  /**
   * Reference the tensor
   * @param tensor tensor name
   * @return reference to the specific tensor
   */
  inline TensorPres referenceTensor(const std::string& tensor) {
    return tryReferenceTensor(tensor).reference();
  }

  OperatorView tryReferenceOperator(const std::string& op) {
    auto p = operator_statuses.find(op);
    if (p == operator_statuses.end())
      throw status_exception("Operator not registered.");
    return OperatorView(p->second.target, p->second.m);
  }

  inline OperatorPres referenceOperator(const std::string& op) {
    return tryReferenceOperator(op).reference();
  }

  inline std::unordered_set<std::string> getTensors() {
    std::unordered_set<std::string> re;
    for (auto& x : tensor_statuses)
      re.insert(x.first);
    return re;
  }
  inline std::unordered_set<std::string> getOperators() {
    std::unordered_set<std::string> re;
    for (auto& x : operator_statuses)
      re.insert(x.first);
    return re;
  }

  void unregisterOperator(const std::string& op) {
    // std::unique_lock<std::shared_mutex> l{om};

    auto p = operator_statuses.find(op);
    if (p == operator_statuses.end())
      throw status_exception("Operator not registered.");
    operator_statuses.erase(p);

    auto q = std::find(execution_order.begin(), execution_order.end(), op);
    assert(q != execution_order.end());
    execution_order.erase(q);
  }

  /**
   * unregisterTensor
   * Unregister a tensor from the storage.
   * Only can be invoked when tensor status storage not inited.
   * @param tensor tensor name
   */
  void unregisterTensor(const std::string& tensor) {
    // std::unique_lock<std::shared_mutex> l{tm};

    auto p = tensor_statuses.find(tensor);
    if (p == tensor_statuses.end())
      throw status_exception("Tensor not registered.");
    tensor_statuses.erase(p);
  }

  /**
   * @brief Clear all status information.
   */
  void clear() {
    tensor_statuses.clear();
    operator_statuses.clear();
  }

  ~MemoryStatus() = default;

}; // struct MemoryStatus

namespace util {

// static std::string get_tensor_type_str(MemoryDataType type) {
//     switch (type) {
//         case MemoryDataType::all:
//             return "all";
//         case MemoryDataType::constant:
//             return "constant";
//         case MemoryDataType::inout:
//             return "inout";
//         case MemoryDataType::weight:
//             return "weight";
//         case MemoryDataType::workspace:
//             return "workspace";
//     }

//     assert(0);
//     return "";
// }

} // namespace util

using TensorView = status::MemoryStatus::TensorView;
using OperatorView = status::MemoryStatus::OperatorView;

} // namespace status

using TensorPres = status::TensorPres;
using MemoryStatus = status::MemoryStatus;

} // namespace mori

namespace mori {

// Abstract struct
struct Backend {
  virtual void init() = 0;

  virtual void submitMemoryStatus(const status::MemoryStatus& status) = 0;

  virtual void start() {}

  virtual void setIteration(int _iteration) = 0;
  virtual void newIteration() = 0;
  virtual void halfIteration() = 0;

  virtual void submitEvent(const events::MemoryEvent& event) = 0;
  virtual events::ScheduleEvents getScheduleEvents() = 0;

  virtual void stop() {}

  virtual void terminate() = 0;

  virtual ~Backend(){};
}; // struct Backend

} // namespace mori

namespace mori {

struct Context final {
 public:
  struct View final {
   protected:
    friend struct Context;

   protected:
    const Context& context;
    std::string prefix;

    View(const Context& _context, const std::string& _prefix)
        : context(_context), prefix(_prefix) {}

    std::string make_target_key(const std::string& key) const {
      std::string target_key = prefix;
      if (prefix != "" && key != "")
        target_key = prefix + "." + key;
      return target_key;
    }

   public:
    const std::string& at(const std::string& key = "") const {
      return context.at(make_target_key(key));
    }
    const std::string& at() const {
      return context.at(prefix);
    }
    bool signal(const std::string& key) const {
      return context.signal(make_target_key(key));
    }
    bool isParamExists(const std::string& key) const {
      return context.isParamExists(make_target_key(key));
    }
    bool isDefaultParam(const std::string& key) const {
      return context.isDefaultParam(make_target_key(key));
    }

    View view(const std::string& _prefix) const {
      std::string target = prefix;
      if (_prefix != "") {
        target.push_back('.');
        target += _prefix;
      }

      return View(context, target);
    }
  }; // struct ContextView

 protected:
  std::unordered_map<std::string, std::string> defaults;
  std::unordered_map<std::string, std::string> contexts;

  void prepareDefaultParams() {
    defaults.emplace("path", "int://local");
    defaults.emplace("scheduler", "fifo");
    defaults.emplace("scheduler.trigger_event", "dependency");

    defaults.emplace("exporters.events", "empty");
    defaults.emplace("exporters.events.method", "empty");
    defaults.emplace("exporters.tensors", "empty");
    defaults.emplace("exporters.tensors.method", "empty");
  }

 public:
  Context() {
    prepareDefaultParams();
  }

  Context(const std::string& _path) {
    // std::ifstream fin(_path);
    // fin<<context;

    prepareDefaultParams();
  }

  Context(const Context& _context) {
    defaults = _context.defaults;
    contexts = _context.contexts;
  }

  Context(Context&& _context) {
    defaults = move(_context.defaults);
    contexts = move(_context.contexts);
  }

  void operator=(const Context& _context) {
    defaults = _context.defaults;
    contexts = _context.contexts;
  }

  void operator=(Context&& _context) {
    defaults = move(_context.defaults);
    contexts = move(_context.contexts);
  }

  std::string& at(const std::string& key) {
    auto p = contexts.find(key);
    if (p != contexts.end())
      return p->second;

    p = defaults.find(key);
    if (p != defaults.end())
      return p->second;

    throw context_missing(key);
  }

  const std::string& at(const std::string& key) const {
    auto p = contexts.find(key);
    if (p != contexts.end())
      return p->second;

    p = defaults.find(key);
    if (p != defaults.end())
      return p->second;

    throw context_missing(key);
  }

  std::string& operator[](const std::string& key) {
    auto p = contexts.find(key);
    if (p != contexts.end())
      return p->second;

    p = defaults.find(key);
    if (p != defaults.end())
      return p->second;

    return contexts[key];
  }

  bool signal(const std::string& key) const {
    return at(key) == "1" ? true : false;
  }

  bool isParamExists(const std::string& key) const {
    auto p = contexts.find(key);
    if (p != contexts.end())
      return true;

    p = defaults.find(key);
    if (p != defaults.end())
      return true;

    return false;
  }

  bool isDefaultParam(const std::string& key) const {
    auto p = contexts.find(key);
    if (p != contexts.end())
      return false;

    p = defaults.find(key);
    if (p != defaults.end())
      return true;

    return false;
  }

  View view(const std::string& prefix) const {
    return View(*this, prefix);
  }

  inline View view() const {
    return view("");
  }

  friend std::istream& operator>>(std::istream& in, const Context& context) {
    return in;
  }

  friend std::ostream& operator<<(std::ostream& out, const Context& context) {
    return out;
  }

}; // struct Context

} // namespace mori

#include <iostream>

namespace mori {

/**
 * LogLevel
 * Describe the level of the log.
 */
enum LogLevel { debug, info, warning, error }; // enum LogLevel;

/**
 * Log
 * Log Entry
 */
struct Log final {}; // struct Log

/**
 * Logger
 * Basic logger interface
 */
struct Logger {
  LogLevel default_level;
  std::string log_buffer;

  virtual void setDefaultLogLevel(LogLevel level) {
    default_level = level;
  }

  inline std::string getLogLevelStr(LogLevel level) {
    switch (level) {
      case debug:
        return "[Debug]  ";
      case info:
        return "[Info]   ";
      case warning:
        return "[Warning]";
      case error:
        return "[Error]  ";
      default:
        return "[Info]   ";
        break;
    }
  }

  virtual void submitInternal(const std::string& log) {
    log_buffer.append(log);
  }
  virtual void flush(LogLevel level) {}
  virtual void flush() {
    flush(default_level);
  }

  virtual void submit(LogLevel level, const std::string& log) {
    log_buffer.clear();
    submitInternal(log);
    flush(level);
  };
  virtual void submit(const std::string& log) {
    submit(default_level, log);
  }

  virtual Logger& operator<<(LogLevel level) {
    default_level = level;
    return *this;
  }
  virtual Logger& operator<<(const std::string& log) {
    submitInternal(log);
    return *this;
  }
}; // struct Logger

/**
 * StdIOLogger
 * Submit logs to std streams
 */
struct StdIOLogger : public Logger {
  virtual void flush(LogLevel level) {
    std::cout << getLogLevelStr(level) << " "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count()
              << " " << log_buffer << std::endl;
    log_buffer.clear();
  }

}; // struct StdIOLogger

} // namespace mori

#ifndef ENABLE_EXTERNAL_BACKEND

#ifndef ENABLE_EXTERNAL_BACKEND
// Provided for frontend, hence frontend does not need to include from backend.

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cassert>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <utility>

#include <fstream>
#include <iostream>
#include <memory>

#include <dlfcn.h>

namespace mori {
namespace utils {

template <typename T>
void* load_dylib(
    const std::string& dylib,
    const std::string& path,
    const std::string& entry,
    std::unique_ptr<T>& ptr) {
  typedef int (*EntryType)(std::unique_ptr<T>&);
  void* hInst = dlopen(path.c_str(), RTLD_LAZY);
  if (!hInst)
    throw dynamic_library_exception("Failed to open dynamic library: " + dylib);
  EntryType f = (EntryType)dlsym(hInst, entry.c_str());

  int ret;
  if (f)
    ret = f(ptr);
  else
    throw dynamic_library_exception("Failed to access entry: " + dylib);

  if (ret != 0)
    throw dynamic_library_exception("Failed to enter entry function: " + dylib);

  return hInst;
}

template <typename T>
void* load_dylib(
    const std::string& dylib,
    const std::string& path,
    const std::string& entry,
    std::unique_ptr<T>& ptr,
    const Context::View& context) {
  typedef int (*EntryType)(std::unique_ptr<T>&, const Context::View&);
  void* hInst = dlopen(path.c_str(), RTLD_LAZY);
  if (!hInst)
    throw dynamic_library_exception("Failed to open dynamic library: " + dylib);
  EntryType f = (EntryType)dlsym(hInst, entry.c_str());

  int ret;
  if (f)
    ret = f(ptr, context);
  else
    throw dynamic_library_exception("Failed to access entry: " + dylib);

  if (ret != 0)
    throw dynamic_library_exception("Failed to enter entry function: " + dylib);

  return hInst;
}

} // namespace utils
} // namespace mori

namespace mori {
namespace exporter {

namespace exportimpl {
/**
 * Implementation of export methods.
 */
struct ExportMethod {
  ExportMethod(const Context::View&) {}
  virtual void exportMessage(const std::string&) {}
  virtual ~ExportMethod() = default;
}; // struct ExportMethod

struct FileExportMethod : public ExportMethod {
  std::ofstream fout;
  FileExportMethod(const Context::View& context_view)
      : ExportMethod(context_view) {
    std::string export_file = context_view.at("filename");
    fout.open(export_file);
  }

  virtual void exportMessage(const std::string& message) override {
    fout << message << std::endl;
  }

  virtual ~FileExportMethod() {
    fout.close();
  }
}; // struct FileExportMethod

} // namespace exportimpl

using ExportMethod = exportimpl::ExportMethod;

/**
 * Export DL memory events.
 */
struct EventsExporter {
  std::unique_ptr<exportimpl::ExportMethod> export_method;
  void* hInst = nullptr;

  EventsExporter(const Context::View& context) {
    std::string export_method_name = context.at("method");
    Context::View context_view = context.view("method");
    if (export_method_name == "empty")
      export_method.reset(new exportimpl::ExportMethod(context_view));
    else if (export_method_name == "file")
      export_method.reset(new exportimpl::FileExportMethod(context_view));
    else
      hInst = utils::load_dylib(
          "Events Export Method",
          context.at("method.path"),
          "export_method_entry",
          export_method,
          context_view);
  }

  virtual void onEvent(const events::MemoryEvent& event) {}

  virtual ~EventsExporter() {
    if (hInst)
      dlclose(hInst);
  }
}; // struct EventsExporter

struct TensorsExporter {
  std::unique_ptr<exportimpl::ExportMethod> export_method;
  void* hInst = nullptr;

  TensorsExporter(const Context::View& context) {
    std::string export_method_name = context.at("method");
    Context::View context_view = context.view("method");
    if (export_method_name == "empty")
      export_method.reset(new exportimpl::ExportMethod(context_view));
    else if (export_method_name == "file")
      export_method.reset(new exportimpl::FileExportMethod(context_view));
    else
      hInst = utils::load_dylib(
          "Events Export Method",
          context.at("method.path"),
          "export_method_entry",
          export_method,
          context_view);
  }
  virtual void onTensor(const status::Tensor& tensor) {}
  virtual void onOperator(const status::Operator& operator_status) {}
  virtual void onEntry(const std::string& op) {}

  virtual ~TensorsExporter() {
    if (hInst)
      dlclose(hInst);
  }
}; // struct TensorExporter

} // namespace exporter
} // namespace mori

namespace mori {
namespace events {

struct EventSet;

struct Events final {
 private:
  friend struct EventSet;

 private:
  int iteration = 0;
  // key: iteration, value: MemoryEvent
  std::multimap<int, MemoryEvent> events;

 public:
  Events() = default;

  void submitEvent(const MemoryEvent& event) {
    events.emplace(iteration, event);
  }

  EventSet select() const;

  int getIteration() const noexcept {
    return iteration;
  }
  void setIteration(int _iteration) noexcept {
    iteration = _iteration;
  }
  void newIteration() noexcept {
    ++iteration;
  }

  ~Events() = default;

}; // struct Events

struct EventSet final {
 private:
  using event_iter = std::multimap<int, MemoryEvent>::const_iterator;

  struct Comparator final {
    bool operator()(const event_iter& p, const event_iter& q) const {
      if (p->first == q->first)
        return p->second.timestamp < q->second.timestamp;
      return p->first < q->first;
    }
  }; // struct Comparator

 public:
  using item = std::pair<int, MemoryEvent>;
  using pred = std::function<bool(const item&)>;
  using res = std::set<event_iter, Comparator>;

 private:
  const std::multimap<int, MemoryEvent>& events_base;
  std::set<event_iter, Comparator> events_cond;
  std::vector<pred> preds;

  bool first_query = true;

 public:
  EventSet(const Events& events) : events_base(events.events) {}

  EventSet(const EventSet&) = default;
  EventSet(EventSet&&) = default;

  EventSet select() {
    return *this;
  }

  EventSet& where(
      const std::function<bool(const std::pair<int, MemoryEvent>&)> f) {
    preds.push_back(f);
    return *this;
  }

  EventSet& get() {
    auto p = preds.begin();

    if (first_query) {
      assert(events_cond.empty());
      auto q = events_base.begin();
      while (q != events_base.end()) {
        if ((*p)(*q))
          events_cond.insert(q);
        ++q;
      }
      ++p;
      first_query = false;
    }

    while (p != preds.end()) {
      auto q = events_cond.begin();
      while (q != events_cond.end()) {
        if ((*p)(**q))
          ++q;
        else
          q = events_cond.erase(q);
      }
      ++p;
    }

    preds.clear();

    return *this;
  }

  const res& ref() const noexcept {
    return events_cond;
  }

  size_t size() const noexcept {
    if (first_query)
      return events_base.size();
    return events_cond.size();
  }

  void clear() {
    events_cond.clear();
    first_query = true;
  }

  ~EventSet() = default;
}; // struct EventSet

inline EventSet Events::select() const {
  return EventSet(*this);
}

/**
 * from
 * Form a from(xxx).select(xxx).where(xxx) query.
 */
static inline EventSet select_from(const EventSet& events) {
  return events;
}

static inline EventSet select_from(const Events& events) {
  return events.select();
}

} // namespace events
} // namespace mori

#include <atomic>
#include <mutex>
#include <string>
#include <vector>

#include <map>
#include <unordered_map>
#include <vector>

namespace mori {
namespace decisions {

struct Node final {
  Region& region;

  size_t lower_remaining_size =
      0; // The remaining size analyzed with lower layer nodes.
  size_t upper_remaining_size =
      0; // The remaining size analyzed with upper layer nodes.

  size_t lower_fragment_remaining_size = 0;
  size_t upper_fragment_remaining_size = 0;

  int cluster = 0;
  int lower_group =
      0; // The group while this node analyzed with lower layer nodes.
  int upper_group =
      0; // The group while this node analyzed with upper layer nodes.

  std::vector<Node*> posts;

  Node(Region& _r) : region(_r) {
    lower_remaining_size = _r.size;
    upper_remaining_size = _r.size;
  }

  void setFragment(size_t _size) {
    region.fragment_size = _size;
    lower_fragment_remaining_size = _size;
    upper_fragment_remaining_size = _size;
  }
}; //  struct Node

struct Model final {
 private:
 private:
  // std::unordered_map<std::string, Node*> tensors;
  MemoryMap memory_map;
  std::unordered_map<std::string, Node> nodes;

  std::map<int, size_t> clusters;

  int current_layer = 0;
  size_t layer_size = 0;

  // size_t smin = 1048576;  // 1 MB
  size_t smin = 16;

  size_t device_size = 0;

  bool sectioned = true;
  bool fragmented = true;
  bool analyzed = false;

 protected:
  std::pair<int, int> getClusterSizeRatio(int c1, int c2) {
    size_t s1 = clusters.at(c1);
    size_t s2 = clusters.at(c2);

    if (s1 == s2)
      return std::make_pair(1, 1);

    size_t c = s1 < s2 ? s1 : s2;
    for (int i = c; i > 0; --i) {
      if (s1 % i == 0 && s2 % i == 0) {
        s1 /= i;
        s2 /= i;
      }
    }

    // TODO: calculate cluster size ratio.
    return std::make_pair(s2, s1);
  }

 protected:
  void fillModel(status::MemoryStatus& status) {
    std::string entry = status.getEntry();
    for (auto& so : status.getExecutionOrder()) {
      status::OperatorPres op_pres = status.referenceOperator(so);
      for (auto& st : op_pres.getTensors()) {
        status::TensorPres tensor_pres = status.referenceTensor(st);
        // if (tensor_pres.isPersistant() || tensor_pres.isTransient())
        // continue; Do submit here.
        Layer& l = memory_map.getCurrentLayer();
        if (l.requested_size + tensor_pres.getSize() > l.size)
          memory_map.createLayer();
        Region r(tensor_pres.getName(), tensor_pres.getSize());
        memory_map.submitMemoryRegion(r);
        nodes.emplace(r.name, memory_map.regions.at(r.name));
      }
    }
  }

  void clustering() {
    clusters[1] = 128;
    clusters[2] = 256;
    clusters[3] = 384;
    clusters[4] = 512;
    // TODO: Cluster the nodes with K-Means++
    for (auto& x : memory_map.layers) {
      for (auto& s : x.regions) {
        // clusters[y.size] = y.size;
        // y.cluster = y.size;
        Node& n = nodes.at(s);
        switch (n.region.size) {
          case 124:
            n.cluster = 1;
            break;
          case 256:
          case 258:
            n.cluster = 2;
            break;
          case 384:
            n.cluster = 3;
            break;
          case 512:
            n.cluster = 4;
            break;
          default:
            assert(0);
            break;
        }
      }
    }
  }

  void grouping() {
    int current_group = 1;

    auto pl = memory_map.layers.rbegin();
    ++pl; // If grouping now, there must be two or more layers.
    while (pl != memory_map.layers.rend()) {
      auto pu = pl;
      --pu;

      auto& lowers = pl->regions;
      auto& uppers = pu->regions;

      auto ql = lowers.begin();
      auto qu = uppers.begin();

      while (ql != lowers.end() && qu != uppers.end()) {
        int lower_cluster = nodes.at(*ql).cluster;
        int upper_cluster = nodes.at(*qu).cluster;

        // Initial ratio.
        auto&& grouping_ratio =
            getClusterSizeRatio(lower_cluster, upper_cluster);

        int lower_count = grouping_ratio.first;
        int upper_count = grouping_ratio.second;

        int lower_count_current = 0;
        int upper_count_current = 0;

        while (ql != lowers.end() && lower_count_current < lower_count) {
          Node& nl = nodes.at(*ql);
          auto&& current_grouping_ratio =
              getClusterSizeRatio(lower_cluster, nl.cluster);
          // Adjustment of grouping.
          if (current_grouping_ratio.first != current_grouping_ratio.second) {
            lower_count = lower_count * current_grouping_ratio.second /
                current_grouping_ratio.first;
            lower_count_current = lower_count_current *
                current_grouping_ratio.second / current_grouping_ratio.first;
            lower_cluster = nl.cluster;
          }
          nl.upper_group = current_group; // Analyzing with upper layer nodes,
                                          // hence upper_group.
          ++lower_count_current;
          ++ql;
        }
        while (qu != uppers.end() && upper_count_current < upper_count) {
          Node& nu = nodes.at(*qu);
          auto&& current_grouping_ratio =
              getClusterSizeRatio(upper_cluster, nu.cluster);
          // Adjustment of grouping.
          if (current_grouping_ratio.first != current_grouping_ratio.second) {
            upper_count = upper_count * current_grouping_ratio.second /
                current_grouping_ratio.first;
            upper_count_current = upper_count_current *
                current_grouping_ratio.second / current_grouping_ratio.first;
            upper_cluster = nu.cluster;
          }
          nu.lower_group = current_group; // Analyzing with lower layer nodes,
                                          // hence lower_group.
          ++upper_count_current;
          ++qu;
        }
        ++current_group;
      }

      if (ql == lowers.end()) {
        while (qu != uppers.end())
          nodes.at(*qu++).lower_group = current_group;
        ++current_group;
      } else if (qu == uppers.end()) {
        while (ql != lowers.end())
          nodes.at(*ql++).upper_group = current_group;
        ++current_group;
      }

      ++pl;
    }
  }

  void generateFragments() {
    bool tensor_moved = true;
    while (tensor_moved) {
      tensor_moved = false;
      auto pl = memory_map.layers.rbegin();
      while (pl != memory_map.layers.rend()) {
        // Fragments exceed the memory capacity.
        if (!pl->isAccomodatable()) {
          // Check the existance of the upper layer.
          if (pl == memory_map.layers.rbegin())
            memory_map.createLayer();
          auto pu = pl;
          --pu;

          auto& lowers = pl->regions;
          auto& uppers = pu->regions;

          auto ql = lowers.begin();
          auto qu = uppers.begin();

          // Remove all the fragments in lower and upper layer, since the
          // fragments should be regenerated.
          while (ql != lowers.end()) {
            Node& nl = nodes.at(*ql);
            if (nl.region.fragment_size != 0) {
              pl->requested_size -= nl.region.fragment_size;
              nl.setFragment(0);
            }
            nl.upper_group = 0;
            ++ql;
          }
          while (qu != uppers.end()) {
            Node& nu = nodes.at(*qu);
            if (nu.region.fragment_size != 0) {
              pu->requested_size -= nu.region.fragment_size;
              nu.setFragment(0);
            }
            nu.lower_group = 0;
            nu.upper_group = 0;
            ++qu;
          }

          ql = lowers.begin();
          qu = uppers.begin();

          do {
            assert(nodes.at(lowers.back()).region.fragment_size == 0);
            qu = uppers.insert(qu, lowers.back());
            ++qu;
            pu->requested_size += nodes.at(lowers.back()).region.size;
            pl->requested_size -= nodes.at(lowers.back()).region.size;
            lowers.pop_back();
          } while (!pl->isAccomodatable());
          tensor_moved = true;

          --pl;
          continue;
        }

        if (tensor_moved)
          break;

        auto pu = pl++;
        if (pl == memory_map.layers.rend())
          break;

        auto& lowers = pl->regions;
        auto& uppers = pu->regions;

        auto ql = lowers.begin();
        auto qu = uppers.begin();

        size_t size_tl = 0;
        size_t size_tu = 0;

        while (ql != lowers.end() &&
               qu != uppers.end()) { // If ql or qu reaches the end of layer, no
                                     // need to generate a fragment.
          Node& nl = nodes.at(*ql);
          Node& nu = nodes.at(*qu);

          size_t size_tl_target = size_tl + nl.region.size;
          size_t size_tu_target =
              size_tu + nu.region.size + nu.region.fragment_size;

          if (size_tl_target == size_tu_target) {
            size_tl += nl.region.size;
            size_tu += nu.region.size + nu.region.fragment_size;
            ++ql;
            ++qu;
          } else if (size_tl_target > size_tu_target) {
            size_tu += nu.region.size + nu.region.fragment_size;
            ++qu;
          } else {
            size_tl += nl.region.size;
            size_t size_frag =
                size_tu + nu.region.size + nu.region.fragment_size - size_tl;
            if (size_frag < smin) {
              // Generate fragment.
              nl.setFragment(size_frag);
              size_tl += size_frag;
              pl->requested_size += size_frag;
            }
            ++ql;
          }
        }
      }
    }
  }

  void generateTree() {
    auto pl = memory_map.layers.begin();
    auto pu = pl;
    ++pu;

    while (pu != memory_map.layers.end()) {
      auto& lowers = pl++->regions;
      auto& uppers = pu++->regions;

      auto ql = lowers.begin();
      auto qu = uppers.begin();

      while (ql != lowers.end() &&
             qu != uppers.end()) { // If ql or qu reaches the end of layer, no
                                   // need to split the lower layer tensors.
        Node& nl = nodes.at(*ql);
        Node& nu = nodes.at(*qu);
        if (nl.upper_remaining_size > nu.lower_remaining_size) {
          // Lower layer node along with the fragment larger than upper layer
          // node.
          size_t size_sect =
              (nl.upper_remaining_size - nu.lower_remaining_size) > smin
              ? nu.lower_remaining_size
              : nl.upper_remaining_size;
          nl.region.sections.push_back(size_sect);
          nl.upper_remaining_size -= size_sect;

          // The nu should be fully covered by nl.
          nu.lower_remaining_size = 0;

          // Process of fragment.
          size_t size_frag =
              nl.upper_remaining_size > nu.lower_fragment_remaining_size
              ? nu.lower_fragment_remaining_size
              : nl.upper_remaining_size;
          nl.upper_remaining_size -= size_frag;
          nu.lower_fragment_remaining_size -= size_frag;
        } else {
          // The remaining lower tensor should be swapped out.
          nl.region.sections.push_back(nl.upper_remaining_size);
          nu.lower_remaining_size -= nl.upper_remaining_size;
          nl.upper_remaining_size = 0;

          // Process of fragment.
          size_t size_frag =
              nl.upper_fragment_remaining_size > nu.lower_remaining_size
              ? nu.lower_remaining_size
              : nl.upper_fragment_remaining_size;
          nl.upper_fragment_remaining_size -= size_frag;
          nu.lower_remaining_size -= size_frag;
        }

        nl.posts.push_back(&nu);
        if (nl.upper_remaining_size == 0 &&
            nl.upper_fragment_remaining_size == 0)
          ++ql;
        if (nu.lower_remaining_size == 0 &&
            nu.lower_fragment_remaining_size == 0)
          ++qu;
      }

      if (qu == uppers.end()) {
        while (ql != lowers.end()) {
          Node& n = nodes.at(*ql++);
          n.region.sections.push_back(n.upper_remaining_size);
          n.upper_remaining_size = 0;
        }
        for (auto& s : uppers)
          assert(nodes.at(s).lower_remaining_size == 0);
      }
      for (auto& s : lowers)
        assert(nodes.at(s).upper_remaining_size == 0);
    }

    // Section information for the top layer. No splition (only one section).
    for (auto& s : memory_map.layers.back()) {
      Region& r = memory_map.regions.at(s);
      r.sections.push_back(r.size);
    }
  }

 public:
  Model() = default;
  Model(const Model&) = default;
  Model(Model&&) = default;

  Model& operator=(const Model&) = default;
  Model& operator=(Model&&) = default;

  void setMemoryInfo(const MemoryInfo& memory_info) {
    device_size = memory_info.device.total_size;
    memory_map.setMemorySize(device_size);
  }

  void analyze(status::MemoryStatus& status, bool fragmented = true) {
    if (analyzed)
      return;

    fillModel(status);
    for (auto& x : memory_map.layers)
      assert(x.isAccomodatable());
    // If only one layer, there'e no need to analyze.
    if (memory_map.layers.size() != 1) {
      generateFragments();
      for (auto& x : memory_map.layers)
        assert(x.isAccomodatable());
      generateTree();
    }
    analyzed = true;
  }

  int getLayerCount() const {
    return memory_map.layers.size();
  }
  const std::vector<Layer>& getLayers() const {
    return memory_map.layers;
  }
  const Layer& getLayer(int _layer) const {
    return memory_map.layers[_layer];
  }
  const Node getMemoryNode(const std::string& _node) const {
    return nodes.at(_node);
  }

  MemoryMap getMemoryMap() {
    if (!analyzed)
      throw status_exception("Memory map not analyzed.");
    return memory_map;
  }

  void clear() noexcept {
    clusters.clear();
    memory_map.clear();
    memory_map.createLayer();
    analyzed = false;
  }

  ~Model() = default;
}; // struct Model

} // namespace decisions
} // namespace mori

namespace mori {

struct MemoryScheduler {
 protected:
  const Context& context;
  status::MemoryStatus& status;
  events::Events& events;

  events::ScheduleEvents schedule_events;

 public:
  MemoryScheduler(
      const Context& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : context(_context), status(_status), events(_events) {}

  /**
   * Action when the scheduling is triggered.
   */
  virtual void onSchedule() = 0;

  /**
   * Action when new memory event is submitted.
   * @param event The submitted event
   */
  virtual void onMemoryEvent(const events::MemoryEvent& event) = 0;

  /**
   * Action when an iteration starts.
   */
  virtual void onNewIteration() = 0;

  inline events::ScheduleEvents getScheduleEvents() {
    return schedule_events;
  }

  void submitEvent(events::MemoryEvent event) {
    onMemoryEvent(event);
  }

  void newIteration() {
    onNewIteration();
  }

  virtual ~MemoryScheduler() = default;

}; // struct MemoryScheduler

struct FIFOMemoryScheduler : public MemoryScheduler {
 private:
  bool model_decided = false;
  bool event_decided = false;

  decisions::Model model;

 protected:
  void analyze() {
    model.setMemoryInfo(status.getMemoryInfo());
    model.analyze(status);
    schedule_events.memory_map = model.getMemoryMap();

    model_decided = true;
  }

 public:
  FIFOMemoryScheduler(
      const Context& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : MemoryScheduler(_context, _status, _events) {}

  virtual void onSchedule() override {
    return;
    if (!model_decided)
      analyze();
    if (model.getLayerCount() == 1)
      return;

    // Prepare memory events
    auto iter_1_res = events.select()
                          .where([](const events::EventSet::item& item) {
                            return item.first == 1;
                          })
                          .get();

    if (iter_1_res.size() == 0)
      return;

    auto iter_1_forward_res =
        iter_1_res.select()
            .where([](const events::EventSet::item& item) {
              return item.second.stage == ApplicationStage::forward;
            })
            .get();

    // Generate swapout events based on analysis model.
    std::vector<status::TensorPres> tensors_swap;
    std::unordered_map<std::string, std::vector<events::ScheduleEvent>>
        events_swapout;
    for (auto& l : model.getLayers()) {
      for (auto& s : l.regions) {
        const decisions::Node& node = model.getMemoryNode(s);
        if (node.posts.empty())
          continue; // No need to swapout tensor.
        status::TensorPres tensor = status.referenceTensor(s);
        tensors_swap.emplace_back(std::move(tensor));
        for (auto& x : node.region.sections) {
          // Generate copyout and freehost events.
          auto iter_1_tensor_forward_res =
              iter_1_forward_res.select()
                  .where([s](const events::EventSet::item& item) {
                    return item.second.tensor == s;
                  })
                  .get();

          std::string last_acquired;
          std::string last_assigned;
          for (auto& y : iter_1_tensor_forward_res.ref()) {
            switch (y->second.type) {
              case events::MemoryEventType::read:
                last_acquired = y->second.op;
                break;
              case events::MemoryEventType::write:
              case events::MemoryEventType::access:
                last_assigned = y->second.op;
                break;
              default:
                break;
            }
          }

          // events_swapout[last_assigned].emplace_back(tensor.getOperatorName(),
          // s, x, events::ScheduleEventType::copyout, last_assigned);
          events_swapout[last_acquired].emplace_back(
              tensor.getOperatorName(),
              s,
              x,
              events::ScheduleEventType::swapout,
              last_acquired);
        }
      }
    }

    for (auto& s : status.getExecutionOrder()) {
      if (events_swapout.find(s) == events_swapout.end())
        continue;
      for (auto& x : events_swapout.at(s)) {
        schedule_events.forward_schedule_events.execution.push_back(x);
      }
    }

    auto iter_1_backward_res =
        iter_1_res.select()
            .where([](const events::EventSet::item& item) {
              return item.second.stage == ApplicationStage::backward;
            })
            .get();

    for (auto& x : tensors_swap) {
      // Get the first access of this tensor in backword stage
      auto target_tensor_backward_res =
          iter_1_backward_res.select()
              .where([&x](const events::EventSet::item& item) {
                return item.second.tensor == x.getName() &&
                    item.second.type != events::MemoryEventType::swapin &&
                    item.second.type != events::MemoryEventType::swapout;
              })
              .get();

      auto opb = (*target_tensor_backward_res.ref().begin())->second.op;
      for (int i = 0; i < 1; ++i) {
        opb = status.getExecutionPost(opb);
        if (opb == "")
          break;
      }

      // Generate swapin event
      if (opb != "")
        schedule_events.backward_schedule_events.execution.emplace(
            schedule_events.backward_schedule_events.execution.begin(),
            x.getOperatorName(),
            x.getName(),
            x.getSize(),
            events::ScheduleEventType::swapin,
            opb);
    }

    event_decided = true;
  }

  virtual void onMemoryEvent(const events::MemoryEvent& event) override {}

  virtual void onNewIteration() override {
    onSchedule();
  }

  virtual ~FIFOMemoryScheduler() = default;

}; // struct FIFOMemoryScheduler

struct DependencyAwareMemoryScheduler : public MemoryScheduler {
  DependencyAwareMemoryScheduler(
      const Context& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : MemoryScheduler(_context, _status, _events) {}

  virtual void onSchedule() override {}
  virtual void onMemoryEvent(const events::MemoryEvent& event) override {}
  virtual void onNewIteration() override {}

  virtual ~DependencyAwareMemoryScheduler() = default;

}; // struct DependencyAwareMemoryScheduler

struct MaximumSizePriorityMemoryScheduler : public MemoryScheduler {
  MaximumSizePriorityMemoryScheduler(
      const Context& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : MemoryScheduler(_context, _status, _events) {}

  virtual void onSchedule() override {}
  virtual void onMemoryEvent(const events::MemoryEvent& event) override {}
  virtual void onNewIteration() override {}

  virtual ~MaximumSizePriorityMemoryScheduler() = default;
}; // struct MaximumSizePriorityMemoryScheduler

} // namespace mori

namespace mori {

/**
 * BasicBackend
 * The backend of Mori.
 */
struct BasicBackend final : public Backend {
 protected:
  // Backend information
  Context context;

  status::MemoryStatus status;
  std::unique_ptr<exporter::TensorsExporter> tensors_exporter;
  void* tensors_exporter_hinst = nullptr;

  events::Events events;
  std::unique_ptr<exporter::EventsExporter> events_exporter;
  void* events_exporter_hinst = nullptr;

  std::unique_ptr<MemoryScheduler> scheduler;
  void* scheduler_hinst = nullptr;

  std::atomic<bool> inited = false;
  std::atomic<bool> started = false;

  // Scheduling information
  std::thread scheduler_thread;
  std::recursive_mutex scheduler_mutex;
  int sleep_interval = 5; // millisecond

 public:
  BasicBackend(Context _context) {
    context = _context;

    // Set up scheduler
    std::string scheduler_name = context.at("scheduler");
    if (scheduler_name == "fifo")
      scheduler = std::unique_ptr<MemoryScheduler>(
          new FIFOMemoryScheduler(context, status, events));
    else if (scheduler_name == "dependency")
      scheduler = std::unique_ptr<MemoryScheduler>(
          new DependencyAwareMemoryScheduler(context, status, events));
    else if (scheduler_name == "maxsize")
      scheduler = std::unique_ptr<MemoryScheduler>(
          new MaximumSizePriorityMemoryScheduler(context, status, events));
    else
      scheduler_hinst = utils::load_dylib(
          "Scheduler",
          context.at("scheduler.path"),
          "scheduler_entry",
          scheduler);

    // Set up events exporter
    std::string events_exporter_name = context.at("exporters.events");
    if (events_exporter_name == "empty")
      events_exporter = std::unique_ptr<exporter::EventsExporter>(
          new exporter::EventsExporter(context.view("exporters.events")));
    else
      events_exporter_hinst = utils::load_dylib(
          "Events Exporter",
          context.at("exporters.events.path"),
          "events_exporter_entry",
          events_exporter,
          context.view("exporters.events"));

    // Set up tensors exporter
    std::string tensors_exporter_name = context.at("exporters.tensors");
    if (tensors_exporter_name == "empty")
      tensors_exporter = std::unique_ptr<exporter::TensorsExporter>(
          new exporter::TensorsExporter(context.view("exporters.tensors")));
    else
      tensors_exporter_hinst = utils::load_dylib(
          "Tensors Exporter",
          context.at("exporters.tensors.path"),
          "tensors_exporter_entry",
          tensors_exporter,
          context.view("exporters.tensors"));
  }

  virtual void init() override {
    if (inited)
      throw inited_exception();
    if (started)
      throw inited_exception();

    inited = true;
  }

  virtual void submitMemoryStatus(
      const status::MemoryStatus& _status) override {
    status = _status;
    for (auto& s : status.getTensors()) {
      status::TensorPres pres = status.referenceTensor(s);
      tensors_exporter->onTensor(pres.get());
    }
    for (auto& s : status.getOperators()) {
      status::OperatorPres pres = status.referenceOperator(s);
      tensors_exporter->onOperator(pres.get());
    }
    tensors_exporter->onEntry(status.getEntry());
  }

  virtual void start() override {
    if (!inited)
      throw uninited_exception();
    if (started)
      throw inited_exception();

    started = true;

    // Init scheduler
    // scheduler->init();
    // if (scheduler->isActiveScheduler()) {
    //     scheduler_thread = std::thread([this]() {
    //         std::unique_lock<std::recursive_mutex>{scheduler_mutex};
    //         while (started) {
    //             scheduler->schedule();
    //             std::this_thread::sleep_for(std::chrono::milliseconds{sleep_interval});
    //         }
    //     });
    //     // Examine if the thread starts properly
    //     while (!scheduler_thread.joinable());
    // }
  }

  virtual void submitEvent(const events::MemoryEvent& event) override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();

    // switch (event.type) {
    //     case events::MemoryEventType::allocate:
    //         status.recordMemoryAllocateEvent(event.tensor);
    //         break;
    //     case events::MemoryEventType::free:
    //         status.recordMemoryFreeEvent(event.tensor);
    //         break;
    //     default:
    //         break;
    // }

    events.submitEvent(event);
    events_exporter->onEvent(event);
    scheduler->submitEvent(event);
  }

  virtual events::ScheduleEvents getScheduleEvents() override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();

    events::ScheduleEvents&& re = scheduler->getScheduleEvents();
    for (auto& x : re.memory_map.getFragmentInfo()) {
      status::TensorPres pres = status.referenceTensor(x.first);
      pres.setFragment(x.second);
    }
    return scheduler->getScheduleEvents();
  }

  /**
   * getIteration
   * The current iteration
   * @return current iteration
   */
  virtual int getIteration() {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();
    return events.getIteration();
  }

  virtual void setIteration(int _iteration) override {
    events.setIteration(_iteration);
  }

  /**
   * newIteration
   * Increase the iteration counting of application
   * Since Mori should schedule the swapping in current iteration, this method
   * may block the training until the scheduler is synchorized with backend and
   * application.
   * @return iteration
   */
  virtual void newIteration() override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();
    // Block to synchorize with scheduler.
    events.newIteration();
    scheduler->newIteration();
  }

  virtual void halfIteration() override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();
  }

  virtual void stop() override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();

    started = false;
    // Examine if the thread terminates properly
    if (scheduler_thread.joinable())
      scheduler_thread.join();
  }

  virtual void terminate() override {
    if (!inited)
      throw uninited_exception();
    if (started)
      throw inited_exception();

    inited = false;
  }

  virtual ~BasicBackend() {
    scheduler.release();
    events_exporter.release();
    tensors_exporter.release();

    if (scheduler_hinst)
      dlclose(scheduler_hinst);
    if (events_exporter_hinst)
      dlclose(events_exporter_hinst);
    if (tensors_exporter_hinst)
      dlclose(tensors_exporter_hinst);
  }
}; // struct BasicBackend

} // namespace mori
#endif
#else
extern "C" int backend_entry(
    std::unique_ptr<Backend>& ptr,
    const Context& _context);
#endif

namespace mori {

struct BackendHandle {
  bool inited = false;

  Logger* logger;

  BackendHandle() = default;
  BackendHandle(BackendHandle&& backend_handle) = default;

  void setLogger(Logger* _logger) {
    if (inited)
      throw inited_exception();
    logger = _logger;
  }

  virtual void init() = 0;

  virtual void submitMemoryStatus(const status::MemoryStatus& status) = 0;

  virtual void start() {}

  virtual void submitEvent(const events::MemoryEvent& event) = 0;
  virtual events::ScheduleEvents getScheduleEvents() = 0;

  virtual void setIteration(int _iteration) = 0;
  virtual void newIteration() = 0;
  virtual void halfIteration() = 0;

  virtual void stop() {}

  virtual void terminate() {
    if (!inited)
      throw uninited_exception();

    inited = false;
  }

  virtual ~BackendHandle() {
    logger = nullptr;
  }
}; // struct BackendHandle

struct LocalBackendHandle : public BackendHandle {
  std::unique_ptr<Backend> backend;

  LocalBackendHandle() {}
  LocalBackendHandle(LocalBackendHandle&& backend_handle) {
    backend = std::move(backend_handle.backend);
  }

  virtual void init() override {
    if (inited)
      return;
    backend->init();
    inited = true;
  }

  virtual void submitMemoryStatus(
      const status::MemoryStatus& _status) override {
    backend->submitMemoryStatus(_status);
  }

  virtual void start() override {
    backend->start();
  }

  virtual void submitEvent(const events::MemoryEvent& event) override {
    (*logger) << LogLevel::info << "Submiting of event " << event;
    logger->flush();
    backend->submitEvent(event);
  }

  virtual void setIteration(int _iteration) override {
    backend->setIteration(_iteration);
  }
  virtual void newIteration() override {
    backend->newIteration();
  }
  virtual void halfIteration() override {
    backend->halfIteration();
  }

  virtual events::ScheduleEvents getScheduleEvents() override {
    return backend->getScheduleEvents();
  }

  virtual void stop() override {
    backend->stop();
  }

  virtual void terminate() override {
    if (!inited)
      throw uninited_exception();

    backend->terminate();

    BackendHandle::terminate();
  }

  virtual ~LocalBackendHandle() {
    backend.reset();
  }
}; // struct LocalBackendHandle

#ifndef ENABLE_EXTERNAL_BACKEND
/**
 * Handle for integrated library backend.
 */
struct IntegratedBackendHandle : public LocalBackendHandle {
  IntegratedBackendHandle(const Context& _context) {
    backend.reset(new mori::BasicBackend(_context));
  }
}; // struct IntegratedBackendHandle
#else
/**
 * Handle for dynamic library backend.
 */
struct DylibBackendHandle : public LocalBackendHandle {
  void* hInst;

  DylibBackendHandle(const Context& _context) : LocalBackendHandle(_context) {
    typedef int (*BackendEntryType)(std::unique_ptr<Backend>&, const Context&);

    const std::string& path = _context.at("path");
    std::string obj_path = std::string(path.begin() + 8, path.end()).c_str();

    hInst = dlopen(obj_path.c_str(), RTLD_LAZY);
    if (!hInst)
      throw dynamic_library_exception(
          "Failed to open backend dynamic library.");
    BackendEntryType backend_entry =
        (BackendEntryType)dlsym(hInst, "backend_entry");

    int ret;
    if (backend_entry)
      ret = backend_entry(backend, _context);
    else
      throw dynamic_library_exception("Failed to access backend entry.");

    if (ret != 0)
      throw dynamic_library_exception("Failed to enter backend.");
  }

  virtual ~DylibBackendHandle() {
    backend.reset();
    dlclose(hInst);
  }
}; // struct DylibBackendHandle

#ifdef ENABLE_REMOTE_BACKEND

/**
 * RemoteBackendHandle
 * Handle for remote library backend.
 * Currently, HTTP Mori Server.
 */

// struct SharedMemoryBackendHandle : public BackendHandle {
//     SharedMemoryBackendHandle(const std::string& path) {}
//     virtual ~SharedMemoryBackendHandle() {}
// };  // struct SharedMemoryBackendHandle

// struct UnixSocketBackendHandle : public BackendHandle {
//     UnixSocketBackendHandle(const std::string& path) {}
//     ~UnixSocketBackendHandle() {}
// };  // struct UnixSocketBackendHandle

// struct HTTPBackendHandle : public BackendHandle {
//     HTTPBackendHandle(const std::string& path) {}
//     ~HTTPBackendHandle() {}
// };  // struct HTTPBackendHandle
#endif
#endif

static std::unique_ptr<BackendHandle> make_backend_handle(
    const Context& context) {
  // if (!context.isParamExists("path")) throw context_missing();

  const std::string& path = context.at("path");
#ifndef ENABLE_EXTERNAL_BACKEND
  if (path.find("int://") == 0)
    return std::unique_ptr<BackendHandle>(new IntegratedBackendHandle(context));
#else
  if (path.find("dylib://") == 0)
    return std::unique_ptr<BackendHandle>(new DylibBackendHandle(context));
#ifdef ENABLE_REMOTE_BACKEND
  if (path.find("http://") == path.begin())
    return std::unique_ptr<BackendHandle>(new RemoteBackendHandle(_context));
  if (path.find("https://") == path.begin())
    return std::unique_ptr<BackendHandle>(new RemoteBackendHandle(_context));
#endif
#endif
  else
    throw context_invalid("path");
}

} // namespace mori

#include <cassert>
#include <shared_mutex>

#include <cassert>

namespace mori {

struct MemoryManager {
 public:
  // Basie memory management methods.
  virtual void* allocateDevice(size_t size) = 0;
  virtual void* allocateHost(size_t size) = 0;
  virtual void* allocate(size_t size) {
    return allocateDevice(size);
  }

  virtual void copyIn(
      void* host_address,
      void* device_address,
      size_t size) = 0;
  virtual void copyOut(
      void* device_address,
      void* host_address,
      size_t size) = 0;

  virtual void freeDevice(void* address) = 0;
  virtual void freeHost(void* address) = 0;

  virtual void* swapIn(void* host_address, void* device_address, size_t size) {
    copyIn(host_address, device_address, size);
    freeHost(host_address);
    return device_address;
  }

  virtual void* swapOut(void* device_address, void* host_address, size_t size) {
    copyOut(device_address, host_address, size);
    freeDevice(device_address);
    return host_address;
  }

  virtual void free(void* address) {
    freeDevice(address);
  }

  // Memory section methods.
  virtual bool isMemorySectionSupported() const = 0;

  virtual void copyDevice(void* src, void* dst, size_t size) {
    void* host_address = allocateHost(size);
    copyOut(src, host_address, size);
    copyIn(host_address, dst, size);
    freeHost(host_address);
  }
  virtual void* split(void* address, size_t size) {
    return nullptr;
  }
  virtual void* salloc(void* address, size_t size) {
    return nullptr;
  }
  virtual bool merge(void* left, void* right) {
    return false;
  }

  // Memory info methods.

  virtual MemoryInfo getMemoryInfo() const = 0;

  virtual ~MemoryManager() {}
};

} // namespace mori

namespace mori {

struct MemoryOperationExecutor final {
 private:
  struct MemoryOperationExecutorImpl {
    MemoryOperationExecutor& executor;

    MemoryOperationExecutorImpl(MemoryOperationExecutor& _executor)
        : executor(_executor) {}

    virtual void copyIn(status::TensorPres& tensor, size_t size) = 0;
    virtual void copyOut(status::TensorPres& tensor, size_t size) = 0;
    virtual void freeDevice(status::TensorPres& tensor, size_t size) = 0;
    virtual void freeHost(status::TensorPres& tensor, size_t size) = 0;
    virtual void fragment(status::TensorPres& tensor) = 0;
    virtual void fuse(status::TensorPres& tensor) = 0;
  }; // struct MemoryOperationExecutorImpl

  struct MemoryOperationExecutorDefaultImpl final
      : public MemoryOperationExecutorImpl {
    MemoryOperationExecutorDefaultImpl(MemoryOperationExecutor& _executor)
        : MemoryOperationExecutorImpl(_executor) {}

    virtual void copyIn(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid(
            "Copying in size larger than tensor size.");
      assert(tensor.getSectionCount() == 1);
      const status::MemorySection& section = tensor.getFirstSection();

      switch (section.status) {
        case status::MemoryStatusType::none:
        case status::MemoryStatusType::host: {
          void* device_address = nullptr;
          // Allocate this section
          device_address =
              executor.memory_manager->allocateDevice(section.size);
          if (device_address == nullptr)
            throw memory_device_insufficience(
                "Device memory insufficient.", section.size);
          executor.layout.recordMemoryAllocateEvent(
              device_address, section.size, tensor.getName());
          // if (tensor.hasFragment()) {
          //     memory_manager->split(device_address, section->size);
          //     tensor.setFragmentPlaced((uint8_t*)device_address +
          //     section->size);
          // }
          // Less possible to happen since copying in usually takes place in
          // backward propagation, while the peak memory usage is gone through.

          if (section.status == status::MemoryStatusType::host) {
            // Copy in this section.
            executor.memory_manager->copyIn(
                section.host_address, device_address, section.size);
            tensor.setCopiedIn(section.offset, device_address);
            // This tensor's status will be coexist.
            assert(section.status == status::MemoryStatusType::coexist);
          } else {
            tensor.setCopiedIn(section.offset, device_address);
            // This tensor's status will be empty.
            assert(section.status == status::MemoryStatusType::empty);
          }
        }
        case status::MemoryStatusType::coexist:
        case status::MemoryStatusType::empty:
        default:
          break;
      }
    }
    void copyOut(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid(
            "Copying out size larger than tensor size.");
      assert(tensor.getSectionCount() == 1);
      const status::MemorySection* section = &(tensor.getFirstSection());

      switch (section->status) {
        case status::MemoryStatusType::device: {
          void* host_address =
              executor.memory_manager->allocateHost(section->size);
          if (host_address == nullptr)
            throw memory_host_insufficience(
                "Host memory insufficient.", section->size);
          executor.memory_manager->copyOut(
              section->device_address, host_address, section->size);
          tensor.setCopiedOut(section->offset, host_address);
          // This tensor's status will be coexist.
          assert(section->status == status::MemoryStatusType::coexist);
        }
        case status::MemoryStatusType::coexist:
        case status::MemoryStatusType::empty:
          // Empty can be regarded as a new kind of 'coexist', the data on
          // device is empty (not assigned), the data on host is empty (do not
          // need host memory space).
        default:
          break;
      }
    }
    virtual void freeDevice(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid("Freeing size larger than tensor size.");
      assert(tensor.getSectionCount() == 1);
      const status::MemorySection* section = &(tensor.getFirstSection());
      switch (section->status) {
        case status::MemoryStatusType::device:
        case status::MemoryStatusType::coexist:
        case status::MemoryStatusType::empty: {
          executor.layout.recordMemoryFreeEvent(section->device_address);
          executor.memory_manager->freeDevice(section->device_address);
          tensor.setDeviceFreed(section->offset);
        }
        default:
          break;
      }
    }
    virtual void freeHost(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid("Freeing size larger than tensor size.");
      assert(tensor.getSectionCount() == 1);
      const status::MemorySection* section = &(tensor.getFirstSection());
      switch (section->status) {
        case status::MemoryStatusType::host:
        case status::MemoryStatusType::coexist:
          executor.memory_manager->freeHost(section->host_address);
          tensor.setHostFreed(section->offset);
          break;
        default:
          break;
      }
    }
    virtual void fragment(status::TensorPres& tensor) override {}
    virtual void fuse(status::TensorPres& tensor) override {}
  }; // struct MemoryOperationDefaultImpl

  struct MemoryOperationExecutorSectionedImpl final
      : public MemoryOperationExecutorImpl {
   protected:
    void relocate(status::TensorPres& tensor) {
      void* device_address =
          executor.memory_manager->allocateDevice(tensor.getSize());
      if (device_address == nullptr) {
        if (tensor.getDeviceSize() != 0)
          executor.swapOut(tensor, tensor.getDeviceSize());
        assert(!tensor.isDeviceLocated());
        device_address =
            executor.memory_manager->allocateDevice(tensor.getSize());
        if (device_address == nullptr)
          throw memory_device_insufficience(
              "Relocation of tensor failed.", tensor.getSize());
      }
      executor.layout.recordMemoryAllocateEvent(
          device_address, tensor.getSize(), tensor.getName());
      // Remove fragment
      if (tensor.hasFragment()) {
        const status::Fragment& fragment = tensor.getFragment();
        if (tensor.getFragment().status == status::MemoryStatusType::empty) {
          executor.layout.recordMemoryFreeEvent(fragment.address);
          executor.memory_manager->freeDevice(fragment.address);
          tensor.setFragmentRemoved();
        }
      }

      const status::MemorySection* section = &(tensor.getFirstSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::empty:
            executor.layout.recordMemoryFreeEvent(section->device_address);
            executor.memory_manager->freeDevice(section->device_address);
            tensor.setDeviceFreed(section->offset);
          case status::MemoryStatusType::none:
            tensor.setCopiedIn(section->offset, device_address);
            break;
          case status::MemoryStatusType::host:
            executor.memory_manager->copyIn(
                section->host_address, device_address, section->size);
            tensor.setCopiedIn(section->offset, device_address);
            break;
          case status::MemoryStatusType::coexist:
          case status::MemoryStatusType::device:
            executor.memory_manager->copyDevice(
                section->device_address, device_address, section->size);
            executor.layout.recordMemoryFreeEvent(section->device_address);
            executor.memory_manager->freeDevice(section->device_address);
            tensor.setMoved(section->offset, device_address);
          default:
            break;
        }
        device_address = (uint8_t*)device_address + section->size;

        const status::MemorySection* section_prev = section->prev();
        if (section_prev != nullptr) {
          if (tensor.isMergeable(section_prev->offset)) {
            // The sections are already in continuous memory region, hence only
            // update memory section information.
            section = &(tensor.merge(section_prev->offset));
          } else {
            executor.memory_manager->split(
                section_prev->device_address, section_prev->size);
            executor.layout.recordMemorySplitEvent(
                section_prev->device_address, section_prev->size);
          }
        }
        section = section->next();
      } while (section != nullptr);
    }

   public:
    MemoryOperationExecutorSectionedImpl(MemoryOperationExecutor& _executor)
        : MemoryOperationExecutorImpl(_executor) {}
    virtual void copyIn(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid(
            "Copying in size larger than tensor size.");
      size_t copied_size = 0;
      const status::MemorySection* section = &(tensor.getLastSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::none:
          case status::MemoryStatusType::host: {
            void* device_address = nullptr;
            // Allocate this section
            // Make a quick path here.
            if (tensor.getSize() == size && tensor.getDeviceSize() == 0) {
              // If copying size equals to tensor size, and tensor currently has
              // no data on device, the whole tensor must have been swapped out
              // before and be copied in now. Hence, just relocate this tensor.
              relocate(tensor);
              return;
            }
            // There may be a number of sections, and the section should be
            // smaller or equal to the specified size.
            device_address = executor.memory_manager->salloc(
                section->device_address, section->size);
            if (device_address == nullptr) {
              // Allocate memory for the tensor and copy in all the data.
              relocate(tensor);
              return;
            }
            // Salloc do not need aligned allcoation.
            executor.layout.recordMemoryAllocateEvent(
                device_address, section->size, tensor.getName(), 1);
            assert(device_address == section->device_address);

            // Less possible to happen since copying in usually takes place in
            // backward propagation, while the peak memory usage is gone
            // through.

            if (section->status == status::MemoryStatusType::host) {
              // Copy in this section.
              executor.memory_manager->copyIn(
                  section->host_address, device_address, section->size);
              tensor.setCopiedIn(section->offset, device_address);
              // This tensor's status will be coexist.
              assert(section->status == status::MemoryStatusType::coexist);
            } else {
              tensor.setCopiedIn(section->offset, device_address);
              // This tensor's status will be empty.
              assert(section->status == status::MemoryStatusType::empty);
            }

            // Process memory section merging.
            if (tensor.isMergeable(section->offset)) {
              assert(executor.memory_manager->merge(
                  device_address, (uint8_t*)device_address + section->size));
              executor.layout.recordMemoryMergeEvent(
                  device_address, (uint8_t*)device_address + section->size);
              tensor.merge(section->offset);
            }
            const status::MemorySection* section_prev = section->prev();
            if (section_prev != nullptr &&
                tensor.isMergeable(section_prev->offset)) {
              assert(executor.memory_manager->merge(
                  section_prev->device_address, device_address));
              executor.layout.recordMemoryMergeEvent(
                  section_prev->device_address, device_address);
              section = &(tensor.merge(section_prev->offset));
            }
          }
          case status::MemoryStatusType::coexist:
          case status::MemoryStatusType::empty:
            copied_size += section->size;
          default:
            break;
        }
        if (copied_size >= size)
          return;
        section = section->prev();
      } while (section != nullptr);
    }
    virtual void copyOut(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid(
            "Copying out size larger than tensor size.");
      size_t copied_size = 0;
      const status::MemorySection* section = &(tensor.getFirstSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::device: {
            if (copied_size + section->size > size) {
              executor.memory_manager->split(
                  section->device_address, size - copied_size);
              executor.layout.recordMemorySplitEvent(
                  section->device_address, size - copied_size);
              tensor.split(section->offset, size - copied_size);
            }

            void* host_address =
                executor.memory_manager->allocateHost(section->size);
            if (host_address == nullptr)
              throw memory_host_insufficience(
                  "Host memory insufficient.", section->size);
            executor.memory_manager->copyOut(
                section->device_address, host_address, section->size);
            tensor.setCopiedOut(section->offset, host_address);
            // This tensor's status will be coexist.
            assert(section->status == status::MemoryStatusType::coexist);

            copied_size += section->size;
            if (copied_size >= size)
              return;
          }
          case status::MemoryStatusType::coexist:
          case status::MemoryStatusType::empty:
            // Empty can be regarded as a new kind of 'coexist', the data on
            // device is empty (not assigned), the data on host is empty (do not
            // need host memory space).
          default:
            break;
        }
        section = section->next();
      } while (section != nullptr);
    }
    virtual void freeDevice(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid("Freeing size larger than tensor size.");
      size_t freed_size = 0;
      const status::MemorySection* section = &(tensor.getFirstSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::device:
          case status::MemoryStatusType::coexist:
          case status::MemoryStatusType::empty: {
            executor.layout.recordMemoryFreeEvent(section->device_address);
            executor.memory_manager->freeDevice(section->device_address);
            tensor.setDeviceFreed(section->offset);

            if (tensor.isMergeable(section->offset))
              tensor.merge(section->offset);
            const status::MemorySection* section_prev = section->prev();
            if (section_prev != nullptr &&
                tensor.isMergeable(section_prev->offset))
              section = &(tensor.merge(section_prev->offset));

            freed_size += section->size;
          }
          default:
            break;
        }
        if (freed_size >= size)
          break;
        section = section->next();
      } while (section != nullptr);
      if (tensor.getDeviceSize() == 0 && tensor.hasFragment()) {
        executor.layout.recordMemoryFreeEvent(tensor.getFragment().address);
        executor.memory_manager->freeDevice(tensor.getFragment().address);
        tensor.setFragmentRemoved();
      }
    }
    virtual void freeHost(status::TensorPres& tensor, size_t size) override {
      if (tensor.getSize() < size)
        throw status::tensor_invalid("Freeing size larger than tensor size.");
      size_t freed_size = 0;
      const status::MemorySection* section = &(tensor.getFirstSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::host:
          case status::MemoryStatusType::coexist: {
            executor.memory_manager->freeHost(section->host_address);
            tensor.setHostFreed(section->offset);
            freed_size += section->size;
            if (tensor.isMergeable(section->offset)) {
              if (section->status != status::MemoryStatusType::none) {
                assert(executor.memory_manager->merge(
                    section->device_address, section->next()->device_address));
                executor.layout.recordMemoryMergeEvent(
                    section->device_address, section->next()->device_address);
              }
              tensor.merge(section->offset);
            }
            const status::MemorySection* section_prev = section->prev();
            if (section_prev != nullptr &&
                tensor.isMergeable(section_prev->offset)) {
              if (section->status != status::MemoryStatusType::none) {
                assert(executor.memory_manager->merge(
                    section_prev->device_address, section->device_address));
                executor.layout.recordMemoryMergeEvent(
                    section_prev->device_address, section->device_address);
              }
              section = &(tensor.merge(section_prev->offset));
            }
            if (freed_size >= size)
              return;
            break;
          }
          default:
            break;
        }
        section = section->next();
      } while (section != nullptr);
    }
    virtual void fragment(status::TensorPres& tensor) override {
      if (!tensor.hasFragment())
        throw status::tensor_invalid("Tensor does not request fragment.");
      void* target_address =
          (uint8_t*)(tensor.getFirstSection().device_address) +
          tensor.getSize();
      void* device_address = executor.memory_manager->salloc(
          target_address, tensor.getFragment().size);
      if (device_address == nullptr)
        throw memory_exception("Allocation for fragment failed.");
      executor.layout.recordMemoryAllocateEvent(
          device_address, tensor.getFragment().size, tensor.getName(), 1);
      tensor.setFragmentPlaced();
    }
    virtual void fuse(status::TensorPres& tensor) override {
      if (!tensor.hasFragment())
        throw status::tensor_invalid("Tensor does not request fragment.");
      executor.layout.recordMemoryFreeEvent(tensor.getFragment().address);
      executor.memory_manager->freeDevice(tensor.getFragment().address);
      tensor.setFragmentRemoved();
    }
  }; // struct MemoryOperationExecutorSectionedImpl

 private:
  layout::MemoryLayout& layout;
  MemoryManager* memory_manager = nullptr;

  MemoryOperationExecutorDefaultImpl default_impl;
  MemoryOperationExecutorSectionedImpl sectioned_impl;
  MemoryOperationExecutorImpl* impl = nullptr;

 public:
  MemoryOperationExecutor(layout::MemoryLayout& _layout)
      : layout(_layout), default_impl(*this), sectioned_impl(*this) {
    impl = &default_impl;
  }

  inline void setMemoryManager(MemoryManager* _memory_manager) {
    memory_manager = _memory_manager;
    if (memory_manager->isMemorySectionSupported())
      impl = &sectioned_impl;
  };

  // void allocate(status::TensorPres& tensor) {
  //     void* device_address = memory_manager->allocate(tensor.getSize() +
  //     tensor.getFragment().size); if (device_address == nullptr) throw
  //     memory_device_insufficience("Device memory insufficient.",
  //     tensor.getSize()); layout.recordMemoryAllocateEvent(device_address,
  //     tensor.getSize() + tensor.getFragment().size, tensor.getName());
  //     tensor.setAllocated(device_address);
  // }

  void copyIn(status::TensorPres& tensor, size_t size) {
    impl->copyIn(tensor, size);
  }

  void copyOut(status::TensorPres& tensor, size_t size) {
    impl->copyOut(tensor, size);
  }

  void freeDevice(status::TensorPres& tensor, size_t size) {
    impl->freeDevice(tensor, size);
  }

  void freeHost(status::TensorPres& tensor, size_t size) {
    impl->freeHost(tensor, size);
  }

  void swapIn(status::TensorPres& tensor, size_t size) {
    copyIn(tensor, size);
    freeHost(tensor, size);
  }

  void swapOut(status::TensorPres& tensor, size_t size) {
    copyOut(tensor, size);
    freeDevice(tensor, size);
  }

  void free(status::TensorPres& tensor, size_t size) {
    freeDevice(tensor, size);
    freeHost(tensor, size);
  }

  void fragment(status::TensorPres& tensor) {
    impl->fragment(tensor);
  }

  void fuse(status::TensorPres& tensor) {
    impl->fuse(tensor);
  }

}; // struct MemoryOperationExecutor

} // namespace mori

namespace mori {

enum struct CallbackStage {
  postSwapIn,
  postSwapOut
}; // enum struct CallbackStage

using Callback = std::function<int(const std::string&, void*)>;
using Callbacks = std::unordered_map<CallbackStage, Callback>;

} // namespace mori

namespace mori {

struct MemoryScheduleExecutor final {
 protected:
  Context context;
  status::MemoryStatus& status;
  layout::MemoryLayout& layout;
  Logger* logger = nullptr;
  Callbacks callbacks;
  std::weak_ptr<BackendHandle> backend_handle;

  // Schedule information
  std::shared_mutex events_m;
  events::StageScheduleEvents forward_schedule_events;
  events::StageScheduleEvents backward_schedule_events;
  std::atomic<events::StageScheduleEvents*> current_eventset;
  std::shared_mutex events_mutex;

  std::mutex new_events_m;
  std::atomic<bool> events_updated = false;
  events::ScheduleEvents new_events;

  std::shared_mutex current_operator_m;
  std::string current_operator;

  // Executor thread
  std::thread executor_thread;
  std::recursive_mutex executor_mutex;

  std::deque<events::ScheduleEvent> activated_events;
  std::mutex queue_m;

  std::atomic<bool> half_iter_sync = false;
  std::atomic<bool> iter_sync = false;

  // The schedule events are ordered.
  // The operator-triggered events are ordered by the execution sequence of
  // operators. The time-triggered events are ordered by the triggering
  // timepoint.
  std::chrono::steady_clock::time_point current_time_offset;
  std::vector<events::ScheduleEvent>::iterator current_timepoint_event_posi;
  std::vector<events::ScheduleEvent>::iterator current_execution_event_posi;

  MemoryOperationExecutor executor;

  std::atomic<bool> inited = false;

  // Time-triggered events require these methods to reset the schedule timepoint
  // offset.
  inline int getExecutionTimepoint() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now() - current_time_offset)
        .count();
  }
  inline void resetExecution() {
    // Reset execution of execution-triggered events.
    current_execution_event_posi = current_eventset.load()->execution.begin();
    // Reset execution of timepoint-triggered events.
    current_time_offset = std::chrono::steady_clock::now();
    current_timepoint_event_posi = current_eventset.load()->timepoint.begin();

    activated_events.clear();
  }

  void activateEvents() {
    std::vector<events::ScheduleEvent>& eventset =
        current_eventset.load()->timepoint;
    std::shared_lock<std::shared_mutex> l{events_mutex};

    // Activate timepoint triggered events.
    // Execution triggered events do not need to be activated here.
    int current_exec_timepoint = getExecutionTimepoint();
    auto current_end = std::find_if(
        current_timepoint_event_posi,
        eventset.end(),
        [current_exec_timepoint](const events::ScheduleEvent& event) {
          return event.timepoint > current_exec_timepoint;
        });

    // Retrieve the schedule events that should be triggered.
    std::unique_lock<std::mutex> queue_lock{queue_m};
    while (current_timepoint_event_posi < current_end) {
      activated_events.push_back(*current_timepoint_event_posi);
      ++current_timepoint_event_posi;
    }
  }

  void executeEvents() {
    std::unique_lock<std::mutex> queue_lock{queue_m};
    while (!activated_events.empty()) {
      // Retrieve tensor information.
      events::ScheduleEvent event = activated_events.front();
      activated_events.pop_front();

      const std::string& operator_name = event.operator_name;
      const std::string& tensor_name = event.tensor_name;
      size_t size = event.size;

      std::shared_lock<std::shared_mutex> col{current_operator_m};
      if (current_operator == tensor_name)
        continue;

      status::TensorPres tensor = status.referenceTensor(tensor_name);
      try {
        switch (event.type) {
          case events::ScheduleEventType::copyin:
            executor.copyIn(tensor, size);
            break;
          case events::ScheduleEventType::copyout:
            executor.copyOut(tensor, size);
            break;
          case events::ScheduleEventType::swapin:
            executor.swapIn(tensor, size);
            if (callbacks.count(CallbackStage::postSwapIn))
              callbacks.at(CallbackStage::postSwapIn)(
                  tensor_name, tensor.getSection(0).device_address);
            (*logger) << LogLevel::debug << "Operator " << operator_name
                      << ": tensor " << tensor_name
                      << " swapped in. (Prefetch)";
            logger->flush();
            break;
          case events::ScheduleEventType::swapout:
            executor.swapOut(tensor, size);
            if (callbacks.count(CallbackStage::postSwapOut))
              callbacks.at(CallbackStage::postSwapOut)(
                  tensor_name, tensor.getSection(0).host_address);
            (*logger) << LogLevel::debug << "Operator " << operator_name
                      << ": tensor " << tensor_name
                      << " swapped out. (Instant)";
            logger->flush();
            break;
          case events::ScheduleEventType::freehost:
            executor.freeHost(tensor, size);
            break;
          case events::ScheduleEventType::freedev:
            executor.freeDevice(tensor, size);
            break;
          case events::ScheduleEventType::free:
            executor.free(tensor, size);
            break;
          default:
            break;
        }
      } catch (std::exception& e) {
        (*logger) << LogLevel::debug
                  << "Exception in executing memory swapping events, reason: "
                  << e.what();
        logger->flush();
      }
    }
  }

 public:
  MemoryScheduleExecutor(
      Context _context,
      status::MemoryStatus& _status,
      layout::MemoryLayout& _layout)
      : context(_context),
        status(_status),
        layout(_layout),
        executor(_layout) {}

  MemoryScheduleExecutor(const MemoryScheduleExecutor&) = delete;
  MemoryScheduleExecutor(MemoryScheduleExecutor&& executor) = delete;

  void setBackendHandle(const std::weak_ptr<BackendHandle>& _backend_handle) {
    if (inited)
      throw inited_exception();
    backend_handle = _backend_handle;
  }

  void setMemoryManager(MemoryManager* _memory_manager) {
    if (inited)
      throw inited_exception();
    executor.setMemoryManager(_memory_manager);
  }

  void setLogger(Logger* _logger) {
    if (inited)
      throw inited_exception();
    logger = _logger;
  }

  void setCallback(
      CallbackStage stage,
      const std::function<int(const std::string&, void*)>& callback) {
    if (inited)
      throw inited_exception();
    callbacks.emplace(stage, callback);
  }

  void init() {
    if (inited)
      throw inited_exception();

    current_eventset.store(&forward_schedule_events);
    resetExecution();

    inited = true;

    executor_thread = std::thread([this]() {
      while (inited) {
        // Examine if synchronization required.
        if (half_iter_sync || iter_sync) {
          // Inactivate all events, and prevent further events.
          std::unique_lock<std::mutex> ql{queue_m};
          activated_events.clear();

          if (half_iter_sync) {
            std::shared_lock<std::shared_mutex> em{events_m};
            assert(current_eventset.load() == &this->forward_schedule_events);
            current_eventset.store(&this->backward_schedule_events);
            resetExecution();
            half_iter_sync = false;
          }
          if (iter_sync) {
            if (events_updated) {
              std::unique_lock<std::shared_mutex> em_n{events_m};
              std::unique_lock<std::mutex> nem{new_events_m};

              this->forward_schedule_events =
                  std::move(this->new_events.forward_schedule_events);
              this->backward_schedule_events =
                  std::move(this->new_events.backward_schedule_events);
              logger->submit(
                  LogLevel::debug,
                  "Memory schedule executor switches to new schedule event set.");
              events_updated = false;
            }

            std::shared_lock<std::shared_mutex> em{events_m};
            current_eventset.store(&this->forward_schedule_events);
            resetExecution();

            iter_sync = false;
          }
        }

        // Execution of schedule events
        // Activate events should be triggered.
        activateEvents();
        // Executed activated events.
        executeEvents();
      }
    });
    // Examine if the thread starts properly
    while (!executor_thread.joinable())
      ;

    logger->submit(LogLevel::debug, "Memory schedule executor initialized.");
  }

  void updateSchedule(const events::ScheduleEvents& _new_events) {
    std::unique_lock<std::mutex> l{new_events_m};
    this->new_events = _new_events;
    events_updated = true;
  }
  void updateSchedule(events::ScheduleEvents&& _new_events) {
    std::unique_lock<std::mutex> l{new_events_m};
    this->new_events = std::move(_new_events);
    events_updated = true;
  }

  void setOperatorStarted(const std::string& op) {
    std::unique_lock<std::shared_mutex> col{current_operator_m};
    current_operator = op;
  }

  void setOperatorFinished(const std::string& op) {
    std::unique_lock<std::mutex> ql{queue_m};
    while (current_execution_event_posi !=
           current_eventset.load()->execution.end()) {
      if (current_execution_event_posi->postop == op) {
        activated_events.push_back(*current_execution_event_posi++);
      } else
        break;
    }
    // logger->submit(LogLevel::debug, "Memory schedule executor moves to next
    // operator.");
  }

  int getIteration() {
    return 0;
  }
  void setIteration(int _iteration) {}

  void newIteration() {
    if (!inited)
      throw uninited_exception();
    iter_sync = true;
    while (iter_sync)
      ;
    logger->submit(
        LogLevel::debug, "Memory schedule executor moves to next iteration.");
  }

  /**
   * @brief Set half of the iteration finished.
   * @note  The schedule events for forward propagation will be synchronized to
   * be executed and the backward propagation schedule events will be prepared
   * to triggered.
   */
  void halfIteration() {
    if (!inited)
      throw uninited_exception();
    half_iter_sync = true;
    while (half_iter_sync)
      ;
  }

  void terminate() {
    if (!inited)
      throw uninited_exception();

    inited = false;

    // Examine if the thread terminates properly
    if (executor_thread.joinable())
      executor_thread.join();
  }

  ~MemoryScheduleExecutor() {
    if (inited)
      terminate();

    logger = nullptr;
  }

}; // struct MemoryScheduleExecutor

} // namespace mori

namespace mori {

// struct Frontend;

/**
 * MemorySession
 * Management of a memory session, which is a complete memory lifecycle of a
 * training iteration.
 */
struct MemorySession final {
 private:
  friend struct Frontend;

 public:
  struct Request final {
   private:
    friend class MemorySession;

   private:
    MemorySession& session;

   private:
    std::string op;

    ApplicationStage stage;

    std::unordered_map<std::string, status::TensorPres> requested_tensors;
    std::atomic<bool> waiting = true;

    /**
     * isTensorWaited
     * Check if tensor has been selected and waited.
     * @param tensor tensor name
     * @return if the tensor has been selected and waited.
     */
    bool isTensorWaited(const std::string& tensor) {
      return requested_tensors.find(tensor) != requested_tensors.end();
    }

    Request(
        MemorySession& _session,
        const std::string& _op,
        ApplicationStage _stage)
        : session(_session), op(_op), stage(_stage) {
      session.sch_executor.setOperatorStarted(_op);
    }

   public:
    Request(const Request&) = delete;
    Request(Request&& _request) : session(_request.session) {
      op = std::move(_request.op);
      stage = _request.stage;
    }

    void waitTensor(const std::string& tensor) {
      // If the tensor waited, it would have been locked on device memory.
      if (isTensorWaited(tensor))
        return;

      auto p = requested_tensors.emplace(
          tensor, session.status.referenceTensor(tensor));
      status::TensorPres& pres = p.first->second;
      // Do not swap in tensor that already on device.
      if (pres.isDeviceAllLocated())
        return;

      size_t acquiring_size = pres.getSize() - pres.getDeviceSize();
      try {
        session.op_executor.swapIn(pres, pres.getSize() - pres.getDeviceSize());
      } catch (memory_device_insufficience& e) {
        // Memory on device not insufficience.
        session.waitMemory(e.demand());
        session.op_executor.swapIn(pres, pres.getSize() - pres.getDeviceSize());
      }

      // Assert the tensor already on device.
      assert(pres.getDeviceSize() == pres.getSize());

      if (session.callbacks.count(CallbackStage::postSwapIn))
        session.callbacks.at(CallbackStage::postSwapIn)(
            tensor, pres.getSection(0).device_address);
      (*session.logger) << LogLevel::debug << "Operator: " << op
                        << ", tensor: " << tensor
                        << " swapped in. (Memory access)";
      session.logger->flush();
      session.backend_handle.lock()->submitEvent(events::MemoryEvent(
          op, tensor, acquiring_size, events::MemoryEventType::swapin, stage));
    }

    // /**
    //  * waitOperator
    //  * Wait all the tensors of an operator to be located in device memory.
    //  * @param op operator to be waited
    //  */
    // void waitOperator(const std::string& op) {
    //     for (auto &s : status.)
    //         waitTensor(x.first, x.second);
    // }

    /**
     * setMemoryDataAssigned
     * Set the memory data is assigned, or written.
     * The operator name should be provided since the outputs of the prev
     * operators may be accessed.
     * @param op operator name
     * @param tensor tensor name
     */
    void setMemoryDataAssigned(const std::string& tensor) {
      if (!waiting)
        throw uninited_exception();
      if (!isTensorWaited(tensor))
        throw status_exception("Tensor not waited.");

      // Do not acquire locks here since the tensor is awaited.
      // Tensor exists since isTensorWaited(tensor) is true.
      status::TensorPres& pres = requested_tensors.at(tensor);
      pres.setAssigned();

      // emit memory event
      session.backend_handle.lock()->submitEvent(events::MemoryEvent(
          op, tensor, pres.getSize(), events::MemoryEventType::write, stage));
    }

    /**
     * setMemoryDataAcquired
     * Set the memory data is acquired, or read.
     * The operator name should be provided since the outputs of the prev
     * operators may be accessed.
     * @param op operator name
     * @param tensor tensor name
     */
    void setMemoryDataAcquired(const std::string& tensor) {
      if (!waiting)
        throw uninited_exception();
      if (!isTensorWaited(tensor))
        throw status_exception("Operator or tensor not waited.");

      status::TensorPres& pres = requested_tensors.at(tensor);
      pres.setAcquired();

      // emit memory event
      session.backend_handle.lock()->submitEvent(events::MemoryEvent(
          op, tensor, pres.getSize(), events::MemoryEventType::read, stage));
    }

    /**
     * setMemoryDataAccessed
     * Set the memory data is accessed.
     * The operator name should be provided since the outputs of the prev
     * operators may be accessed.
     * @param tensor tensor name
     */
    void setMemoryDataAccessed(const std::string& tensor) {
      if (!waiting)
        throw uninited_exception();
      if (!isTensorWaited(tensor))
        throw status_exception("Tensor not waited.");

      status::TensorPres& pres = requested_tensors.at(tensor);
      pres.setAccessed();

      // emit memory event
      session.backend_handle.lock()->submitEvent(events::MemoryEvent(
          op, tensor, pres.getSize(), events::MemoryEventType::access, stage));
    }

    void release() {
      for (auto& x : requested_tensors)
        x.second.release();

      session.sch_executor.setOperatorFinished(op);
      waiting = false;
    }

    ~Request() {
      if (waiting)
        release();
    }

  }; // inner struct Request

 private:
  Context context;

  std::weak_ptr<BackendHandle> backend_handle;

  status::MemoryStatus& status;
  layout::MemoryLayout& layout;

  MemoryScheduleExecutor& sch_executor;
  MemoryOperationExecutor op_executor;

  Callbacks callbacks;

  Logger* logger;

  ApplicationStage stage = ApplicationStage::forward;

  void setBackendHandle(const std::weak_ptr<BackendHandle>& _backend_handle) {
    backend_handle = _backend_handle;
  }
  void setMemoryManager(MemoryManager* _memory_manager) {
    op_executor.setMemoryManager(_memory_manager);
  }
  void setLogger(Logger* _logger) {
    logger = _logger;
  }
  void setCallback(
      CallbackStage stage,
      const std::function<int(const std::string&, void*)>& callback) {
    callbacks.emplace(stage, callback);
  }

 public:
  MemorySession(
      const Context& _context,
      MemoryScheduleExecutor& _executor,
      status::MemoryStatus& _status,
      layout::MemoryLayout& _layout)
      : context(_context),
        status(_status),
        layout(_layout),
        sch_executor(_executor),
        op_executor(_layout) {}

  int getIteration() const {
    return 0;
  }

  void setIteration(int iteration) {
    sch_executor.setIteration(iteration);
    backend_handle.lock()->setIteration(iteration);
  }

  /**
   * @brief Set the new iteration is ready.
   * @note  This method will synchronize with the schedule executor, to assure
   * all the swappings are finished.
   */
  void newIteration() {
    // Reset stage
    stage = ApplicationStage::forward;

    sch_executor.newIteration();
    backend_handle.lock()->newIteration();
  }

  /**
   * @brief Set the forward progagation, or half of the iteration is executed.
   * @note  This method will synchronize with the schedule executor, to assure
   * all the swap-outs are finished.
   */
  void halfIteration() {
    // Reverse stage
    if (stage == ApplicationStage::forward)
      stage = ApplicationStage::backward;
    else
      stage = ApplicationStage::forward;

    sch_executor.halfIteration();
    // backend_handle.lock()->
  }

  /**
   * @brief Set the memory data has dynamic shape and size changed.
   * @param op operator name
   * @param tensor tensor name
   * @param size dynamic shape size
   */
  void setMemoryDataReshaped(
      const std::string& op,
      const std::string& tensor,
      size_t size) {
    if (!status.isTensorRegistered(tensor))
      throw status_exception("Tensor not registered.");

    status::TensorPres pres = status.referenceTensor(tensor);
    pres.setReshaped(size);

    // emit memory event
    backend_handle.lock()->submitEvent(events::MemoryEvent(
        op, tensor, pres.getSize(), events::MemoryEventType::reshape, stage));
  }

  /**
   * @brief Set the memory data is allocated.
   * @param op operator name
   * @param tensor tensor name
   * @param address tensor address
   */
  void setMemoryDataAllocated(
      const std::string& op,
      const std::string& tensor,
      void* address) {
    if (!status.isTensorRegistered(tensor))
      throw status_exception("Tensor not registered.");

    status::TensorPres pres = status.referenceTensor(tensor);
    pres.setAllocated(address);
    if (pres.hasFragment())
      op_executor.fragment(pres);

    layout.recordMemoryAllocateEvent(address, pres.getSize(), tensor);

    // emit memory event
    backend_handle.lock()->submitEvent(events::MemoryEvent(
        op, tensor, pres.getSize(), events::MemoryEventType::allocate, stage));
  }

  void setMemoryDataAllocated(const std::string& tensor, void* address) {
    setMemoryDataAllocated("", tensor, address);
  }

  /**
   * @brief  Assure the data of a operator is moved to the device memory before
   * the operator is launched.
   * @return MemoryRequest object
   */
  Request createRequest(const std::string& op = "") {
    // if (!status.isOperatorRegistered(op)) throw status_exception("Operator
    // not registered.");
    Request re(*this, op, stage);
    return re;
  }

  /**
   * @brief Wait for available memory. Memory insufficent is an emergency event,
   * hence an independent method is provided.
   * @param size Memory size that should be released.
   * @note Currently this method adopts a FIFO strategy that the firstly
   * forward-propagating operator will be firstly released.
   */
  void waitMemory(size_t size) {
    size_t released_size = 0;

    for (auto& op_name : status.getExecutionOrder()) {
      status::OperatorView op_view = status.tryReferenceOperator(op_name);
      if (!op_view.isReferenced())
        continue;
      status::OperatorPres op_pres = op_view.reference();
      // Forward propagation and backward propagation share the same set of
      // operators.
      if (op_pres.isBackwardPropagation())
        continue;

      for (auto& s : op_pres.getTensors()) {
        // Try to release memory from tensors.
        std::string tensor_name = s;
        while (true) {
          status::TensorView tensor_view =
              status.tryReferenceTensor(tensor_name);
          if (!tensor_view.isReferenced())
            break;
          status::TensorPres tensor_pres = tensor_view.reference();
          // // Do not swap out persistant or transient tensors.
          // if (tensor_pres.isPersistant() || tensor_pres.isTransient()) break;
          // Do not swap out tensors that already host-only.
          if (!tensor_pres.isDeviceLocated())
            break;

          // Prepare to swap out this tensor.
          uint8_t* device_address_e =
              (uint8_t*)(tensor_pres.getLastSection().device_address) +
              tensor_pres.getLastSection().size;
          if (tensor_pres.getFragment().status ==
              status::MemoryStatusType::empty)
            device_address_e += tensor_pres.getFragment().size;
          int releasing_b = tensor_pres.getDeviceSize();
          if (tensor_pres.getFragment().status ==
              status::MemoryStatusType::empty)
            releasing_b += tensor_pres.getFragment().size;
          int releasing_size = releasing_b;
          if (releasing_size + released_size > size)
            releasing_size = size - released_size;
          op_executor.swapOut(tensor_pres, releasing_size);
          int releasing_e = tensor_pres.getDeviceSize();
          if (tensor_pres.getFragment().status ==
              status::MemoryStatusType::empty)
            releasing_e += tensor_pres.getFragment().size;

          std::string op_name = tensor_pres.getOperatorName();
          if (callbacks.count(CallbackStage::postSwapOut))
            callbacks.at(CallbackStage::postSwapOut)(
                tensor_name, tensor_pres.getSection(0).host_address);
          (*logger) << LogLevel::debug << "Operator " << op_name << ": tensor "
                    << tensor_name << " swapped out. (Memory insufficience)";
          logger->flush();

          backend_handle.lock()->submitEvent(events::MemoryEvent(
              op_name,
              tensor_name,
              releasing_b - releasing_e,
              events::MemoryEventType::swapout,
              stage));

          released_size += releasing_b - releasing_e;
          if (released_size >= size)
            break;

          assert(releasing_e == 0);

          if (!layout.isSectionExist(device_address_e))
            break;
          layout::MemorySection section =
              layout.getMemorySection(device_address_e);
          if (section.name == "") {
            released_size += section.size;
            if (released_size >= size)
              break;
            // Memory demand unmet.
            device_address_e += section.size;
            if (!layout.isSectionExist(device_address_e))
              break;
            section = layout.getMemorySection(device_address_e);
          }
          tensor_name = section.name;
        }
        if (released_size >= size)
          break;
        // Releasing failed. Try next tensor.
        released_size = 0;
      }
      if (released_size >= size)
        break;
    }

    if (released_size >= size) {
      // (*logger) << LogLevel::info << "Memory insufficient, mori releases " <<
      // released_size << " of memory."; logger->flush();
    } else {
      // Mori wait memory failed.
      (*logger) << LogLevel::info << "Mori memory releasing failed, "
                << " unmet.";
      logger->flush();
    }
  }

  /**
   * @brief Set the memory data is freed.
   * @param op operator name
   * @param tensor tensor name
   */
  void setMemoryDataFreed(const std::string& op, const std::string& tensor) {
    if (!status.isTensorRegistered(tensor))
      throw status_exception("Operator or tensor not registered.");

    status::TensorPres pres = status.referenceTensor(tensor);
    op_executor.freeHost(pres, pres.getHostSize());
    const status::MemorySection* section = (&pres.getFirstSection());
    do {
      switch (section->status) {
        case status::MemoryStatusType::empty:
        case status::MemoryStatusType::device: {
          void* device_address = section->device_address;
          pres.setDeviceFreed(section->offset);
          layout.recordMemoryFreeEvent(device_address);
        }
        case status::MemoryStatusType::none:
          break;
        default:
          // coexist, host
          assert(0);
          break;
      }

      if (pres.isMergeable(section->offset))
        pres.merge(section->offset);
      const status::MemorySection* section_prev = section->prev();
      if (section_prev != nullptr && pres.isMergeable(section_prev->offset))
        section = &(pres.merge(section->offset));

      section = section->next();
    } while (section != nullptr);

    assert(pres.getSectionCount() == 1);
    assert(!pres.isDeviceLocated());

    // emit memory event
    backend_handle.lock()->submitEvent(events::MemoryEvent(
        op, tensor, pres.getSize(), events::MemoryEventType::free, stage));
  }

  void setMemoryDataFreed(const std::string& tensor) {
    setMemoryDataFreed("", tensor);
  }

  ~MemorySession() = default;
}; // struct MemorySession

using MemoryRequest = MemorySession::Request;

} // namespace mori

namespace mori {

/**
 * Frontend of Mori, provided to the DL system.
 * In this version only single-thread graph execution is considered.
 */
struct Frontend {
 protected:
  struct Impl {
    Frontend& frontend;
    Impl(Frontend& _frontend) : frontend(_frontend) {}

    virtual void setMemoryManager(MemoryManager* _mem_manager) = 0;
    virtual void setLogger(Logger* _logger) = 0;

    virtual void init() = 0;
    virtual bool isInited() const noexcept = 0;

    virtual void registerTensor(const status::Tensor& tensor) = 0;
    virtual void registerOperator(const status::Operator& operator_status) = 0;
    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) = 0;

    virtual void setEntry(const std::string& _op) = 0;

    virtual void setCallback(
        CallbackStage stage,
        const std::function<int(const std::string& tensor, void* ptr)>&
            callback) = 0;

    virtual void start() = 0;
    virtual bool isStarted() const noexcept = 0;

    virtual MemorySession& getSession() = 0;

    virtual void updateSchedule() = 0;

    virtual void unregisterTensor(const std::string& tensor) = 0;
    virtual void unregisterOperator(const std::string& op) = 0;

    virtual void stop() = 0;

    virtual void terminate() = 0;
  }; // struct Impl

  struct UninitedImpl final : public Impl {
    UninitedImpl(Frontend& _frontend) : Impl(_frontend) {}

    virtual void setMemoryManager(MemoryManager* _mem_manager) override {
      if (_mem_manager == nullptr)
        throw status_exception("Memory manager assigned to null pointer.");

      frontend.mem_manager = _mem_manager;
      frontend.executor.setMemoryManager(_mem_manager);
      frontend.session.setMemoryManager(_mem_manager);

      frontend.memory_status.setMemoryInfo(_mem_manager->getMemoryInfo());
      frontend.memory_layout.setMemoryInfo(_mem_manager->getMemoryInfo());
    }
    virtual void setLogger(Logger* _logger) override {
      if (_logger == nullptr)
        frontend.logger = &frontend.empty_logger;
      frontend.logger = _logger;

      frontend.backend_handle->setLogger(frontend.logger);
      frontend.session.setLogger(frontend.logger);
      frontend.executor.setLogger(frontend.logger);
    }

    virtual void init() override {
      // Forward callpath initialization failed.
      if (frontend.backend_handle == nullptr)
        throw status_exception("Backend not inited.");
      // Backward callpath initialization failed.
      if (frontend.mem_manager == nullptr)
        throw status_exception("Memory manager not assigned.");

      frontend.backend_handle->init();

      frontend.impl = &frontend.inited_impl;
      frontend.logger->submit(LogLevel::info, "Mori frontend inited.");
    }
    virtual bool isInited() const noexcept override {
      return false;
    }

    virtual void registerTensor(const status::Tensor& tensor) override {
      (*frontend.logger) << LogLevel::error << "Registering tensor "
                         << tensor.getName()
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }
    virtual void registerOperator(
        const status::Operator& operator_status) override {
      (*frontend.logger) << LogLevel::error << "Registering operator "
                         << operator_status.getName()
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }
    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) override {
    //     (*logger)<<LogLevel::error<<"Updating operator "<<op<<" while
    //     frontend not initialized."; logger->flush(); throw
    //     uninited_exception();
    // }
    virtual void setEntry(const std::string& _op) override {
      (*frontend.logger) << LogLevel::error << "Setting entry operator " << _op
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void setCallback(
        CallbackStage stage,
        const std::function<int(const std::string& tensor, void* ptr)>&
            callback) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting callbacks while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void start() override {
      (*frontend.logger) << LogLevel::error
                         << "Starting uninitialized frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }
    virtual bool isStarted() const noexcept override {
      return false;
    }

    virtual MemorySession& getSession() override {
      (*frontend.logger)
          << LogLevel::error
          << "Referencing to session from uninitialized frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void updateSchedule() override {
      (*frontend.logger) << LogLevel::error
                         << "Updating schedule while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      (*frontend.logger) << LogLevel::error << "Unregistering tensor " << tensor
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }
    virtual void unregisterOperator(const std::string& op) override {
      (*frontend.logger) << LogLevel::error << "Unregistering operator " << op
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void stop() override {
      (*frontend.logger) << LogLevel::error
                         << "Stopping uninitialized frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void terminate() override {
      (*frontend.logger) << LogLevel::error
                         << "Terminating uninitialized frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }
  }; // struct UninitedImpl

  struct InitedImpl final : public Impl {
    InitedImpl(Frontend& _frontend) : Impl(_frontend) {}

    virtual void setMemoryManager(MemoryManager* _mem_manager) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting memory manager for initialized frontend.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual void setLogger(Logger* _logger) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting logger for initialized frontend.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual void init() override {
      (*frontend.logger) << LogLevel::error
                         << "Initializing frontend that already inited.";
      frontend.logger->flush();
      throw inited_exception();
    }
    virtual bool isInited() const noexcept override {
      return true;
    }

    virtual void registerTensor(const status::Tensor& tensor) override {
      frontend.memory_status.registerTensor(tensor);
      (*frontend.logger) << LogLevel::debug << "Tensor " << tensor.getName()
                         << " registered.";
      frontend.logger->flush();
    }

    virtual void registerOperator(
        const status::Operator& operator_status) override {
      frontend.memory_status.registerOperator(operator_status);
      (*frontend.logger) << LogLevel::debug << "Operator "
                         << operator_status.getName() << " registered.";
      frontend.logger->flush();
    }

    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) override {
    //     memory_status.updateOperator(op, tensor_status);
    //     // backend_handle->updateOperator(op, tensor_status);

    //     (*logger)<<LogLevel::debug<<"Operator "<<op<<" updated.";
    //     logger->flush();
    // }

    virtual void setEntry(const std::string& _op) override {
      frontend.memory_status.setEntry(_op);
    }

    virtual void setCallback(
        CallbackStage stage,
        const std::function<int(const std::string& tensor, void* ptr)>&
            callback) override {
      frontend.executor.setCallback(stage, callback);
      frontend.session.setCallback(stage, callback);
    }

    virtual void start() override {
      frontend.backend_handle->submitMemoryStatus(frontend.memory_status);

      frontend.executor.init();
      frontend.backend_handle->start();
      frontend.impl = &frontend.started_impl;
      (*frontend.logger) << LogLevel::debug << "Mori started.";
      frontend.logger->flush();
    }

    virtual bool isStarted() const noexcept override {
      return false;
    }

    virtual MemorySession& getSession() override {
      (*frontend.logger) << LogLevel::error
                         << "Referencing to session from not-started frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void updateSchedule() override {
      (*frontend.logger) << LogLevel::error
                         << "Updating schedule for not-started frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      frontend.memory_status.unregisterTensor(tensor);
      (*frontend.logger) << LogLevel::debug << "Tensor " << tensor
                         << " unregistered.";
      frontend.logger->flush();
    }

    virtual void unregisterOperator(const std::string& op) override {
      frontend.memory_status.unregisterOperator(op);
      (*frontend.logger) << LogLevel::debug << "Operator " << op
                         << " unregistered.";
      frontend.logger->flush();
    }

    virtual void stop() override {
      (*frontend.logger) << LogLevel::error << "Stopping non-started frontend.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void terminate() override {
      frontend.backend_handle->terminate();
      frontend.memory_status.clear();

      frontend.impl = &frontend.uninited_impl;

      frontend.logger->submit(LogLevel::info, "Mori frontend terminated.");
    }
  }; // struct InitedImpl

  struct StartedImpl final : public Impl {
    StartedImpl(Frontend& _frontend) : Impl(_frontend) {}

    virtual void setMemoryManager(MemoryManager* _mem_manager) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting memory manager for started frontend.";
      frontend.logger->flush();
      throw inited_exception();
    }
    virtual void setLogger(Logger* _logger) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting logger for started frontend.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual void init() override {
      (*frontend.logger) << LogLevel::error
                         << "Initializing frontend that already started.";
      frontend.logger->flush();
      throw inited_exception();
    }
    virtual bool isInited() const noexcept override {
      return true;
    }

    virtual void registerTensor(const status::Tensor& tensor) override {
      (*frontend.logger) << LogLevel::error << "Registering tensor "
                         << tensor.getName() << " while frontend started.";
      frontend.logger->flush();
      throw inited_exception();
    }
    virtual void registerOperator(
        const status::Operator& operator_status) override {
      (*frontend.logger) << LogLevel::error << "Registering operator "
                         << operator_status.getName()
                         << " while frontend started.";
      frontend.logger->flush();
      throw inited_exception();
    }

    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) {
    //     if (!inited) {
    //         (*logger)<<LogLevel::error<<"Updating operator "<<op<<" while
    //         frontend not initialized."; logger->flush(); throw
    //         uninited_exception();
    //     }
    // }

    virtual void setEntry(const std::string& _op) override {
      (*frontend.logger) << LogLevel::error << "Setting entry operator " << _op
                         << " while frontend started.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual void setCallback(
        CallbackStage stage,
        const std::function<int(const std::string& tensor, void* ptr)>&
            callback) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting callbacks while frontend started.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual void start() override {
      (*frontend.logger) << LogLevel::error << "Frontend already started.";
      frontend.logger->flush();
      throw inited_exception();
    }

    virtual bool isStarted() const noexcept override {
      return true;
    }

    virtual MemorySession& getSession() override {
      return frontend.session;
    }

    virtual void updateSchedule() override {
      auto&& event_set = frontend.backend_handle->getScheduleEvents();
      for (auto& x : event_set.memory_map.getFragmentInfo()) {
        status::TensorPres pres =
            frontend.memory_status.referenceTensor(x.first);
        pres.setFragment(x.second);
      }

      frontend.executor.updateSchedule(event_set);

      (*frontend.logger) << LogLevel::debug << "Schedule updated.";
      frontend.logger->flush();
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      (*frontend.logger) << LogLevel::error << "Unregistering tensor " << tensor
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void unregisterOperator(const std::string& op) override {
      (*frontend.logger) << LogLevel::error << "Unregistering operator " << op
                         << " while frontend not initialized.";
      frontend.logger->flush();
      throw uninited_exception();
    }

    virtual void stop() override {
      frontend.executor.terminate();
      frontend.backend_handle->stop();
      frontend.impl = &frontend.inited_impl;
    }

    virtual void terminate() override {
      stop();
      frontend.impl = &frontend.inited_impl;
      // Terminate should switch the frontend to uninited state.
      frontend.impl->terminate();
    }
  }; // struct InitedStartedImpl

 protected:
  UninitedImpl uninited_impl;
  InitedImpl inited_impl;
  StartedImpl started_impl;

  Impl* impl = &uninited_impl;

 protected:
  Context context;

  std::shared_ptr<BackendHandle> backend_handle = nullptr;

  MemoryManager* mem_manager = nullptr;
  status::MemoryStatus memory_status;
  layout::MemoryLayout memory_layout;

  // Each frontend holds one memory session.
  MemorySession session;
  MemoryScheduleExecutor executor;

  Logger empty_logger;
  Logger* logger = nullptr;

 public:
  Frontend(const Context& _context)
      : uninited_impl(*this),
        inited_impl(*this),
        started_impl(*this),
        context(_context),
        session(_context, executor, memory_status, memory_layout),
        executor(_context, memory_status, memory_layout) {
    // Set backend
    backend_handle = make_backend_handle(_context);

    session.setBackendHandle(backend_handle);
    executor.setBackendHandle(backend_handle);

    logger = &empty_logger;
  }

  /**
   * @brief Set memory manager for memory swapping.
   * @param _mem_manager Pointer to memory manager used in DL framework.
   */
  inline void setMemoryManager(MemoryManager* _mem_manager) {
    impl->setMemoryManager(_mem_manager);
  }
  /**
   * @brief Set logger.
   * @param _logger Pointer to logger used in DL framework.
   */
  inline void setLogger(Logger* _logger) {
    impl->setLogger(_logger);
  }

  /**
   * @brief Init mori frontend.
   * @note Mutiple call to this method would lead to mori::inited_exception.
   */
  inline void init() {
    impl->init();
  }
  /**
   * @brief If mori frontend inited.
   * @return If mori frontend inited.
   */
  inline bool isInited() const {
    return impl->isInited();
  }

  /**
   * @brief Register a tensor in DL procedure.
   * @param tensor Information of the tensor to be registered.
   */
  inline void registerTensor(const status::Tensor& tensor) {
    impl->registerTensor(tensor);
  }

  /**
   * @brief Register an operator in this graph.
   * @param operator_status Information of the operator to be registered.
   * @note The register order will be regarded as the execution order.
   */
  inline void registerOperator(const status::Operator& operator_status) {
    impl->registerOperator(operator_status);
  }

  /**
   * @brief Update operator.
   * @param operator_status Information of the operator to be updated.
   */
  // inline void updateOperator(const std::string& op, const status::Tensor&
  // tensor_status) { impl->updateOperator(op, tensor_status); }

  /**
   * @brief Set entry operator of the DL computation graph.
   * @param op Entry operator name.
   */
  inline void setEntry(const std::string& op) {
    impl->setEntry(op);
  }

  /**
   * @brief Set callback functions for memory swapping. postSwapOut and
   * postSwapIn supported.
   * @param stage Stage of the callback function.
   * @param callback Callback function.
   * @see enum struct mori::CallbackStage
   */
  inline void setCallback(
      CallbackStage stage,
      const std::function<int(const std::string& tensor, void* ptr)>&
          callback) {
    impl->setCallback(stage, callback);
  }

  /**
   * @brief Start mori frontend. Mori session and background executor will be
   * started.
   * @note Mutiple call to this method would lead to mori::inited_exception.
   */
  inline void start() {
    impl->start();
  }
  /**
   * @brief If mori frontend started.
   * @return If mori frontend started.
   */
  inline bool isStarted() const noexcept {
    return impl->isStarted();
  }

  /**
   * @brief Reference to Mori memory swapping session.
   * @return Reference to Mori memory swapping session.
   */
  inline MemorySession& getSession() {
    return impl->getSession();
  }

  /**
   * @brief Update current memory swapping schedule
   */
  inline void updateSchedule() {
    impl->updateSchedule();
  }

  /**
   * @brief Unregister a tensor in DL procedure.
   * @param tensor Tensor to be unregistered.
   */
  inline void unregisterTensor(const std::string& tensor) {
    impl->unregisterTensor(tensor);
  }

  /**
   * @brief Unregister an operator in this graph.
   * @param op Operator to be unregistered.
   */
  inline void unregisterOperator(const std::string& op) {
    impl->unregisterOperator(op);
  }

  /**
   * @brief Stop mori frontend. Mori session and background executor will be
   * stopped.
   * @note Mutiple call to this method would lead to mori::uninited_exception.
   */
  inline void stop() {
    impl->stop();
  }

  /**
   * @brief Terminate mori frontend.
   * @note Mutiple call to this method would lead to mori::uninited_exception.
   */
  inline void terminate() {
    impl->terminate();
  }

  ~Frontend() {
    if (impl != &uninited_impl)
      impl->terminate();

    backend_handle.reset();
    mem_manager = nullptr;
    logger = nullptr;
  }

}; // struct Frontend

using MemorySwappingManager = Frontend;

} // namespace mori
