#pragma once

/**
 * libMori
 * Copyright Xin 2022-2023.
 */

#include <cassert>
#include <chrono>
#include <sstream>
#include <string>

namespace mori {

enum struct ApplicationStage {
  all,
  forward,
  backward,
  update
}; // enum struct ApplicationStage

enum struct Direction { prev, post }; // enum struct Direction

namespace utils {

static std::string get_application_stage_str(ApplicationStage stage) {
  switch (stage) {
    case ApplicationStage::all:
      return "all";
    case ApplicationStage::forward:
      return "forward";
    case ApplicationStage::backward:
      return "backward";
    case ApplicationStage::update:
      return "update";
  }
  return "";
}

} // namespace utils
} // namespace mori

#include <chrono>
#include <cmath>
#include <sstream>
#include <string>

namespace mori {
namespace utils {

static long get_timestamp_val(
    const std::chrono::steady_clock::time_point& timestamp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             timestamp.time_since_epoch())
      .count();
}

inline static void* address_offset(void* address, size_t size) {
  return (uint8_t*)address + size;
}

inline static size_t address_distance(void* dst, void* src) {
  return (uint8_t*)dst - (uint8_t*)src;
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

#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

namespace mori {

/**
 * LogLevel
 * Describe the level of the log.
 */
enum struct LogLevel { debug, info, warning, error }; // enum struct LogLevel

static std::string get_log_level_str(LogLevel level) {
  switch (level) {
    case LogLevel::debug:
      return "[Debug]  ";
    case LogLevel::warning:
      return "[Warning]";
    case LogLevel::error:
      return "[Error]  ";
    default:
      // info
      return "[Info]   ";
  }
}

/**
 * Logger
 * Basic logger interface
 */
struct Logger {
 protected:
  typedef Logger& (*func)(Logger&);

 protected:
  std::unordered_map<std::thread::id, LogLevel> default_levels;
  std::unordered_map<std::thread::id, std::ostringstream> sls;
  mutable std::shared_mutex dm;
  mutable std::shared_mutex tm;

  std::ostringstream sg;
  mutable std::mutex sm;

  template <typename T>
  void submitInternal(const T& info) {
    std::shared_lock<std::shared_mutex> l{tm};
    auto p = sls.find(std::this_thread::get_id());
    if (p == sls.end()) {
      l.unlock();
      std::unique_lock<std::shared_mutex> lu{tm};
      p = sls.emplace(std::this_thread::get_id(), "").first;
      lu.unlock();
      l.lock();
    }

    auto& sl = p->second;
    l.unlock();
    sl << info;
  }

  virtual void log(LogLevel level, const std::string& log) {}

 public:
  inline void setDefaultLogLevel(LogLevel level) {
    std::shared_lock<std::shared_mutex> l{dm};
    auto p = default_levels.find(std::this_thread::get_id());
    if (p == default_levels.end()) {
      l.unlock();
      std::unique_lock<std::shared_mutex> lu{dm};
      p = default_levels.emplace(std::this_thread::get_id(), level).first;
      lu.unlock();
      l.lock();
    }
    default_levels.at(std::this_thread::get_id()) = level;
  }
  inline LogLevel getDefaultLogLevel() const {
    std::shared_lock<std::shared_mutex> l{dm};
    auto p = default_levels.find(std::this_thread::get_id());
    if (p == default_levels.end())
      return LogLevel::debug;
    return p->second;
  }

  void flush(LogLevel level) {
    std::unique_lock<std::mutex> ls{sm};

    std::shared_lock<std::shared_mutex> lt{tm};
    auto& sl = sls[std::this_thread::get_id()];
    lt.unlock();

    sg << get_log_level_str(level) << " "
       << std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count()
       << " " << sl.str();
    sg.flush();
    std::string entry = sg.str();
    sg.str("");
    ls.unlock();

    log(level, entry);
    sl.str("");
  }
  void flush() {
    flush(getDefaultLogLevel());
  }

  template <typename T>
  void submit(LogLevel level, const T& entry) {
    submitInternal(entry);
    flush(level);
  };
  template <typename T>
  inline void submit(const T& entry) {
    submit(getDefaultLogLevel(), entry);
  }

  Logger& operator<<(LogLevel level) {
    setDefaultLogLevel(level);
    return *this;
  }
  Logger& operator<<(func _func) {
    return _func(*this);
  }
  template <typename T>
  Logger& operator<<(const T& info) {
    submitInternal(info);
    return *this;
  }

  void clear() {
    std::unique_lock<std::mutex> ls{sm};
    sg.str("");
    ls.unlock();

    std::unique_lock<std::shared_mutex> lt{tm};
    sls.clear();
    lt.unlock();
  }

}; // struct Logger

static Logger& endl(Logger& logger) {
  logger.flush();
  return logger;
}

/**
 * StdIOLogger
 * Submit logs to std streams
 */
struct StdIOLogger : public Logger {
 protected:
  virtual void log(LogLevel level, const std::string& entry) override {
    switch (level) {
      case LogLevel::warning:
        std::clog << entry << std::endl;
        break;
      case LogLevel::error:
        std::cerr << entry << std::endl;
        break;
      default:
        // debug, info
        std::cout << entry << std::endl;
        break;
    }
  }

}; // struct StdIOLogger

} // namespace mori

namespace mori {
namespace events {

enum struct MemoryEventType {
  allocate,
  write,
  read,
  access,
  swapin,
  swapout,
  free,
  reshape
}; // enum struct MemoryEventType

namespace utils {
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
} // namespace utils

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
    ss << "Timestamp: " << mori::utils::get_timestamp_val(timestamp)
       << " operator: " << op << " tensor: " << tensor << " size: " << size
       << " type: " << utils::get_event_type_str(type)
       << " stage: " << mori::utils::get_application_stage_str(stage);
    return ss.str();
  }

}; // struct MemoryEvents

static Logger& operator<<(Logger& logger, const MemoryEvent& event) {
  logger << static_cast<std::string>(event);
  return logger;
}

} // namespace events
} // namespace mori

#include <chrono>
#include <string>

namespace mori {
namespace events {

enum struct ExecutionEventType {
  request,
  release,
  execution
}; // enum struct ExecutionEventType

namespace utils {
static std::string get_event_type_str(ExecutionEventType type) {
  switch (type) {
    case ExecutionEventType::request:
      return "request";
    case ExecutionEventType::release:
      return "release";
    default:
      return "execution";
  }
}
} // namespace utils

struct ExecutionEvent {
  std::string op;
  ExecutionEventType type;
  ApplicationStage stage;
  std::chrono::steady_clock::time_point timestamp;

  ExecutionEvent() {
    op = "";
    type = ExecutionEventType::execution;
    stage = ApplicationStage::all;
    timestamp = std::chrono::steady_clock::now();
  }

  ExecutionEvent(
      const std::string& _op,
      ExecutionEventType _type,
      ApplicationStage _stage,
      const std::chrono::steady_clock::time_point& _timestamp) {
    op = _op;
    type = _type;
    stage = _stage;
    timestamp = _timestamp;
  }

  ExecutionEvent(
      const std::string& _op,
      ExecutionEventType _type,
      ApplicationStage _stage) {
    op = _op;
    type = _type;
    stage = _stage;
    timestamp = std::chrono::steady_clock::now();
  }

  ExecutionEvent(const ExecutionEvent& event) = default;
  ExecutionEvent& operator=(const ExecutionEvent& event) = default;

  bool operator<(const ExecutionEvent& event) const {
    return timestamp < event.timestamp;
  }

  operator std::string() const {
    std::stringstream ss;
    ss << "Timestamp: " << mori::utils::get_timestamp_val(timestamp)
       << " operator: " << op << " type: " << utils::get_event_type_str(type)
       << " stage: " << mori::utils::get_application_stage_str(stage);
    return ss.str();
  }
}; // struct ExecutionEvent

static Logger& operator<<(Logger& logger, const ExecutionEvent& event) {
  logger << static_cast<std::string>(event);
  return logger;
}

} // namespace events
} // namespace mori

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <map>
#include <set>
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
  struct Block {
    void* address = nullptr;
    size_t size = 0;
  }; // inner struct Block

  struct Device {
    std::string type;

    Block common_block;
    Block persistent_block;
    Block transient_block;

    size_t total_size = 512;
    size_t align_size = 512;
    size_t reserved_size = 0;
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
  re.device.align_size = 256; // 256 B
  re.host.type = "cpu";
  re.host.total_size = host;
  return re;
}

} // namespace mori

#include <string>

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

struct memory_address_invalid : public memory_exception {

}; // struct memory_address_invalid

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
namespace layout {

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

struct MemoryMap;

struct MemoryMapBuilder final {
  std::unordered_map<std::string, Region> regions;
  std::vector<Layer> layers;

  MemoryInfo memory_info;

  int current_layer = 0;

  MemoryMapBuilder() {
    layers.emplace_back();
  }

  inline void setMemoryInfo(const MemoryInfo& _memory_info) {
    if (layers.size() != 1 || !layers[0].regions.empty())
      throw inited_exception("Memory map on built.");
    memory_info = _memory_info;
    layers[0].size = _memory_info.device.common_block.size;
  }
  inline const MemoryInfo& getMemoryInfo() const noexcept {
    return memory_info;
  };

  inline void createLayer() {
    layers.emplace_back(memory_info.device.common_block.size);
    ++current_layer;
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
  inline const Layer& getCurrentLayer() const {
    return getLayer(current_layer);
  }
  inline std::vector<Layer>& getLayers() {
    return layers;
  }
  inline const std::vector<Layer>& getLayers() const {
    return layers;
  }
  inline const std::vector<size_t>& getSections(
      const std::string& tensor) const {
    return regions.at(tensor).sections;
  }
  inline size_t getFragmentSize(const std::string& tensor) const {
    return regions.at(tensor).fragment_size;
  }
  inline std::unordered_map<std::string, Region>& getRegions() {
    return regions;
  }
  inline const std::unordered_map<std::string, Region>& getRegions() const {
    return regions;
  }

  std::unordered_map<std::string, size_t> getFragmentInfo() const {
    std::unordered_map<std::string, size_t> re;
    for (auto& x : regions) {
      if (x.second.fragment_size != 0)
        re.emplace(x.first, x.second.fragment_size);
    }
    return re;
  }

  inline MemoryMap build();

  void clear() {
    regions.clear();
    layers.clear();
  }
}; // struct MemoryMapBuilder

/**
 * Describe the layout for all tensors in the memory.
 */
struct MemoryMap {
 private:
  std::unordered_map<std::string, Region> regions;
  std::vector<Layer> layers;

  MemoryInfo memory_info;

  int current_layer = 0;

 public:
  MemoryMap() = default;
  MemoryMap(const MemoryMapBuilder& builder)
      : regions(builder.regions),
        layers(builder.layers),
        memory_info(builder.memory_info),
        current_layer(builder.current_layer) {}

  inline const MemoryInfo& getMemoryInfo() const noexcept {
    return memory_info;
  };

  inline const Layer& referenceLayer(int _layer) const {
    return layers.at(_layer);
  }
  inline const Layer& referenceLayer() const {
    return referenceLayer(current_layer);
  }
  inline const Region& referenceRegion(const std::string& _region) const {
    return regions.at(_region);
  }

  inline int getLayersCount() const {
    return layers.size();
  }
  inline const Layer& getLayer(int layer) const {
    return layers[layer];
  }
  inline const Layer& getCurrentLayer() const {
    return getLayer(current_layer);
  }
  inline const std::vector<Layer>& getLayers() const {
    return layers;
  }
  inline const std::vector<size_t>& getSections(
      const std::string& tensor) const {
    return regions.at(tensor).sections;
  }
  inline size_t getFragmentSize(const std::string& tensor) const {
    return regions.at(tensor).fragment_size;
  }
  inline const std::unordered_map<std::string, Region>& getRegions() const {
    return regions;
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

MemoryMap MemoryMapBuilder::build() {
  return MemoryMap(*this);
}

enum struct MemoryBlockType {
  common,
  persistent,
  transient
}; // enum struct MemoryBlockType

struct MemoryRegion final {
  std::string name = ""; // Tensor information

  void* address = nullptr;
  size_t size = 0;

  bool allocated = false;
}; // struct MemoryRegion

struct Block final {
  MemoryBlockType type;
  std::map<void*, MemoryRegion> regions;
  mutable std::shared_mutex m;
  size_t total_size;

  Block(MemoryBlockType block_type, void* address, size_t size) {
    type = block_type;
    total_size = size;
    MemoryRegion s;
    s.address = address;
    s.size = size;
    regions.emplace(s.address, s);
  }
  Block(const Block& block) {
    type = block.type;
    regions = block.regions;
    total_size = block.total_size;
  }
  Block(Block&& block) {
    type = block.type;
    regions = std::move(block.regions);
    total_size = block.total_size;
  }
}; // struct Block

struct MemoryDefragmentationExecutor;

struct MemoryLayout final {
 private:
  friend struct MemoryDefragmentationExecutor;

 private:
  std::map<void*, Block> blocks;
  // std::shared_mutex m;

  size_t align_size;

 protected:
  inline std::map<void*, Block>::const_iterator locateMemoryBlock(
      void* address) const {
    auto bp = blocks.upper_bound(address);
    if (bp == blocks.begin())
      return blocks.cend();
    return std::prev(bp);
  }
  inline std::map<void*, Block>::iterator locateMemoryBlock(void* address) {
    auto bp = blocks.upper_bound(address);
    if (bp == blocks.begin())
      return blocks.end();
    return std::prev(bp);
  }

 public:
  MemoryLayout() = default;

  inline void setMemoryInfo(const MemoryInfo& info) {
    assert(blocks.empty());

    blocks.emplace(
        info.device.common_block.address,
        Block(
            MemoryBlockType::common,
            info.device.common_block.address,
            info.device.common_block.size));
    blocks.emplace(
        info.device.persistent_block.address,
        Block(
            MemoryBlockType::persistent,
            info.device.persistent_block.address,
            info.device.persistent_block.size));
    blocks.emplace(
        info.device.transient_block.address,
        Block(
            MemoryBlockType::transient,
            info.device.transient_block.address,
            info.device.transient_block.size));

    align_size = info.device.align_size;
  }

  bool isRegionExist(void* address, Direction direction = Direction::post)
      const {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      return false;
    std::shared_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    if (direction == Direction::post)
      return regions.find(address) != regions.end();
    else
      return regions.find(address) != regions.begin();
  }
  MemoryRegion getMemoryRegion(
      void* address,
      Direction direction = Direction::post) const {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_unmanaged();
    std::shared_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    auto sp = regions.find(address);
    if (direction == Direction::post) {
      if (sp == regions.end())
        throw memory_unmanaged();
      return sp->second;
    } else {
      if (sp == regions.begin())
        throw memory_unmanaged();
      return std::prev(sp)->second;
    }
  }

  bool isPersistent(void* address) const {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_unmanaged();
    std::shared_lock<std::shared_mutex> l{bp->second.m};
    return bp->second.type == MemoryBlockType::persistent;
  }
  bool isTransient(void* address) const {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_unmanaged();
    std::shared_lock<std::shared_mutex> l{bp->second.m};
    return bp->second.type == MemoryBlockType::transient;
  }
  bool isCommon(void* address) const {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_unmanaged();
    std::shared_lock<std::shared_mutex> l{bp->second.m};
    return bp->second.type == MemoryBlockType::common;
  }

  void recordMemoryAllocateEvent(
      void* address,
      size_t size,
      const std::string& tensor,
      size_t alignment) {
    if (address == nullptr)
      throw memory_address_invalid();
    // Since MemoryLayout is only a recorder of memory layout information, no
    // need to implement for malloc and salloc seperately.
    if (size == 0)
      return recordMemoryAllocateEvent(address, alignment, tensor, alignment);

    auto bp = blocks.upper_bound(address);
    assert(bp != blocks.begin());
    --bp;

    std::unique_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    auto p = regions.begin();
    while (p != regions.end() &&
           utils::address_offset(p->first, p->second.size) <= address)
      ++p;
    if (p == regions.end() || p->first > address || p->second.allocated)
      throw memory_allocated(address);
    if (utils::address_offset(p->first, p->second.size) <
        utils::address_offset(address, size))
      throw memory_operation_invalid(
          address,
          "Memory cannot be allocated at specificied address with size.");

    // The original unallocated space should be splited to three parts.
    if (p->first < address) {
      // Left part exists.
      MemoryRegion s;
      s.address = address;
      s.size = (uint8_t*)p->first - (uint8_t*)address + p->second.size;
      auto q = regions.emplace(address, s);
      assert(q.second);
      p->second.size = (uint8_t*)address - (uint8_t*)p->first;
      p = q.first;
    }
    // Now p->first == address
    if (p->second.size > size) {
      // Right part exists.
      // Create empty region
      MemoryRegion s;
      s.address = (uint8_t*)address + size;
      s.size = p->second.size - size;
      auto q = regions.emplace(s.address, s);
      assert(q.second);
      p->second.size = size;
    }
    p->second.name = tensor;
    p->second.allocated = true;
  }
  void recordMemoryAllocateEvent(
      void* address,
      size_t size,
      const std::string& tensor) {
    if (address == nullptr)
      throw memory_address_invalid();
    if (!utils::memory_address_aligned(address, align_size))
      throw memory_exception(address, "Memory address not aligned.");
    size_t aligned_size = utils::get_memory_aligned_size(size, align_size);
    if (aligned_size == 0)
      aligned_size = align_size;
    recordMemoryAllocateEvent(address, aligned_size, tensor, align_size);
  }
  void recordMemoryFreeEvent(void* address, const std::string& tensor = "") {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_not_allocated(address);

    std::unique_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    // Check if allocated device memory.
    auto p = regions.find(address);
    // Device memory not allocated.
    if (p == regions.end() || !p->second.allocated)
      throw memory_not_allocated(address);
    p->second.name = "";
    p->second.allocated = false;

    // Merging free regions.
    auto prev = p;
    auto post = p;
    ++post;
    if (post != regions.end() && !post->second.allocated) {
      p->second.size += post->second.size;
      regions.erase(post);
    }

    if (p == regions.begin())
      return;
    --prev;
    if (!prev->second.allocated) {
      prev->second.size += p->second.size;
      regions.erase(p);
    }
  }
  void recordMemorySplitEvent(void* address, size_t size) {
    if (address == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(address);
    if (bp == blocks.end())
      throw memory_not_allocated(address);

    std::unique_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    auto p = regions.find(address);
    if (p == regions.end() || !p->second.allocated)
      throw memory_not_allocated(address);
    if (p->second.size <= size)
      throw memory_operation_invalid(
          address, "Memory section equals or be smaller than spliting size.");

    MemoryRegion s = p->second;
    s.address = utils::address_offset(address, size);
    s.size -= size;
    regions.emplace(s.address, s);
    p->second.size = size;
  }
  void recordMemoryMergeEvent(void* left, void* right) {
    if (left == nullptr || right == nullptr)
      throw memory_address_invalid();
    auto bp = locateMemoryBlock(left);
    if (bp == blocks.end())
      throw memory_not_allocated(left);

    std::unique_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;
    auto q = regions.find(left);
    if (q == regions.end() || !q->second.allocated)
      throw memory_not_allocated(
          left, "Memory for left section not allocated.");
    auto p = q++;
    if (q == regions.end() || q->first != right || !q->second.allocated)
      throw memory_not_allocated(
          right, "Memory for right section not allocated.");
    if ((uint8_t*)left + p->second.size != (uint8_t*)right)
      throw memory_operation_invalid(left, "Memory sections not continuous.");

    p->second.size += q->second.size;
    regions.erase(q);
  }

}; // struct MemoryLayout

} // namespace layout
} // namespace mori

namespace mori {
namespace events {

enum struct ScheduleEventType {
  allocate,
  copyin,
  copyout,
  swapin,
  swapout,
  freedev,
  freehost,
  free
}; // enum struct ScheduleEventType

namespace utils {

static std::string get_schedule_event_type_str(ScheduleEventType type) {
  switch (type) {
    case ScheduleEventType::allocate:
      return "allocate";
    case ScheduleEventType::copyin:
      return "copyin";
    case ScheduleEventType::copyout:
      return "copyout";
    case ScheduleEventType::swapin:
      return "swapin";
    case ScheduleEventType::swapout:
      return "swapout";
    case ScheduleEventType::freedev:
      return "freedev";
    case ScheduleEventType::freehost:
      return "freehost";
    case ScheduleEventType::free:
      return "free";
    default:
      break;
  }
  assert(0);
  return "";
}

} // namespace utils

struct ScheduleEvent final {
  std::string operator_name = "";
  std::string tensor_name = "";
  size_t size = 0;

  ScheduleEventType type = ScheduleEventType::allocate;
  std::string postop = ""; // For execution-triggered events, the event should
                           // be executed after executing postop.
  long timepoint = 0; // For timepoing-triggered events, the event should be
                      // executed after specificied timepoint.

  bool instant = false;

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
      const std::string& _postop,
      bool _instant = false)
      : operator_name(_op_name),
        tensor_name(_tensor_name),
        size(_size),
        type(_event_type),
        postop(_postop),
        instant(_instant) {}
  ScheduleEvent(
      const std::string& _op_name,
      const std::string& _tensor_name,
      size_t _size,
      ScheduleEventType _event_type,
      long _timepoint,
      bool _instant = false)
      : operator_name(_op_name),
        tensor_name(_tensor_name),
        size(_size),
        type(_event_type),
        timepoint(_timepoint),
        instant(_instant) {}
}; // struct ScheduleEvent

struct StageScheduleEvents {
  std::unordered_map<std::string, std::vector<ScheduleEvent>> execution;
  std::vector<ScheduleEvent> timepoint;
}; // struct StageScheduleEvents

struct ScheduleEvents {
  layout::MemoryMap memory_map;
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
namespace status {

enum struct MemoryDataType {
  all,
  inout,
  weight,
  workspace,
  constant
}; // enum struct MemoryType

enum struct MemoryStatusType {
  none,
  empty,
  device,
  host,
  coexist,
  swapin,
  swapout
}; // enum struct MemoryDataStatusType

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

  MemoryStatusType status = MemoryStatusType::none;

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
  MemoryStatusType status = MemoryStatusType::none;

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
  MemoryDataType type = MemoryDataType::all;

  // Indicating if the tensor should be considered in swapping.
  bool persistent = false;
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
    sections.emplace(
        0, MemorySection{0, _size, nullptr, nullptr, MemoryStatusType::none});
    // if (_size < 1048576 * 4) transient = true;
  }
  Tensor(const std::string& _name, size_t _size, MemoryDataType _type)
      : Tensor(_name, _size) {
    type = _type;
    if (_type == MemoryDataType::constant || _type == MemoryDataType::weight)
      persistent = true;
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
  inline void setPersistent(bool _persistent) {
    persistent = _persistent;
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
  inline bool isPersistent() const noexcept {
    return persistent;
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
      if (x.second.status == MemoryStatusType::empty ||
          x.second.status == MemoryStatusType::device ||
          x.second.status == MemoryStatusType::coexist)
        return true;
    }
    return false;
  }
  /**
   * If tensor has all data located on device.
   */
  bool isDeviceAllLocated() const noexcept {
    for (auto& x : sections) {
      if (x.second.status == MemoryStatusType::none ||
          x.second.status == MemoryStatusType::host)
        return false;
    }
    return true;
  }
  /**
   * If tensor has data located on host.
   */
  bool isHostLocated() const noexcept {
    for (auto& x : sections) {
      if (x.second.status == MemoryStatusType::host ||
          x.second.status == MemoryStatusType::coexist)
        return true;
    }
    return false;
  }
  /**
   * If tensor has all data located on host.
   */
  bool isHostAllLocated() const noexcept {
    for (auto& x : sections) {
      if (x.second.status == MemoryStatusType::none ||
          x.second.status == MemoryStatusType::empty ||
          x.second.status == MemoryStatusType::device)
        return false;
    }
    return true;
  }
  /**
   * If tensor has data located on host or device.
   */
  bool isMemoryLocated() const noexcept {
    for (auto& x : sections) {
      if (x.second.status != MemoryStatusType::none)
        return true;
    }
    return false;
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
      throw status_exception("Set reshaped for sectioned tensor.");
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
        case MemoryStatusType::empty:
          if (size != 0)
            x.second.status = MemoryStatusType::device;
        case MemoryStatusType::device:
          break;
        case MemoryStatusType::coexist:
          throw status_exception("Accessing data not released on host.");
        default:
          throw status_exception("Accessing data not on device.");
      }
    }
  }
  void setAcquired() {
    for (auto& x : sections) {
      switch (x.second.status) {
        case MemoryStatusType::coexist:
        case MemoryStatusType::device:
        case MemoryStatusType::empty:
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
      case MemoryStatusType::device:
        memory_section.status = MemoryStatusType::coexist;
        host_size += memory_section.size;
      case MemoryStatusType::coexist:
      case MemoryStatusType::empty:
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
      case MemoryStatusType::none:
        memory_section.status = MemoryStatusType::empty;
        break;
      case MemoryStatusType::host:
        memory_section.status = MemoryStatusType::coexist;
      case MemoryStatusType::coexist:
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
      case MemoryStatusType::empty:
      case MemoryStatusType::device:
      case MemoryStatusType::coexist:
        break;
      default: // device none
        throw status_exception("No data on device while moving memory data.");
        break;
    }
  }
  void setHostFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case MemoryStatusType::coexist:
        memory_section.status = MemoryStatusType::device;
        break;
      case MemoryStatusType::host:
        memory_section.status = MemoryStatusType::none;
        break;
      default: // none empty device
        throw status_exception("No data on host while freeing host memory.");
    }
    host_size -= memory_section.size;
  }
  void setDeviceFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case MemoryStatusType::coexist:
        memory_section.status = MemoryStatusType::host;
        break;
      case MemoryStatusType::empty:
      case MemoryStatusType::device:
        memory_section.status = MemoryStatusType::none;
        break;
      default: // none host
        throw status_exception("No data on host while freeing host memory.");
    }

    device_size -= memory_section.size;
  }
  void setFreed(size_t offset) {
    MemorySection& memory_section = sections.at(offset);
    switch (memory_section.status) {
      case MemoryStatusType::coexist:
        device_size -= memory_section.size;
        host_size -= memory_section.size;
        break;
      case MemoryStatusType::empty:
      case MemoryStatusType::device:
        device_size -= memory_section.size;
        break;
      case MemoryStatusType::host:
        host_size -= memory_section.size;
        break;
      default: // none
        throw status_exception(
            "No data on host and device while freeing memory.");
    }
    memory_section.status = MemoryStatusType::none;
  }

  inline bool hasFragment() const noexcept {
    return fragment.size != 0;
  }
  inline const Fragment& getFragment() const noexcept {
    return fragment;
  }
  inline void setFragment(size_t _size) {
    if (fragment.status != MemoryStatusType::none)
      throw status_exception("Setting existed fragment size.");
    fragment.size = _size;
  }

  void setFragmentPlaced(void* address) {
    if (fragment.status != MemoryStatusType::none)
      throw status_exception("Placing existed fragment.");
    fragment.status = MemoryStatusType::empty;
    fragment.address = address;
  }

  inline void setFragmentPlaced() {
    setFragmentPlaced((uint8_t*)(sections.at(0).device_address) + size);
  }

  void setFragmentRemoved() {
    if (fragment.status == MemoryStatusType::none)
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
  Operator(const Operator& _op) = default;
  Operator(Operator&& _op) = default;
  Operator& operator=(const Operator& _op) = default;
  Operator& operator=(Operator&& _op) = default;

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
  inline bool isPersistent() const noexcept {
    return status.isPersistent();
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
    return status.isDeviceAllLocated();
  }
  inline bool isHostLocated() const noexcept {
    return status.isHostLocated();
  }
  inline bool isHostAllLocated() const noexcept {
    return status.isHostAllLocated();
  }
  inline bool isMemoryLocated() const noexcept {
    return status.isMemoryLocated();
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

  inline bool hasExecutionPost(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    return ++p != execution_order.end();
  }
  inline std::string getExecutionPost(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    if (++p == execution_order.end())
      return "";
    return *p;
  }

  inline bool hasExecutionPrev(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    return p != execution_order.begin();
  }
  inline std::string getExecutionPrev(const std::string& op) const {
    auto p = std::find(execution_order.begin(), execution_order.end(), op);
    if (p == execution_order.end())
      throw status_exception("Operator not registered.");
    if (p == execution_order.begin())
      return "";
    return *--p;
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

  inline std::unordered_set<std::string> getTensors() const {
    std::unordered_set<std::string> re;
    for (auto& x : tensor_statuses)
      re.insert(x.first);
    return re;
  }
  inline std::unordered_set<std::string> getOperators() const {
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

namespace utils {

static std::string get_tensor_type_str(MemoryDataType type) {
  switch (type) {
    case MemoryDataType::all:
      return "all";
    case MemoryDataType::constant:
      return "constant";
    case MemoryDataType::inout:
      return "inout";
    case MemoryDataType::weight:
      return "weight";
    case MemoryDataType::workspace:
      return "workspace";
  }

  assert(0);
  return "";
}

} // namespace utils

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
  virtual void submitEvent(const events::ExecutionEvent& event) = 0;
  virtual events::ScheduleEvents getScheduleEvents() = 0;

  virtual void stop() {}

  virtual void terminate() = 0;

  virtual ~Backend(){};
}; // struct Backend

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
    defaults.emplace("scheduler", "section");
    defaults.emplace("scheduler.dependency.timeaware", "true");
    defaults.emplace("scheduler.dependency.thershold", "2");

    defaults.emplace("exporters.events", "empty");
    defaults.emplace("exporters.events.method", "empty");
    defaults.emplace("exporters.tensors", "empty");
    defaults.emplace("exporters.tensors.method", "empty");
    defaults.emplace("exporters.schedule", "empty");
    defaults.emplace("exporters.schedule.method", "empty");
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
    defaults = std::move(_context.defaults);
    contexts = std::move(_context.contexts);
  }

  void operator=(const Context& _context) {
    defaults = _context.defaults;
    contexts = _context.contexts;
  }

  void operator=(Context&& _context) {
    defaults = std::move(_context.defaults);
    contexts = std::move(_context.contexts);
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
    return at(key) == "true" ? true : false;
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

  virtual void onMemoryEvent(const events::MemoryEvent& event) const {}
  virtual void onExecutionEvent(const events::ExecutionEvent& event) const {}

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
  virtual void onTensors(status::MemoryStatus& status) const {}

  virtual ~TensorsExporter() {
    if (hInst)
      dlclose(hInst);
  }
}; // struct TensorExporter

struct ScheduleExporter {
  std::unique_ptr<exportimpl::ExportMethod> export_method;
  void* hInst = nullptr;

  ScheduleExporter(const Context::View& context) {
    std::string export_method_name = context.at("method");
    Context::View context_view = context.view("method");
    if (export_method_name == "empty")
      export_method.reset(new exportimpl::ExportMethod(context_view));
    else if (export_method_name == "file")
      export_method.reset(new exportimpl::FileExportMethod(context_view));
    else
      hInst = utils::load_dylib(
          "Schedule Events Export Method",
          context.at("method.path"),
          "export_method_entry",
          export_method,
          context_view);
  }
  virtual void onScheduleEvents(const events::ScheduleEvents& events) const {}

  virtual ~ScheduleExporter() {
    if (hInst)
      dlclose(hInst);
  }
}; // struct TensorExporter

} // namespace exporter
} // namespace mori

namespace mori {
namespace events {

template <typename T>
struct EventSet;

struct Events final {
 private:
  friend struct EventSet<MemoryEvent>;
  friend struct EventSet<ExecutionEvent>;

 private:
  int iteration = 0;
  // key: iteration, value: MemoryEvent / ExecutionEvent
  std::multimap<int, MemoryEvent> memory_events;
  std::multimap<int, ExecutionEvent> execution_events;

 public:
  Events() = default;

  void submitEvent(const MemoryEvent& event) {
    memory_events.emplace(iteration, event);
  }
  void submitEvent(const ExecutionEvent& event) {
    execution_events.emplace(iteration, event);
  }

  EventSet<MemoryEvent> from_memory_events() const;
  EventSet<ExecutionEvent> from_execution_events() const;

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

template <typename T>
struct EventSet final {
 private:
  friend Events;

 private:
  using event_base = std::multimap<int, T>;
  using event_iter = typename event_base::const_iterator;

  struct Comparator final {
    bool operator()(const event_iter& p, const event_iter& q) const {
      if (p->first == q->first)
        return p->second.timestamp < q->second.timestamp;
      return p->first < q->first;
    }
  }; // struct Comparator

 public:
  using item = std::pair<int, T>;
  using pred = std::function<bool(const item&)>;
  using res = std::set<event_iter, Comparator>;

 private:
  const event_base& events_base;
  std::set<event_iter, Comparator> events_cond;
  std::vector<pred> preds;

  bool first_query = true;

 private:
  EventSet(const event_base& events) : events_base(events) {}

 public:
  EventSet(const EventSet&) = default;
  EventSet(EventSet&&) = default;

  EventSet select() const {
    return *this;
  }

  EventSet& where(const std::function<bool(const std::pair<int, T>&)> f) {
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

  inline const res& ref() const noexcept {
    return events_cond;
  }

  inline size_t size() const noexcept {
    if (first_query)
      return events_base.size();
    return events_cond.size();
  }

  inline bool empty() const noexcept {
    return events_cond.empty();
  }

  inline void clear() {
    events_cond.clear();
    first_query = true;
  }

  ~EventSet() = default;
}; // struct EventSet

inline EventSet<MemoryEvent> Events::from_memory_events() const {
  return EventSet<MemoryEvent>{this->memory_events};
}

inline EventSet<ExecutionEvent> Events::from_execution_events() const {
  return EventSet<ExecutionEvent>{this->execution_events};
}

template <typename T>
inline static EventSet<T> select(const EventSet<T>& event_set) {
  EventSet<T> re = event_set;
  re.get();
  return re;
}

} // namespace events
} // namespace mori

#include <algorithm>
#include <atomic>
#include <mutex>
#include <string>
#include <vector>

#include <map>
#include <unordered_map>
#include <vector>

namespace mori {
namespace decisions {

struct LayoutModel final {
 public:
  struct Node final {
    layout::Region& region;

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

    Node(layout::Region& _r) : region(_r) {
      lower_remaining_size = _r.size;
      upper_remaining_size = _r.size;
    }

    void setFragment(size_t _size) {
      region.fragment_size = _size;
      lower_fragment_remaining_size = _size;
      upper_fragment_remaining_size = _size;
    }
  }; //  inner struct Node

 private:
  // std::unordered_map<std::string, Node*> tensors;
  layout::MemoryMapBuilder memory_map_builer;
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
  void fillModel(status::MemoryStatus& status) {
    std::string entry = status.getEntry();
    for (auto& so : status.getExecutionOrder()) {
      status::OperatorPres op_pres = status.referenceOperator(so);
      for (auto& st : op_pres.getTensors()) {
        status::TensorPres tensor_pres = status.referenceTensor(st);
        if (tensor_pres.isPersistent() || tensor_pres.isTransient())
          continue;
        // Do submit here.
        layout::Layer& l = memory_map_builer.getCurrentLayer();
        size_t aligned_size = utils::get_memory_aligned_size(
            tensor_pres.getSize(),
            memory_map_builer.getMemoryInfo().device.align_size);
        if (l.requested_size + aligned_size > l.size)
          memory_map_builer.createLayer();
        layout::Region r(tensor_pres.getName(), aligned_size);
        memory_map_builer.submitMemoryRegion(r);
        nodes.emplace(r.name, memory_map_builer.regions.at(r.name));
      }
    }
  }

  void generateFragments() {
    bool tensor_moved = true;
    while (tensor_moved) {
      tensor_moved = false;
      auto pl = memory_map_builer.layers.rbegin();
      while (pl != memory_map_builer.layers.rend()) {
        // Fragments exceed the memory capacity.
        if (!pl->isAccomodatable()) {
          // Check the existance of the upper layer.
          if (pl == memory_map_builer.layers.rbegin())
            memory_map_builer.createLayer();
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
        if (pl == memory_map_builer.layers.rend())
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
    auto pl = memory_map_builer.layers.begin();
    auto pu = pl;
    ++pu;

    while (pu != memory_map_builer.layers.end()) {
      auto& lowers = pl++->regions;
      auto& uppers = pu++->regions;

      auto ql = lowers.begin();
      auto qu = uppers.begin();

      while (ql != lowers.end() &&
             qu != uppers.end()) { // If ql or qu reaches the end of layer, no
                                   // need to split the lower layer tensors.
        Node& nl = nodes.at(*ql);
        Node& nu = nodes.at(*qu);

        size_t size_sectioned =
            nl.upper_remaining_size > nu.lower_remaining_size
            ? nu.lower_remaining_size
            : nl.upper_remaining_size;
        if (size_sectioned >= smin)
          nl.region.sections.push_back(size_sectioned);
        else
          nl.region.sections.back() += size_sectioned;
        nl.upper_remaining_size -= size_sectioned;
        nu.lower_remaining_size -= size_sectioned;

        // Process of fragment.
        if (nl.upper_remaining_size > 0) {
          size_t size_frag =
              nl.upper_remaining_size > nu.lower_fragment_remaining_size
              ? nu.lower_fragment_remaining_size
              : nl.upper_remaining_size;
          nl.upper_remaining_size -= size_frag;
          nu.lower_fragment_remaining_size -= size_frag;
        } else if (nu.lower_remaining_size > 0) {
          size_t size_frag =
              nu.lower_remaining_size > nl.upper_fragment_remaining_size
              ? nl.upper_fragment_remaining_size
              : nu.lower_remaining_size;
          nu.lower_remaining_size -= size_frag;
          nl.upper_fragment_remaining_size -= size_frag;
        } else {
          size_t size_frag = nl.upper_fragment_remaining_size >
                  nu.lower_fragment_remaining_size
              ? nu.lower_fragment_remaining_size
              : nl.upper_fragment_remaining_size;
          nl.upper_fragment_remaining_size -= size_frag;
          nu.lower_fragment_remaining_size -= size_frag;
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
          if (n.upper_remaining_size >= smin)
            n.region.sections.push_back(n.upper_remaining_size);
          else
            n.region.sections.back() += n.upper_remaining_size;
          n.upper_remaining_size = 0;
        }
        for (auto& s : uppers)
          assert(nodes.at(s).lower_remaining_size == 0);
      }
      for (auto& s : lowers)
        assert(nodes.at(s).upper_remaining_size == 0);
    }

    // Section information for the top layer. No splition (only one section).
    for (auto& s : memory_map_builer.layers.back()) {
      layout::Region& r = memory_map_builer.regions.at(s);
      r.sections.push_back(r.size);
    }
  }

 public:
  LayoutModel() = default;
  LayoutModel(const LayoutModel&) = default;
  LayoutModel(LayoutModel&&) = default;

  LayoutModel& operator=(const LayoutModel&) = default;
  LayoutModel& operator=(LayoutModel&&) = default;

  void setMemoryInfo(const MemoryInfo& memory_info) {
    device_size = memory_info.device.common_block.size;
    memory_map_builer.setMemoryInfo(memory_info);
  }

  void analyze(status::MemoryStatus& status, bool fragmented = true) {
    if (analyzed)
      return;

    fillModel(status);
    for (auto& x : memory_map_builer.layers)
      assert(x.isAccomodatable());
    // If only one layer, there'e no need to analyze.
    if (memory_map_builer.layers.size() != 1) {
      generateFragments();
      for (auto& x : memory_map_builer.layers)
        assert(x.isAccomodatable());
      generateTree();
    }
    analyzed = true;
  }

  int getLayerCount() const {
    return memory_map_builer.layers.size();
  }
  const std::vector<layout::Layer>& getLayers() const {
    return memory_map_builer.layers;
  }
  const layout::Layer& getLayer(int _layer) const {
    return memory_map_builer.layers[_layer];
  }
  const Node getMemoryNode(const std::string& _node) const {
    return nodes.at(_node);
  }

  layout::MemoryMap getMemoryMap() {
    if (!analyzed)
      throw status_exception("Memory map not analyzed.");
    return memory_map_builer.build();
  }

  void clear() noexcept {
    clusters.clear();
    memory_map_builer.clear();
    memory_map_builer.createLayer();
    analyzed = false;
  }

  ~LayoutModel() = default;
}; // struct Model

} // namespace decisions
} // namespace mori

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace mori {
namespace decisions {

struct TransferringModel {
  long analyze(size_t size) {
    // long t = size / 1048576;
    // return t / 12;
    return size >> 2;
  }

}; // struct TransferModel

struct TimeModel final {
 public:
  struct Timespan final {
    std::string target;
    long span = 0;
    bool synchronization = false;
    long timepoint = 0;

    Timespan() = default;
    Timespan(const std::string& _target, long _span)
        : target(_target), span(_span) {}

    inline bool isSynchronization() {
      return synchronization;
    }
    inline void setSynchronization(bool _synchronization) {
      synchronization = _synchronization;
    }
  }; // inner struct Timespan

  struct Lane final {
    Direction synchronization_type = Direction::prev;
    std::vector<std::pair<std::string, Timespan>> timespans;
    std::string current_synchronization_label = "";

    void submitSynchronizationLabel(const std::string synchronization_label) {
      if (synchronization_type == Direction::post) {
        for (auto p = timespans.rbegin(); p != timespans.rend(); ++p) {
          if (p->second.synchronization)
            break;
          if (p->first != synchronization_label)
            throw status_exception("Synchronization label mismatch.");
        }
      }
      Timespan timespan(synchronization_label, 0);
      auto& target = timespans.emplace_back(synchronization_label, timespan);
      target.second.synchronization = true;
      current_synchronization_label = synchronization_label;
    }
    void submitTimespan(
        const std::string synchronization_label,
        const Timespan& _timespan) {
      if (synchronization_type == Direction::post) {
        if (synchronization_label == current_synchronization_label)
          throw status_exception("Synchronization label mismatch.");
      } else {
        if (synchronization_label != current_synchronization_label)
          throw status_exception("Synchronization label mismatch.");
      }
      timespans.emplace_back(synchronization_label, _timespan);
    }
  }; // inner struct Lane

 private:
  std::unordered_set<std::string> enabled_synchronization_labels;

  bool strong_synchronization = false;

 public:
  Lane execution_lane;
  Lane transferring_lane;

 protected:
  void analyzeSynchronization() {
    auto ptrans = transferring_lane.timespans.rbegin();

    long total_execution_time = 0;
    for (auto pexec = execution_lane.timespans.rbegin();
         pexec != execution_lane.timespans.rend();
         ++pexec) {
      if (pexec->second.synchronization) {
        auto penabled =
            enabled_synchronization_labels.find(pexec->second.target);
        if (penabled == enabled_synchronization_labels.end())
          continue;
      } else {
        total_execution_time += pexec->second.span;
        continue;
      }

      // Synchronize execution and transferring lane.
      long total_transferring_time = 0;
      while (ptrans != transferring_lane.timespans.rend()) {
        if (ptrans->second.synchronization) {
          auto penabled =
              enabled_synchronization_labels.find(ptrans->second.target);
          if (penabled != enabled_synchronization_labels.end())
            break;
        } else
          total_transferring_time += ptrans->second.span;
        ++ptrans;
      }
      if (ptrans != transferring_lane.timespans.rend())
        assert(ptrans->second.target == pexec->second.target);
      if (total_execution_time >= total_transferring_time) {
        ptrans->second.span = total_execution_time - total_transferring_time;
        total_execution_time = 0;
      } else {
        ptrans->second.span = 0;
        total_execution_time = strong_synchronization
            ? (total_execution_time - total_transferring_time)
            : 0;
      }
      ++ptrans;
    }
  }

  void generateTimepoint() {
    long current_timepoint = 0;
    for (auto p = execution_lane.timespans.begin();
         p != execution_lane.timespans.end();
         ++p) {
      p->second.timepoint = current_timepoint;
      current_timepoint += p->second.span;
    }
    for (auto p = transferring_lane.timespans.rbegin();
         p != transferring_lane.timespans.rend();
         ++p) {
      current_timepoint -= p->second.span;
      p->second.timepoint = current_timepoint;
    }
  }

 public:
  TimeModel() {
    execution_lane.synchronization_type = Direction::prev;
    transferring_lane.synchronization_type = Direction::post;
  }

  inline void submitExecutionSynchronization(
      const std::string& synchronization_label) {
    execution_lane.submitSynchronizationLabel(synchronization_label);
  }
  inline void submitExecutionTimespan(
      const std::string& synchronization_label,
      const Timespan& timespan) {
    execution_lane.submitTimespan(synchronization_label, timespan);
  }
  inline void submitTransferringSynchronization(
      const std::string& synchronization_label) {
    transferring_lane.submitSynchronizationLabel(synchronization_label);
  }
  inline void submitTransferringTimespan(
      const std::string& synchronization_label,
      const Timespan& timespan) {
    transferring_lane.submitTimespan(synchronization_label, timespan);
  }

  void setSynchronizationEnabled(const std::string& _label) {
    enabled_synchronization_labels.insert(_label);
  }

  inline bool isStrongSynchronization() const {
    return strong_synchronization;
  }
  void setStrongSynchronization(bool _strong_synchronization) {
    strong_synchronization = _strong_synchronization;
  }

  void analyze() {
    analyzeSynchronization();
    generateTimepoint();
  }
}; // struct TimeModel

} // namespace decisions
} // namespace mori

namespace mori {

struct MemoryScheduler {
 protected:
  const Context::View& context;
  status::MemoryStatus& status;
  events::Events& events;

  events::ScheduleEvents schedule_events;

  std::atomic<int> current_iteration = 0;

 protected:
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
   * Action when new execution event is submitted.
   * @param event The submitted event
   */
  virtual void onMemoryEvent(const events::ExecutionEvent& event) = 0;

  /**
   * Action when an iteration starts.
   */
  virtual void onNewIteration() = 0;

 public:
  MemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : context(_context), status(_status), events(_events) {}

  inline events::ScheduleEvents getScheduleEvents() {
    onSchedule();
    return schedule_events;
  }

  void submitEvent(events::MemoryEvent event) {
    onMemoryEvent(event);
  }
  void submitEvent(events::ExecutionEvent event) {
    onMemoryEvent(event);
  }

  void newIteration() {
    ++current_iteration;
    onNewIteration();
  }

  virtual ~MemoryScheduler() = default;

}; // struct MemoryScheduler

struct EventBasedMemoryScheduler : public MemoryScheduler {
 protected:
  bool event_decided = false;

  virtual void preAnalyzeEvents() = 0;
  virtual std::unordered_set<std::string> analyzeForwardEvents(
      const events::EventSet<events::MemoryEvent>&) = 0;
  virtual void analyzeBackwardEvents(
      const events::EventSet<events::MemoryEvent>&,
      const std::unordered_set<std::string>&) = 0;
  virtual void postAnalyzeEvents() = 0;

  virtual void onSchedule() override {
    if (event_decided)
      return;

    auto iter_1_mem_res =
        events.from_memory_events()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              // return item.first == (current_iteration - 1);
              return item.first == 1;
            })
            .get();
    if (iter_1_mem_res.empty())
      return;

    auto iter_1_forward_mem_res =
        iter_1_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              return item.second.stage == ApplicationStage::forward;
            })
            .get();

    auto iter_1_backward_mem_res =
        iter_1_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              return item.second.stage == ApplicationStage::backward;
            })
            .get();

    preAnalyzeEvents();
    auto tensors_swapped = analyzeForwardEvents(iter_1_forward_mem_res);
    analyzeBackwardEvents(iter_1_backward_mem_res, tensors_swapped);
    postAnalyzeEvents();

    event_decided = true;
  }

  virtual void onMemoryEvent(const events::MemoryEvent& event) override {
    if (!event_decided)
      return;

    if (event.stage == ApplicationStage::forward &&
        event.type == events::MemoryEventType::swapin) {
      // Indicate schedule error.
      assert(true);
    }
  }

  virtual void onMemoryEvent(const events::ExecutionEvent& event) override {}
  virtual void onNewIteration() override {}

 public:
  EventBasedMemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : MemoryScheduler(_context, _status, _events) {}
  virtual ~EventBasedMemoryScheduler() = default;
}; // struct EventBasedMemoryScheduler

struct FIFOMemoryScheduler : public EventBasedMemoryScheduler {
 protected:
  virtual void preAnalyzeEvents() override {}
  virtual std::unordered_set<std::string> analyzeForwardEvents(
      const events::EventSet<events::MemoryEvent>& iter_1_forward_mem_res)
      override {
    auto iter_1_forward_swapout_res =
        iter_1_forward_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              return item.second.type == events::MemoryEventType::swapout;
            })
            .get();
    // No need to swap.
    if (iter_1_forward_swapout_res.empty())
      return std::unordered_set<std::string>();

    size_t unmet_memory_requirement = 0;
    size_t released_memory_requirement = 0;
    for (auto& x : iter_1_forward_swapout_res.ref())
      unmet_memory_requirement += x->second.size;

    std::unordered_set<std::string> tensors_swapped;
    // Tensors to swapout.
    for (auto& s : status.getExecutionOrder()) {
      status::OperatorPres op_pres = status.referenceOperator(s);
      // Forward propagation and backward propagation share the same set of
      // operators.
      if (op_pres.isBackwardPropagation())
        continue;

      for (auto& tensor_name : op_pres.getTensors()) {
        status::TensorPres tensor_pres = status.referenceTensor(tensor_name);
        // Do not swap out persistant tensors.
        if (tensor_pres.isPersistent() || tensor_pres.isTransient())
          continue;

        // Get the last access of this tensor in forward stage.
        auto iter_1_tensor_forward_res =
            iter_1_forward_mem_res.select()
                .where([&tensor_name](
                           const events::EventSet<events::MemoryEvent>::item&
                               item) {
                  return item.second.tensor == tensor_name &&
                      item.second.type != events::MemoryEventType::swapin &&
                      item.second.type != events::MemoryEventType::swapout;
                })
                .get();

        bool forward_event_generated = false;
        std::string last_acquired;
        std::string last_assigned;
        for (auto& y : iter_1_tensor_forward_res.ref()) {
          switch (y->second.type) {
            case events::MemoryEventType::allocate:
              // Tensor allocated in forward propagation, swapout event should
              // be generated.
              forward_event_generated = true;
            case events::MemoryEventType::read:
              last_acquired = y->second.op;
              break;
            case events::MemoryEventType::write:
            case events::MemoryEventType::access:
              last_assigned = y->second.op;
              break;
            case events::MemoryEventType::free:
              // Tensor released in forward propagation, swapout event should
              // not be generated.
              forward_event_generated = false;
            default:
              break;
          }
        }

        if (!forward_event_generated)
          continue;

        tensors_swapped.insert(tensor_name);
        // Generate swapout event
        // schedule_events.forward_schedule_events.execution[last_assigned].emplace_back(tensor_pres.getOperatorName(),
        // tensor_pres.getName(), tensor_pres.getSize(),
        // events::ScheduleEventType::copyout, last_assigned);
        schedule_events.forward_schedule_events.execution[last_acquired]
            .emplace_back(
                tensor_pres.getOperatorName(),
                tensor_pres.getName(),
                tensor_pres.getSize(),
                events::ScheduleEventType::swapout,
                last_acquired);
        released_memory_requirement += tensor_pres.getSize();

        // if (unmet_memory_requirement <= released_memory_requirement) break;
      }
    }

    return tensors_swapped;
  }
  virtual void postAnalyzeEvents() override {}

 public:
  FIFOMemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : EventBasedMemoryScheduler(_context, _status, _events) {}
}; // struct FIFOMemoryScheduler

struct ExecutionTimeAwareMemoryScheduler : public FIFOMemoryScheduler {
 protected:
  decisions::TimeModel time_model;
  decisions::TransferringModel transferring_model;

  std::unordered_map<std::string, long> execution_timespans;

 protected:
  virtual void preAnalyzeEvents() override {
    auto iter_1_backward_res =
        events.from_execution_events()
            .where(
                [](const events::EventSet<events::ExecutionEvent>::item& item) {
                  // return item.first == (current_iteration - 1);
                  return item.first == 1 &&
                      item.second.stage == ApplicationStage::backward;
                })
            .get();
    // No memory events.
    if (iter_1_backward_res.empty())
      return;

    auto iter_1_backward_request_res =
        iter_1_backward_res.select()
            .where(
                [](const events::EventSet<events::ExecutionEvent>::item& item) {
                  return item.second.type ==
                      events::ExecutionEventType::request;
                })
            .get();
    auto iter_1_backward_release_res =
        iter_1_backward_res.select()
            .where(
                [](const events::EventSet<events::ExecutionEvent>::item& item) {
                  return item.second.type ==
                      events::ExecutionEventType::release;
                })
            .get();

    std::unordered_map<std::string, long> request_timepoints;
    std::unordered_map<std::string, long> release_timepoints;

    for (auto& x : iter_1_backward_request_res.ref())
      request_timepoints[x->second.op] =
          utils::get_timestamp_val(x->second.timestamp);
    for (auto& x : iter_1_backward_release_res.ref())
      release_timepoints[x->second.op] =
          utils::get_timestamp_val(x->second.timestamp);
    assert(request_timepoints.size() == release_timepoints.size());

    for (auto& s : status.getExecutionOrder()) {
      status::OperatorPres operator_pres = status.referenceOperator(s);
      if (!operator_pres.isBackwardPropagation())
        continue;
      if (request_timepoints.count(s) != 1 || release_timepoints.count(s) != 1)
        continue;

      execution_timespans.emplace(
          s, release_timepoints.at(s) - request_timepoints.at(s));
      decisions::TimeModel::Timespan timespan(
          s, release_timepoints.at(s) - request_timepoints.at(s));
      time_model.submitExecutionSynchronization(s);
      time_model.submitExecutionTimespan(s, timespan);
    }
  }

 public:
  ExecutionTimeAwareMemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : FIFOMemoryScheduler(_context, _status, _events) {
    time_model.setStrongSynchronization(false);
  }
  virtual ~ExecutionTimeAwareMemoryScheduler() = default;
}; // struct ExecutionTimeAwareMemoryScheduler

struct SectionAwareMemoryScheduler : public ExecutionTimeAwareMemoryScheduler {
 private:
  bool layout_model_decided = false;

  decisions::LayoutModel layout_model;

 protected:
  void analyzeLayoutModel() {
    layout_model.setMemoryInfo(status.getMemoryInfo());
    layout_model.analyze(status);
    schedule_events.memory_map = layout_model.getMemoryMap();

    layout_model_decided = true;
  }

  virtual void preAnalyzeEvents() override {
    if (!layout_model_decided)
      analyzeLayoutModel();
    if (layout_model.getLayerCount() == 1)
      return; // No need of memory swapping.
    ExecutionTimeAwareMemoryScheduler::preAnalyzeEvents();
  }

  virtual std::unordered_set<std::string> analyzeForwardEvents(
      const events::EventSet<events::MemoryEvent>& iter_1_forward_mem_res)
      override {
    std::unordered_set<std::string> tensors_swapped;

    auto iter_1_forward_swapout_res =
        iter_1_forward_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              return item.second.type == events::MemoryEventType::swapout;
            })
            .get();
    // No need to swap.
    if (iter_1_forward_swapout_res.empty())
      return tensors_swapped;

    size_t unmet_memory_requirement = 0;
    size_t released_memory_requirement = 0;
    for (auto& x : iter_1_forward_swapout_res.ref())
      unmet_memory_requirement += x->second.size;

    // Generate swapout events based on analysis model.
    // All tensors are accessed in forward propagation.

    for (auto& l : layout_model.getLayers()) {
      for (auto& s : l.regions) {
        const decisions::LayoutModel::Node& node =
            layout_model.getMemoryNode(s);
        if (node.posts.empty())
          continue; // No need to swapout tensor.

        // Generate copyout and freehost events.
        auto iter_1_tensor_forward_res =
            iter_1_forward_mem_res.select()
                .where([s](const events::EventSet<events::MemoryEvent>::item&
                               item) { return item.second.tensor == s; })
                .get();

        bool forward_event_generated = false;
        std::string last_acquired;
        std::string last_assigned;
        for (auto& y : iter_1_tensor_forward_res.ref()) {
          switch (y->second.type) {
            case events::MemoryEventType::allocate:
              // Tensor allocated in forward propagation, swapout event should
              // be generated.
              forward_event_generated = true;
            case events::MemoryEventType::read:
              last_acquired = y->second.op;
              break;
            case events::MemoryEventType::write:
            case events::MemoryEventType::access:
              last_assigned = y->second.op;
              break;
            case events::MemoryEventType::free:
              // Tensor released in forward propagation, swapout event should
              // not be generated.
              forward_event_generated = false;
            default:
              break;
          }
        }

        if (!forward_event_generated)
          continue;

        tensors_swapped.insert(s);
        status::TensorPres pres = status.referenceTensor(s);
        for (auto& x : node.region.sections) {
          // schedule_events.forward_schedule_events.execution[last_assigned].emplace_back(pres.getOperatorName(),
          // s, x, events::ScheduleEventType::copyout, last_acquired);
          schedule_events.forward_schedule_events.execution[last_acquired]
              .emplace_back(
                  pres.getOperatorName(),
                  s,
                  x,
                  events::ScheduleEventType::swapout,
                  last_acquired);
          released_memory_requirement += x;
        }

        // if (unmet_memory_requirement <= released_memory_requirement) break;
      }
    }
    return tensors_swapped;
  }
  virtual void analyzeBackwardEvents(
      const events::EventSet<events::MemoryEvent>& iter_1_backward_mem_res,
      const std::unordered_set<std::string>& tensors_swapped) override {
    auto iter_1_backward_access_res =
        iter_1_backward_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              if (item.second.type == events::MemoryEventType::allocate)
                return false;
              if (item.second.type == events::MemoryEventType::free)
                return false;
              return item.second.type != events::MemoryEventType::swapin &&
                  item.second.type != events::MemoryEventType::swapout;
            })
            .get();

    for (auto& s : status.getExecutionOrder()) {
      auto op_pres = status.referenceOperator(s);
      if (!op_pres.isBackwardPropagation())
        continue;

      auto target_operator_backward_res =
          iter_1_backward_access_res.select()
              .where(
                  [&s, &tensors_swapped](
                      const events::EventSet<events::MemoryEvent>::item& item) {
                    if (item.second.op != s)
                      return false;
                    return tensors_swapped.find(item.second.tensor) !=
                        tensors_swapped.end();
                  })
              .get();
      if (target_operator_backward_res.empty())
        continue;

      for (auto& x : target_operator_backward_res.ref()) {
        status::TensorPres tensor_pres =
            status.referenceTensor(x->second.tensor);
        decisions::TimeModel::Timespan timespan(
            x->second.tensor,
            transferring_model.analyze(tensor_pres.getSize()));
        time_model.submitTransferringTimespan(s, timespan);
      }
      time_model.submitTransferringSynchronization(s);
      time_model.setSynchronizationEnabled(s);
    }

    time_model.analyze();

    for (auto& x : time_model.transferring_lane.timespans) {
      if (x.second.synchronization)
        continue;
      status::TensorPres pres = status.referenceTensor(x.second.target);
      // Generate swapin event
      schedule_events.backward_schedule_events.timepoint.emplace_back(
          pres.getOperatorName(),
          pres.getName(),
          pres.getSize(),
          events::ScheduleEventType::copyin,
          x.second.timepoint);
    }
  }

 public:
  SectionAwareMemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : ExecutionTimeAwareMemoryScheduler(_context, _status, _events) {
    time_model.setStrongSynchronization(true);
  }
  virtual ~SectionAwareMemoryScheduler() = default;
}; // struct SectionAwareMemoryScheduler

struct DependencyAwareMemoryScheduler
    : public ExecutionTimeAwareMemoryScheduler {
 private:
  struct TensorRelation {
    std::string current_operator = "";
    bool schedule_changed = false;
    int position = 0;

    TensorRelation(const std::string op)
        : current_operator(op), schedule_changed(false), position(0) {}
  }; // inner struct TensorRelation

 private:
  std::unordered_map<std::string, TensorRelation> tensor_operator_relations;
  std::unordered_set<std::string> tensor_swapout_this_iter;

  bool time_aware = true;
  size_t thershold = 2;

 public:
  DependencyAwareMemoryScheduler(
      const Context::View& _context,
      status::MemoryStatus& _status,
      events::Events& _events)
      : ExecutionTimeAwareMemoryScheduler(_context, _status, _events) {
    time_aware = context.signal("dependency.timeaware");
    thershold = std::stoul(context.at("dependency.thershold"));
  }

  virtual void analyzeBackwardEvents(
      const events::EventSet<events::MemoryEvent>& iter_1_backward_mem_res,
      const std::unordered_set<std::string>& tensors_swapped) override {
    auto iter_1_backward_access_res =
        iter_1_backward_mem_res.select()
            .where([](const events::EventSet<events::MemoryEvent>::item& item) {
              if (item.second.type == events::MemoryEventType::allocate)
                return false;
              if (item.second.type == events::MemoryEventType::free)
                return false;
              return item.second.type != events::MemoryEventType::swapin &&
                  item.second.type != events::MemoryEventType::swapout;
            })
            .get();

    // Generate swap events.
    for (auto& x : tensors_swapped) {
      // Get the first access of this tensor in backword stage
      auto target_tensor_backward_res =
          iter_1_backward_access_res.select()
              .where(
                  [&x](
                      const events::EventSet<events::MemoryEvent>::item& item) {
                    return item.second.tensor == x;
                  })
              .get();

      if (target_tensor_backward_res.ref().empty())
        continue;

      status::TensorPres pres = status.referenceTensor(x);
      std::string opb = (*target_tensor_backward_res.ref().begin())->second.op;
      size_t execution_time = 0;
      size_t transfer_time = transferring_model.analyze(pres.getSize());
      for (int i = 0; i < thershold + 1; ++i) {
        if (!status.hasExecutionPrev(opb))
          break;
        opb = status.getExecutionPrev(opb);
        assert(opb != "");
        if (!time_aware)
          continue;
        if (execution_time >= transfer_time)
          break;
        execution_time += execution_timespans[opb];
      }

      assert(opb != "");
      // Generate swapin event
      schedule_events.backward_schedule_events.execution[opb].emplace_back(
          pres.getOperatorName(),
          pres.getName(),
          pres.getSize(),
          events::ScheduleEventType::copyin,
          opb);
      tensor_operator_relations.emplace(pres.getName(), opb);
    }
  }
  virtual void onMemoryEvent(const events::MemoryEvent& event) override {
    FIFOMemoryScheduler::onMemoryEvent(event);

    if (!time_aware)
      return;

    if (event.stage == ApplicationStage::backward &&
        (event.type == events::MemoryEventType::swapin ||
         event.type == events::MemoryEventType::swapout)) {
      // Indicate swapping in too late or too early
      auto p = tensor_operator_relations.find(event.tensor);
      if (p == tensor_operator_relations.end())
        return;

      // Indicate schedule already adjusted with no benefits
      if (p->second.position == 0 && p->second.schedule_changed)
        return;

      // Indicate this tensor is swapped in after swapped out in backward
      // propagation
      if (tensor_swapout_this_iter.find(event.tensor) !=
          tensor_swapout_this_iter.end())
        return;

      auto& schedule_events_set =
          schedule_events.backward_schedule_events.execution;
      auto q = std::find_if(
          schedule_events_set[p->second.current_operator].begin(),
          schedule_events_set[p->second.current_operator].end(),
          [event](const events::ScheduleEvent& _event) {
            return _event.tensor_name == event.tensor;
          });
      assert(q != schedule_events_set[p->second.current_operator].end());

      std::string opb = "";
      if (event.type == events::MemoryEventType::swapin) {
        if (p->second.position == -4)
          return;
        if (!status.hasExecutionPrev(p->second.current_operator))
          return;
        opb = status.getExecutionPrev(p->second.current_operator);
        p->second.position -= 1;
      } else {
        if (p->second.position == 4)
          return;
        if (!status.hasExecutionPost(p->second.current_operator))
          return;
        opb = status.getExecutionPost(p->second.current_operator);
        p->second.position += 1;
        tensor_swapout_this_iter.insert(event.tensor);
      }
      // Cannot solve this schedule problem.
      assert(opb != "");

      events::ScheduleEvent new_event = *q;
      new_event.operator_name = opb;
      schedule_events_set[opb].push_back(new_event);
      schedule_events_set[p->second.current_operator].erase(q);

      p->second.current_operator = opb;
      p->second.schedule_changed = true;
    }
  }
  virtual void onNewIteration() override {
    tensor_swapout_this_iter.clear();
  }

  virtual ~DependencyAwareMemoryScheduler() = default;

}; // struct DependencyAwareMemoryScheduler

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
  std::mutex events_m;

  std::unique_ptr<MemoryScheduler> scheduler;
  void* scheduler_hinst = nullptr;
  std::unique_ptr<exporter::ScheduleExporter> schedule_exporter;
  void* schedule_exporter_hinst = nullptr;

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
    Context::View scheduler_context = context.view("scheduler");
    if (scheduler_name == "section")
      scheduler = std::unique_ptr<MemoryScheduler>(
          new SectionAwareMemoryScheduler(scheduler_context, status, events));
    else if (scheduler_name == "dependency")
      scheduler =
          std::unique_ptr<MemoryScheduler>(new DependencyAwareMemoryScheduler(
              scheduler_context, status, events));
    else
      scheduler_hinst = utils::load_dylib(
          "Scheduler",
          context.at("scheduler.path"),
          "scheduler_entry",
          scheduler,
          scheduler_context);

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

    // Set up schedule exporter
    std::string schedule_exporter_name = context.at("exporters.schedule");
    if (schedule_exporter_name == "empty")
      schedule_exporter = std::unique_ptr<exporter::ScheduleExporter>(
          new exporter::ScheduleExporter(context.view("exporters.schedule")));
    else
      schedule_exporter_hinst = utils::load_dylib(
          "Schedule Exporter",
          context.at("exporters.schedule.path"),
          "schedule_exporter_entry",
          schedule_exporter,
          context.view("exporters.schedule"));
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
    tensors_exporter->onTensors(status);
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

    std::unique_lock<std::mutex> l{events_m};
    events.submitEvent(event);
    events_exporter->onMemoryEvent(event);
    l.unlock();
    scheduler->submitEvent(event);
  }

  virtual void submitEvent(const events::ExecutionEvent& event) override {
    if (!inited)
      throw uninited_exception();
    if (!started)
      throw uninited_exception();

    std::unique_lock<std::mutex> l{events_m};
    events.submitEvent(event);
    events_exporter->onExecutionEvent(event);
    l.unlock();
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
    schedule_exporter->onScheduleEvents(re);
    return re;
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
  virtual void submitEvent(const events::ExecutionEvent& event) = 0;
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
    (*logger) << LogLevel::debug << "Submiting of event " << event << endl;
    backend->submitEvent(event);
  }
  virtual void submitEvent(const events::ExecutionEvent& event) override {
    (*logger) << LogLevel::debug << "Submiting of event " << event << endl;
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
    virtual void copyOut(status::TensorPres& tensor, size_t size) override {
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
              bool res = executor.memory_manager->merge(
                  device_address, (uint8_t*)device_address + section->size);
              assert(res);
              executor.layout.recordMemoryMergeEvent(
                  device_address, (uint8_t*)device_address + section->size);
              tensor.merge(section->offset);
            }
            const status::MemorySection* section_prev = section->prev();
            if (section_prev != nullptr &&
                tensor.isMergeable(section_prev->offset)) {
              bool res = executor.memory_manager->merge(
                  section_prev->device_address, device_address);
              assert(res);
              executor.layout.recordMemoryMergeEvent(
                  section_prev->device_address, device_address);
              section = &(tensor.merge(section_prev->offset));
            }
            copied_size += section->size;
          }
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
      const status::MemorySection* section = &(tensor.getLastSection());
      do {
        switch (section->status) {
          case status::MemoryStatusType::host:
          case status::MemoryStatusType::coexist: {
            executor.memory_manager->freeHost(section->host_address);
            tensor.setHostFreed(section->offset);
            freed_size += section->size;
            if (tensor.isMergeable(section->offset)) {
              if (section->status != status::MemoryStatusType::none) {
                bool res = executor.memory_manager->merge(
                    section->device_address, section->next()->device_address);
                assert(res);
                executor.layout.recordMemoryMergeEvent(
                    section->device_address, section->next()->device_address);
              }
              tensor.merge(section->offset);
            }
            const status::MemorySection* section_prev = section->prev();
            if (section_prev != nullptr &&
                tensor.isMergeable(section_prev->offset)) {
              if (section->status != status::MemoryStatusType::none) {
                bool res = executor.memory_manager->merge(
                    section_prev->device_address, section->device_address);
                assert(res);
                executor.layout.recordMemoryMergeEvent(
                    section_prev->device_address, section->device_address);
              }
              section = &(tensor.merge(section_prev->offset));
            }
            if (freed_size >= size)
              return;
          }
          default:
            break;
        }
        section = section->prev();
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

  /**
   * Copy in tensor data from host memory to device memory with specific size.
   * Copy from the last section that located only on host.
   * @param tensor Tensor to be copied in
   * @param size   Size to be copied in
   */
  void copyIn(status::TensorPres& tensor, size_t size) {
    impl->copyIn(tensor, size);
  }

  /**
   * Copy out tensor data from device memory to host memory with specific size.
   * Copy from the first section that located only on device.
   * @param tensor Tensor to be copied out
   * @param size   Size to be copied out
   */
  void copyOut(status::TensorPres& tensor, size_t size) {
    impl->copyOut(tensor, size);
  }

  /**
   * Free device memory with specific size.
   * Free from the first section that located on device
   * @param tensor Tensor to be freed on device
   * @param size   Size to be freed on device
   */
  void freeDevice(status::TensorPres& tensor, size_t size) {
    impl->freeDevice(tensor, size);
  }

  /**
   * Free host memory with specific size.
   * Free from the last section that located on host
   * @param tensor Tensor to be freed on host
   * @param size   Size to be freed on host
   */
  void freeHost(status::TensorPres& tensor, size_t size) {
    impl->freeHost(tensor, size);
  }

  /**
   * Swap in tensor data from host memory to device memory with specific size.
   * Swap from the first section that located only on host.
   * @param tensor Tensor to be copied in
   * @param size   Size to be copied in
   */
  void swapIn(status::TensorPres& tensor, size_t size) {
    copyIn(tensor, size);
    freeHost(tensor, size);
  }

  /**
   * Swap out tensor data from device memory to host memory with specific size.
   * Swap from the first section that located only on device.
   * @param tensor Tensor to be copied out
   * @param size   Size to be copied out
   */
  void swapOut(status::TensorPres& tensor, size_t size) {
    copyOut(tensor, size);
    freeDevice(tensor, size);
  }

  /**
   * Free device and host memory with specific size.
   * Free from the first section that located on device or host
   * @param tensor Tensor to be freed on device and host
   * @param size   Size to be freed on device and host
   */
  void free(status::TensorPres& tensor, size_t size) {
    freeDevice(tensor, size);
    freeHost(tensor, size);
  }

  /**
   * Place fragment for the tensor
   * @param tensor Tensor to be placed fragment
   */
  void fragment(status::TensorPres& tensor) {
    impl->fragment(tensor);
  }

  /**
   * Release and fuse the fragment for the tensor
   * @param tensor Tensor to be fused
   */
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

#include <functional>

namespace mori {
namespace utils {

template <typename T>
struct PresentationFunction final {
  inline static void require(T& target) {
    target.require();
  }
  inline static void release(T& target) {
    target.release();
  }
}; // struct AutoReleaseFunction

template <typename T>
struct Presentation final {
 public:
  using PresentationFunctionType = std::function<void(T&)>;

 private:
  T& target;

  PresentationFunctionType require_func;
  PresentationFunctionType release_func;

  std::atomic<bool> presented = false;

 public:
  Presentation(T& _target) : target(_target) {
    require_func = PresentationFunction<T>::require;
    release_func = PresentationFunction<T>::release;
  }
  Presentation(
      T& _target,
      const PresentationFunctionType& _require_func,
      const PresentationFunctionType& _release_func)
      : target(_target),
        require_func(_require_func),
        release_func(_release_func) {}
  inline void require() {
    if (presented)
      throw inited_exception("Target already required.");
    require_func(target);
    presented = true;
  }
  inline void release() {
    if (!presented)
      throw inited_exception("Target not required.");
    release_func(target);
    presented = false;
  }
  ~Presentation() {
    if (presented)
      release();
  }

}; // struct Presentation

} // namespace utils
} // namespace mori

namespace mori {

struct BackendHandle;

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

  // Executor thread
  std::thread executor_thread;
  std::recursive_mutex executor_mutex;

  std::deque<events::ScheduleEvent> activated_events;
  std::mutex queue_m;

  // Memory synchronization information
  std::mutex exec_sync_mutex;
  std::chrono::steady_clock::time_point exec_sync_time_offset;

  std::atomic<bool> half_iter_sync = false;
  std::atomic<bool> iter_sync = false;
  std::atomic<bool> exec_sync = false;
  std::atomic<bool> next_op_sync = false;

  std::atomic<int> iteration = 0;

  // The schedule events are ordered.
  // The operator-triggered events are ordered by the execution sequence of
  // operators. The time-triggered events are ordered by the triggering
  // timepoint.
  std::chrono::steady_clock::time_point current_time_offset;
  std::vector<events::ScheduleEvent>::iterator current_timepoint_event_posi;

  MemoryOperationExecutor executor;

  std::atomic<bool> inited = false;

  // Time-triggered events require these methods to reset the schedule timepoint
  // offset.
  inline long getExecutionTimepoint() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - current_time_offset)
        .count();
  }
  inline void resetExecution() {
    std::unique_lock<std::mutex> queue_lock{queue_m};
    activated_events.clear();
    queue_lock.unlock();

    // Reset execution of timepoint-triggered events.
    current_time_offset = std::chrono::steady_clock::now();
    exec_sync_time_offset = current_time_offset;
    current_timepoint_event_posi = current_eventset.load()->timepoint.begin();

    next_op_sync = false;
  }

  void activateEvents() {
    std::vector<events::ScheduleEvent>& eventset =
        current_eventset.load()->timepoint;
    std::shared_lock<std::shared_mutex> l{events_mutex};

    // Activate timepoint triggered events.
    // Execution triggered events do not need to be activated here.
    long current_exec_timepoint = getExecutionTimepoint();
    auto current_end = std::find_if(
        current_timepoint_event_posi,
        eventset.end(),
        [current_exec_timepoint](const events::ScheduleEvent& event) {
          return event.timepoint > current_exec_timepoint;
        });

    // Retrieve the schedule events that should be triggered.
    std::unique_lock<std::mutex> queue_lock{queue_m};
    while (current_timepoint_event_posi < current_end) {
      if (current_timepoint_event_posi->instant)
        executeEvent(*current_timepoint_event_posi);
      else
        activated_events.push_back(*current_timepoint_event_posi);
      ++current_timepoint_event_posi;
    }
  }

  bool executeEvent(const events::ScheduleEvent& event) {
    status::TensorView tensor_view =
        status.tryReferenceTensor(event.tensor_name);
    if (!tensor_view.isReferenced())
      return false;
    status::TensorPres tensor_pres = tensor_view.reference();

    switch (event.type) {
      case events::ScheduleEventType::copyin:
        // No data to copy in.
        if (tensor_pres.isDeviceAllLocated())
          break;
        if (!tensor_pres.isHostLocated())
          break;
        executor.copyIn(tensor_pres, event.size);
        if (callbacks.count(CallbackStage::postSwapIn))
          callbacks.at(CallbackStage::postSwapIn)(
              event.tensor_name, tensor_pres.getSection(0).device_address);
        (*logger) << LogLevel::debug << "Operator " << event.operator_name
                  << ": tensor " << event.tensor_name
                  << " copied in. (Prefetch)" << endl;
        break;
      case events::ScheduleEventType::copyout:
        // No data to copy out.
        if (!tensor_pres.isDeviceLocated())
          break;
        if (tensor_pres.isHostAllLocated())
          break;
        executor.copyOut(tensor_pres, event.size);
        break;
      case events::ScheduleEventType::swapin:
        // No data to swap in.
        if (tensor_pres.isDeviceAllLocated())
          break;
        if (!tensor_pres.isHostLocated())
          break;
        executor.swapIn(tensor_pres, event.size);
        if (callbacks.count(CallbackStage::postSwapIn))
          callbacks.at(CallbackStage::postSwapIn)(
              event.tensor_name, tensor_pres.getSection(0).device_address);
        (*logger) << LogLevel::debug << "Operator " << event.operator_name
                  << ": tensor " << event.tensor_name
                  << " swapped in. (Prefetch)" << endl;
        break;
      case events::ScheduleEventType::swapout:
        // No data to swap out.
        if (!tensor_pres.isDeviceLocated())
          break;
        if (tensor_pres.isHostAllLocated())
          break;
        executor.swapOut(tensor_pres, event.size);
        if (callbacks.count(CallbackStage::postSwapOut))
          callbacks.at(CallbackStage::postSwapOut)(
              event.tensor_name, tensor_pres.getSection(0).host_address);
        (*logger) << LogLevel::debug << "Operator " << event.operator_name
                  << ": tensor " << event.tensor_name
                  << " swapped out. (Instant)" << endl;
        break;
      case events::ScheduleEventType::freehost:
        // No data to free on host.
        if (!tensor_pres.isHostLocated())
          break;
        executor.freeHost(tensor_pres, event.size);
        break;
      case events::ScheduleEventType::freedev:
        // No data to free on device.
        if (!tensor_pres.isDeviceLocated())
          break;
        executor.freeDevice(tensor_pres, event.size);
        if (callbacks.count(CallbackStage::postSwapOut))
          callbacks.at(CallbackStage::postSwapOut)(
              event.tensor_name, tensor_pres.getSection(0).host_address);
        (*logger) << LogLevel::debug << "Operator " << event.operator_name
                  << ": tensor " << event.tensor_name
                  << " freed on device. (Instant)" << endl;
        break;
      case events::ScheduleEventType::free:
        // No data to free on host and device.
        if (!tensor_pres.isMemoryLocated())
          break;
        executor.free(tensor_pres, event.size);
        break;
      default:
        break;
    }
    return true;
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

        if (exec_sync)
          continue;
        std::unique_lock<std::mutex> l{exec_sync_mutex, std::try_to_lock};
        if (!l.owns_lock())
          continue;
        // Execution of schedule events
        // Activate events should be triggered.
        activateEvents();
        // Execute activated events.
        std::unique_lock<std::mutex> queue_lock{queue_m};
        size_t target_executed_events = activated_events.size();
        size_t current_executed_events = 0;
        queue_lock.unlock();

        while (current_executed_events < target_executed_events) {
          if (half_iter_sync || iter_sync || exec_sync)
            break;
          if (next_op_sync)
            continue;

          queue_lock.lock();
          // Retrieve tensor information.
          if (activated_events.empty())
            return;
          const events::ScheduleEvent& event = activated_events.front();
          queue_lock.unlock();

          try {
            if (executeEvent(event)) {
              // Success execution of event.
              queue_lock.lock();
              activated_events.pop_front();
              queue_lock.unlock();
              ++current_executed_events;
            }
          } catch (memory_insufficience& e) {
            (*logger)
                << LogLevel::debug
                << "Exception in executing memory swapping events, reason: "
                << e.what() << ", " << e.demand() << " unmet." << endl;
            next_op_sync = true;
          } catch (std::exception& e) {
            (*logger)
                << LogLevel::debug
                << "Exception in executing memory swapping events, reason: "
                << e.what() << endl;
          }
        }
        // Currently no more schedule events.
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

  void setOperatorStarted(const std::string& op) {}

  void setOperatorFinished(const std::string& op) {
    next_op_sync = false;
    std::unique_lock<std::mutex> ql{queue_m};
    for (auto& x : current_eventset.load()->execution[op]) {
      if (x.instant)
        executeEvent(x);
      else
        activated_events.push_back(x);
    }
    // logger->submit(LogLevel::debug, "Memory schedule executor moves to next
    // operator.");
  }

  int getIteration() {
    return iteration;
  }
  void setIteration(int _iteration) {
    iteration = _iteration;
  }

  void newIteration() {
    if (!inited)
      throw uninited_exception();
    iter_sync = true;
    while (iter_sync)
      ;
    logger->submit(
        LogLevel::debug, "Memory schedule executor moves to next iteration.");
    ++iteration;
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

  /**
   * @brief Synchronize with memory schedule executor. Prevent further
   * activation and exectuion of memory schedule events.
   * @note  Leverage with mori::utils::Presentation suggested.
   */
  void synchronize() {
    exec_sync = true;
    exec_sync_mutex.lock();
    // Memory insufficient, block the forward schedule events and perform
    // passive memory swapping. Since the memory swapping is performed on the
    // specific copying stream, this synchroization does not lead to further
    // overhead.
    exec_sync_time_offset = std::chrono::steady_clock::now();
  }
  /**
   * @brief Release memory scheduler executor. Proceed further activation and
   * execution of memory schedule events.
   * @note  Leverage with mori::utils::Presentation suggested.
   */
  void release() {
    if (exec_sync_mutex.try_lock()) {
      // Indicating synchronization not locked.
      exec_sync_mutex.unlock();
      throw uninited_exception(
          "Memory Schedule executor not in synchronization.");
    }
    std::chrono::steady_clock::duration synchronization_time_duration =
        std::chrono::steady_clock::now() - exec_sync_time_offset;
    current_time_offset += synchronization_time_duration;
    exec_sync_mutex.unlock();
    exec_sync = false;
  }

  ~MemoryScheduleExecutor() {
    if (inited)
      terminate();

    logger = nullptr;
  }

}; // struct MemoryScheduleExecutor

namespace utils {

template <>
struct PresentationFunction<MemoryScheduleExecutor> {
  inline static void require(MemoryScheduleExecutor& target) {
    target.synchronize();
  }
  inline static void release(MemoryScheduleExecutor& target) {
    target.release();
  }
}; // struct PresentationFunction<MemoryScheduleExecutor>

} // namespace utils
} // namespace mori

namespace mori {
namespace layout {

struct MemoryDefragmentationExecutor {
 private:
  status::MemoryStatus& status;
  layout::MemoryLayout& layout;
  MemoryManager* memory_manager;

  std::map<size_t, std::set<void*>> allocated_regions;
  std::map<size_t, std::set<void*>> idle_regions;

  std::shared_mutex m;

 protected:
  void performCopyDevice(void* src, void* dst, size_t size) {
    assert(src >= dst);

    MemoryRegion region = layout.getMemoryRegion(src);

    status::TensorView tensor_view = status.tryReferenceTensor(region.name);
    if (!tensor_view.isReferenced())
      return; // Cannot move this tensor
    status::TensorPres tensor_pres = tensor_view.reference();

    const status::MemorySection* section = &(tensor_pres.getFirstSection());
    while (section != nullptr) {
      if (section->device_address == src)
        break;
      section = section->next();
    }
    if (section == nullptr)
      throw memory_unmanaged();

    if (utils::address_offset(dst, size) >= src) {
      // No interleaving of the two memory regions.
      memory_manager->salloc(dst, size);
      memory_manager->copyDevice(src, dst, size);
      memory_manager->freeDevice(src);

      layout.recordMemoryAllocateEvent(dst, size, tensor_pres.getName());
      layout.recordMemoryFreeEvent(src);

      allocated_regions[size].insert(dst);
      allocated_regions[size].erase(src);
      idle_regions[size].insert(src);
      idle_regions[size].erase(dst);

    } else {
      // Interleaving of memory regions.
      memory_manager->salloc(dst, utils::address_distance(src, dst));
      bool res = memory_manager->merge(dst, src);
      assert(res);
      memory_manager->copyDevice(src, dst, size);
      void* right = memory_manager->split(dst, size);
      memory_manager->freeDevice(right);

      layout.recordMemoryAllocateEvent(
          dst, utils::address_distance(src, dst), tensor_pres.getName());
      layout.recordMemoryMergeEvent(dst, src);
      layout.recordMemorySplitEvent(dst, size);
      layout.recordMemoryFreeEvent(right);

      allocated_regions[size].insert(dst);
      allocated_regions[size].erase(src);
      idle_regions[utils::address_distance(src, dst)].insert(src);
      idle_regions[utils::address_distance(src, dst)].erase(dst);
    }

    tensor_pres.setMoved(section->offset, dst);
  }

 public:
  MemoryDefragmentationExecutor(
      status::MemoryStatus& _status,
      layout::MemoryLayout& _layout)
      : status(_status), layout(_layout) {}

  inline void setMemoryManager(MemoryManager* _memory_manager) {
    memory_manager = _memory_manager;

    assert(allocated_regions.empty());
    assert(idle_regions.empty());

    MemoryInfo&& info = memory_manager->getMemoryInfo();
    idle_regions[info.device.transient_block.size].insert(
        info.device.transient_block.address);
  }

  inline void recordMemoryAllocateEvent(void* address) {
    if (!layout.isTransient(address))
      throw memory_unmanaged();
    MemoryRegion region = layout.getMemoryRegion(address);

    std::unique_lock<std::shared_mutex> l{m};

    assert(allocated_regions[region.size].count(address) == 0);
    assert(idle_regions[region.size].count(address) == 1);

    allocated_regions[region.size].insert(address);
    idle_regions[region.size].erase(address);
  }

  inline void recordMemoryFreeEvent(void* address) {
    if (!layout.isTransient(address))
      throw memory_unmanaged();
    MemoryRegion region = layout.getMemoryRegion(address);

    std::unique_lock<std::shared_mutex> l{m};

    assert(allocated_regions[region.size].count(address) == 1);
    assert(idle_regions[region.size].count(address) == 0);

    allocated_regions[region.size].erase(address);
    idle_regions[region.size].insert(address);
  }

  std::pair<size_t, size_t> getTransientBlockAllocatableSize(
      size_t granularity) const noexcept {
    std::pair<size_t, size_t> re;
    re.first = 0;
    re.second = 0;
    for (auto& x : idle_regions) {
      if (x.first >= granularity)
        re.first += x.first * x.second.size();
      else
        re.second += x.first * x.second.size();
    }
    return re;
  }

  void performDefragmentation(size_t granularity) {
    auto bp = std::find_if(
        layout.blocks.begin(),
        layout.blocks.end(),
        [](const std::pair<void*, Block>& p) {
          return p.second.type == MemoryBlockType::transient;
        });
    assert(bp != layout.blocks.end());

    std::unique_lock<std::shared_mutex> l{bp->second.m};
    auto& regions = bp->second.regions;

    for (auto p = regions.begin(); p != regions.end(); ++p) {
      if (p->second.allocated)
        continue;
      if (p->second.size >= granularity)
        continue;
      // Find fragmentation
      if (!allocated_regions[p->second.size].empty()) {
        auto q = allocated_regions[p->second.size].rbegin();
        if (*q != p->first) {
          // Fast path
          assert(*q >= p->first);
          performCopyDevice(*q, p->first, p->second.size);
          continue;
        }
      }
      // Slow path
      auto q = p;
      ++q;
      if (q == regions.end())
        break;
      assert(q->second.allocated);
      performCopyDevice(q->first, p->first, q->second.size);
    }
  }

}; // struct MemoryDefragmentationExecutor

} // namespace layout
} // namespace mori

namespace mori {

/**
 * MemorySession
 * Management of a memory session, which is a complete memory lifecycle of a
 * training iteration.
 */
struct MemorySession final {
 private:
  friend struct Frontend;

 public:
  using MemoryFunction = std::function<bool()>;

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
    std::atomic<bool> executing = false;

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
        : session(_session), op(_op), stage(_stage) {}

   public:
    Request(const Request&) = delete;
    Request(Request&& _request) : session(_request.session) {
      op = std::move(_request.op);
      stage = _request.stage;
    }

    void waitTensor(const std::string& tensor) {
      if (!waiting)
        throw uninited_exception();
      if (executing)
        throw inited_exception();

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
        session.op_executor.copyIn(pres, pres.getSize() - pres.getDeviceSize());
      } catch (memory_device_insufficience& e) {
        // Memory on device not insufficience.
        size_t released_size =
            session.waitMemory(pres.getSize(), [this, &pres]() {
              try {
                session.op_executor.copyIn(
                    pres, pres.getSize() - pres.getDeviceSize());
              } catch (memory_device_insufficience& e) {
                return false;
              }
              return true;
            });
        if (!pres.isDeviceAllLocated())
          throw e;
      }
      assert(pres.isDeviceAllLocated());

      if (session.callbacks.count(CallbackStage::postSwapIn))
        session.callbacks.at(CallbackStage::postSwapIn)(
            tensor, pres.getSection(0).device_address);
      (*session.logger) << LogLevel::debug << "Operator: " << op
                        << ", tensor: " << tensor
                        << " swapped in. (Memory access)" << endl;
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

    void setOperationStarted() {
      if (!waiting)
        throw uninited_exception();
      if (executing)
        throw inited_exception();

      session.sch_executor.setOperatorStarted(op);
      session.backend_handle.lock()->submitEvent(events::ExecutionEvent(
          op, events::ExecutionEventType::request, stage));

      executing = true;
    }

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
      if (executing)
        throw inited_exception();
      if (!isTensorWaited(tensor))
        throw status_exception("Tensor not waited.");

      // Do not acquire locks here since the tensor is awaited.
      // Tensor exists since isTensorWaited(tensor) is true.
      status::TensorPres& pres = requested_tensors.at(tensor);
      if (pres.isHostLocated())
        session.op_executor.freeHost(pres, pres.getHostSize());
      assert(!pres.isHostLocated());
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
      if (executing)
        throw inited_exception();
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
      if (executing)
        throw inited_exception();
      if (!isTensorWaited(tensor))
        throw status_exception("Tensor not waited.");

      status::TensorPres& pres = requested_tensors.at(tensor);
      if (pres.isHostLocated())
        session.op_executor.freeHost(pres, pres.getHostSize());
      assert(!pres.isHostLocated());
      pres.setAccessed();

      // emit memory event
      session.backend_handle.lock()->submitEvent(events::MemoryEvent(
          op, tensor, pres.getSize(), events::MemoryEventType::access, stage));
    }

    void setOperationFinished() {
      if (!waiting)
        throw uninited_exception();
      if (!executing)
        throw uninited_exception();
      session.sch_executor.setOperatorFinished(op);
      session.backend_handle.lock()->submitEvent(events::ExecutionEvent(
          op, events::ExecutionEventType::release, stage));

      executing = false;
    }

    void release() {
      if (executing)
        setOperationFinished();
      if (!waiting)
        return;

      for (auto& x : requested_tensors)
        x.second.release();

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

  MemoryInfo memory_info;

  layout::MemoryDefragmentationExecutor defrag_executor;

  Callbacks callbacks;

  Logger* logger;

  ApplicationStage stage = ApplicationStage::forward;

  void setBackendHandle(const std::weak_ptr<BackendHandle>& _backend_handle) {
    backend_handle = _backend_handle;
  }
  void setMemoryManager(MemoryManager* _memory_manager) {
    op_executor.setMemoryManager(_memory_manager);
    defrag_executor.setMemoryManager(_memory_manager);

    memory_info = _memory_manager->getMemoryInfo();
  }
  void setLogger(Logger* _logger) {
    logger = _logger;
  }
  void setCallback(
      CallbackStage stage,
      const std::function<int(const std::string&, void*)>& callback) {
    callbacks.emplace(stage, callback);
  }

  size_t waitTensorMemory(size_t size, const std::string& initial_tensor) {
    std::string tensor_name = initial_tensor;
    while (true) {
      // Since the memory schedule executor is synchronized, a tensor that
      // cannot be referenced must be waited by memory session.
      status::TensorView tensor_view = status.tryReferenceTensor(tensor_name);
      if (!tensor_view.isReferenced())
        return 0;
      status::TensorPres tensor_pres = tensor_view.reference();
      // Do not swap out tensors that already host-only.
      if (!tensor_pres.isDeviceLocated())
        return 0;
      uint8_t* device_address_e =
          (uint8_t*)(tensor_pres.getLastSection().device_address);
      // // Do not swap out persistent or transient tensors.
      if (!layout.isCommon(device_address_e))
        return 0;

      // Prepare to swap out this tensor.
      // Step 1: Locate the first section on device.
      const status::MemorySection* section = &(tensor_pres.getFirstSection());
      while (section != nullptr) {
        if (section->status == status::MemoryStatusType::empty ||
            section->status == status::MemoryStatusType::device ||
            section->status == status::MemoryStatusType::coexist)
          break;
        section = section->next();
      }
      // Since data located on device, a section will be selected finally.
      assert(section != nullptr);

      // Step 2: Locate prev memory region.
      size_t avail_size = 0;
      if (layout.isRegionExist(section->device_address, Direction::prev)) {
        layout::MemoryRegion memory_region_prev =
            layout.getMemoryRegion(section->device_address, Direction::prev);
        if (!memory_region_prev.allocated)
          avail_size = memory_region_prev.size;
      } else
        avail_size = 0;
      if (avail_size >= size)
        return avail_size;

      // Step 3: Calculate swapping amount
      device_address_e += tensor_pres.getLastSection().size;
      if (tensor_pres.getFragment().status == status::MemoryStatusType::empty)
        device_address_e += tensor_pres.getFragment().size;
      size_t releasing_b = tensor_pres.getDeviceSize();
      if (tensor_pres.getFragment().status == status::MemoryStatusType::empty)
        releasing_b += tensor_pres.getFragment().size;
      size_t releasing_size = releasing_b;
      if (releasing_size + avail_size > size)
        releasing_size = size - avail_size;
      // If partically swap tensor, reserve aligned size
      assert(
          utils::get_memory_aligned_size(
              releasing_size, memory_info.device.align_size) >= releasing_size);
      size_t releasing_alignment_size =
          utils::get_memory_aligned_size(
              releasing_size, memory_info.device.align_size) -
          releasing_size;
      if (releasing_size + releasing_alignment_size <=
          tensor_pres.getDeviceSize())
        releasing_size += releasing_alignment_size;
      else
        releasing_alignment_size = 0;
      op_executor.swapOut(tensor_pres, releasing_size);
      size_t releasing_e = tensor_pres.getDeviceSize();
      if (tensor_pres.getFragment().status == status::MemoryStatusType::empty)
        releasing_e += tensor_pres.getFragment().size;

      std::string op_name = tensor_pres.getOperatorName();
      if (callbacks.count(CallbackStage::postSwapOut))
        callbacks.at(CallbackStage::postSwapOut)(
            tensor_name, tensor_pres.getSection(0).host_address);
      (*logger) << LogLevel::debug << "Operator " << op_name << ": tensor "
                << tensor_name << " swapped out. (Memory insufficience)"
                << endl;

      backend_handle.lock()->submitEvent(events::MemoryEvent(
          op_name,
          tensor_name,
          releasing_b - releasing_e,
          events::MemoryEventType::swapout,
          stage));

      assert(releasing_b >= (releasing_e + releasing_alignment_size));
      avail_size =
          avail_size + releasing_b - releasing_e - releasing_alignment_size;
      if (avail_size >= size)
        return avail_size;

      assert(releasing_e == 0);

      if (!layout.isRegionExist(device_address_e))
        return avail_size;
      layout::MemoryRegion region = layout.getMemoryRegion(device_address_e);
      if (!region.allocated) {
        avail_size += region.size;
        if (avail_size >= size)
          return avail_size;
        // Memory demand unmet.
        device_address_e += region.size;
        if (!layout.isRegionExist(device_address_e))
          return avail_size;
        region = layout.getMemoryRegion(device_address_e);
      }
      tensor_name = region.name;
    }
    return 0;
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
        op_executor(_layout),
        defrag_executor(_status, _layout) {}

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

    (*logger) << LogLevel::info << "Iteration: " << sch_executor.getIteration()
              << endl;
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
    (*logger) << LogLevel::debug
              << "Half iteration: " << sch_executor.getIteration() << endl;
    // backend_handle.lock()->
  }

  inline ApplicationStage getStage() {
    return stage;
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
    // if (layout.isTransient(address))
    // defrag_executor.recordMemoryAllocateEvent(address);

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
   * @return Memory size released.
   * @note Currently this method adopts a FIFO strategy that the firstly
   * forward-propagating operator will be firstly released.
   */
  size_t waitMemory(
      size_t size,
      const MemoryFunction& func = []() { return false; }) {
    utils::Presentation<MemoryScheduleExecutor> presentation(sch_executor);
    presentation.require();
    if (func())
      return size;

    size_t avail_size = 0;
    for (auto& op_name : status.getExecutionOrder()) {
      status::OperatorView op_view = status.tryReferenceOperator(op_name);
      if (!op_view.isReferenced())
        continue;
      status::OperatorPres op_pres = op_view.reference();
      // Forward propagation and backward propagation share the same set of
      // tensors. if (op_pres.isBackwardPropagation()) continue;

      for (auto& s : op_pres.getTensors()) {
        // Try to release memory from tensors.
        avail_size = waitTensorMemory(size, s);
        if (avail_size >= size)
          break;
      }
      if (avail_size >= size)
        break;
    }

    // Memory demand unmet, try defragmentation.
    // auto pair = defrag_executor.getTransientBlockAllocatableSize(size);
    // if (pair.second >= size) defrag_executor.performDefragmentation(size);

    if (avail_size >= size)
      (*logger) << LogLevel::info << "Memory insufficient, mori releases "
                << avail_size << " of memory." << endl;
    else
      (*logger) << LogLevel::info << "Mori memory releasing failed, "
                << size - avail_size << " unmet." << endl;

    func();
    presentation.release();
    return avail_size;
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
          // if (layout.isTransient(device_address))
          // defrag_executor.recordMemoryFreeEvent(device_address);
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

    virtual void setCallback(CallbackStage stage, const Callback& callback) = 0;

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
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }
    virtual void registerOperator(
        const status::Operator& operator_status) override {
      (*frontend.logger) << LogLevel::error << "Registering operator "
                         << operator_status.getName()
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }
    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) override {
    //     (*logger)<<LogLevel::error<<"Updating operator "<<op<<" while
    //     frontend not initialized." << endl; throw uninited_exception();
    // }
    virtual void setEntry(const std::string& _op) override {
      (*frontend.logger) << LogLevel::error << "Setting entry operator " << _op
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }

    virtual void setCallback(CallbackStage stage, const Callback& callback)
        override {
      (*frontend.logger) << LogLevel::error
                         << "Setting callbacks while frontend not initialized."
                         << endl;
      throw uninited_exception();
    }

    virtual void start() override {
      (*frontend.logger) << LogLevel::error
                         << "Starting uninitialized frontend." << endl;
      throw uninited_exception();
    }
    virtual bool isStarted() const noexcept override {
      return false;
    }

    virtual MemorySession& getSession() override {
      (*frontend.logger)
          << LogLevel::error
          << "Referencing to session from uninitialized frontend." << endl;
      throw uninited_exception();
    }

    virtual void updateSchedule() override {
      (*frontend.logger) << LogLevel::error
                         << "Updating schedule while frontend not initialized."
                         << endl;
      throw uninited_exception();
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      (*frontend.logger) << LogLevel::error << "Unregistering tensor " << tensor
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }
    virtual void unregisterOperator(const std::string& op) override {
      (*frontend.logger) << LogLevel::error << "Unregistering operator " << op
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }

    virtual void stop() override {
      (*frontend.logger) << LogLevel::error
                         << "Stopping uninitialized frontend." << endl;
      throw uninited_exception();
    }

    virtual void terminate() override {
      (*frontend.logger) << LogLevel::error
                         << "Terminating uninitialized frontend." << endl;
      throw uninited_exception();
    }
  }; // struct UninitedImpl

  struct InitedImpl final : public Impl {
    InitedImpl(Frontend& _frontend) : Impl(_frontend) {}

    virtual void setMemoryManager(MemoryManager* _mem_manager) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting memory manager for initialized frontend."
                         << endl;
      throw inited_exception();
    }

    virtual void setLogger(Logger* _logger) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting logger for initialized frontend." << endl;
      throw inited_exception();
    }

    virtual void init() override {
      (*frontend.logger) << LogLevel::error
                         << "Initializing frontend that already inited."
                         << endl;
      throw inited_exception();
    }
    virtual bool isInited() const noexcept override {
      return true;
    }

    virtual void registerTensor(const status::Tensor& tensor) override {
      frontend.memory_status.registerTensor(tensor);
      (*frontend.logger) << LogLevel::debug << "Tensor " << tensor.getName()
                         << " registered." << endl;
    }

    virtual void registerOperator(
        const status::Operator& operator_status) override {
      frontend.memory_status.registerOperator(operator_status);
      (*frontend.logger) << LogLevel::debug << "Operator "
                         << operator_status.getName() << " registered." << endl;
    }

    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) override {
    //     memory_status.updateOperator(op, tensor_status);
    //     // backend_handle->updateOperator(op, tensor_status);

    //     (*logger)<<LogLevel::debug<<"Operator "<<op<<" updated." << endl;
    // }

    virtual void setEntry(const std::string& _op) override {
      frontend.memory_status.setEntry(_op);
    }

    virtual void setCallback(CallbackStage stage, const Callback& callback)
        override {
      frontend.executor.setCallback(stage, callback);
      frontend.session.setCallback(stage, callback);
    }

    virtual void start() override {
      frontend.backend_handle->submitMemoryStatus(frontend.memory_status);

      frontend.executor.init();
      frontend.backend_handle->start();
      frontend.impl = &frontend.started_impl;
      (*frontend.logger) << LogLevel::debug << "Mori started." << endl;
    }

    virtual bool isStarted() const noexcept override {
      return false;
    }

    virtual MemorySession& getSession() override {
      (*frontend.logger) << LogLevel::error
                         << "Referencing to session from not-started frontend."
                         << endl;
      throw uninited_exception();
    }

    virtual void updateSchedule() override {
      (*frontend.logger) << LogLevel::error
                         << "Updating schedule for not-started frontend."
                         << endl;
      throw uninited_exception();
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      frontend.memory_status.unregisterTensor(tensor);
      (*frontend.logger) << LogLevel::debug << "Tensor " << tensor
                         << " unregistered." << endl;
    }

    virtual void unregisterOperator(const std::string& op) override {
      frontend.memory_status.unregisterOperator(op);
      (*frontend.logger) << LogLevel::debug << "Operator " << op
                         << " unregistered." << endl;
    }

    virtual void stop() override {
      (*frontend.logger) << LogLevel::error << "Stopping non-started frontend."
                         << endl;
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
                         << "Setting memory manager for started frontend."
                         << endl;
      throw inited_exception();
    }
    virtual void setLogger(Logger* _logger) override {
      (*frontend.logger) << LogLevel::error
                         << "Setting logger for started frontend." << endl;
      throw inited_exception();
    }

    virtual void init() override {
      (*frontend.logger) << LogLevel::error
                         << "Initializing frontend that already started."
                         << endl;
      throw inited_exception();
    }
    virtual bool isInited() const noexcept override {
      return true;
    }

    virtual void registerTensor(const status::Tensor& tensor) override {
      (*frontend.logger) << LogLevel::error << "Registering tensor "
                         << tensor.getName() << " while frontend started."
                         << endl;
      throw inited_exception();
    }
    virtual void registerOperator(
        const status::Operator& operator_status) override {
      (*frontend.logger) << LogLevel::error << "Registering operator "
                         << operator_status.getName()
                         << " while frontend started." << endl;
      throw inited_exception();
    }

    // virtual void updateOperator(const std::string& op, const status::Tensor&
    // tensor_status) {
    //     if (!inited) {
    //         (*logger)<<LogLevel::error<<"Updating operator "<<op<<" while
    //         frontend not initialized." << endl; throw uninited_exception();
    //     }
    // }

    virtual void setEntry(const std::string& _op) override {
      (*frontend.logger) << LogLevel::error << "Setting entry operator " << _op
                         << " while frontend started." << endl;
      throw inited_exception();
    }

    virtual void setCallback(CallbackStage stage, const Callback& callback)
        override {
      (*frontend.logger) << LogLevel::error
                         << "Setting callbacks while frontend started." << endl;
      throw inited_exception();
    }

    virtual void start() override {
      (*frontend.logger) << LogLevel::error << "Frontend already started."
                         << endl;
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

      (*frontend.logger) << LogLevel::info
                         << "Memory swapping schedule updated." << endl;
    }

    virtual void unregisterTensor(const std::string& tensor) override {
      (*frontend.logger) << LogLevel::error << "Unregistering tensor " << tensor
                         << " while frontend not initialized." << endl;
      throw uninited_exception();
    }

    virtual void unregisterOperator(const std::string& op) override {
      (*frontend.logger) << LogLevel::error << "Unregistering operator " << op
                         << " while frontend not initialized." << endl;
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
  inline void setCallback(CallbackStage stage, const Callback& callback) {
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
    assert(impl == &uninited_impl);

    backend_handle.reset();
    mem_manager = nullptr;
    logger = nullptr;
  }

}; // struct Frontend

using MemorySwappingManager = Frontend;

} // namespace mori