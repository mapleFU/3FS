#pragma once

#include <atomic>
#include <fmt/format.h>
#include <folly/Likely.h>
#include <folly/ThreadLocal.h>
#include <folly/concurrency/AtomicSharedPtr.h>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/utils/AtomicValue.h"
#include "common/utils/MagicEnum.hpp"
#include "common/utils/Path.h"
#include "common/utils/Result.h"
#include "common/utils/RobinHood.h"
#include "common/utils/Toml.hpp"
#include "common/utils/TypeTraits.h"

DECLARE_string(cfg);

namespace hf3fs {

/*
 * 3FS 配置系统总览
 *
 * - 基于 CRTP 的通用配置框架：用户通过继承 ConfigBase<T> 声明配置类型 T。
 * - 通过一组宏（CONFIG_ITEM/CONFIG_HOT_UPDATED_ITEM/CONFIG_OBJ/CONFIG_SECT 等）在编译期注册
 *   配置项与子节（section）的成员偏移，形成键 → 成员指针 的映射，用于运行时解析与访问。
 * - 支持热更新与原子更新：配置可以从 TOML、字符串、文件、命令行覆盖项进行更新；热更新受每个
 *   项的“是否可热更新”控制；原子更新先克隆校验后提交，避免部分更新导致的中间态。
 * - 线程安全与低开销读取：原始类型使用原子封装（AtomicValue），复杂类型使用线程本地缓存（TLSStore）
 *   提供近似无锁读取；所有结构化更新在互斥锁下执行。
 * - 序列化/反序列化：支持将配置完整或按键转换为 TOML；向量、集合、映射、可选值以及枚举等均有
 *   明确的序列化规则。
 * - 校验与回调：每项可绑定检查器（checker）用于值合法性校验；配置完成更新后触发回调守卫（CallbackGuard）。
 * - 变体类型：通过 CONFIG_VARIANT_TYPE 为同一配置声明多种子节实现，按当前选择的 type 输出与解析。
 *
 * 支持的值类型示例：
 *  - 原始类型：std::string、int64_t、double、bool、枚举
 *  - 容器类型：std::vector、std::set、std::map 及其元素为上述类型或 IConfig 派生类型
 */

// 声明一个子节（section）对象并注册到当前配置类型的节表中；可选提供初始化器对该子节进行默认设定。
#define CONFIG_OBJ(name, cls, ...) /* optional parameter: initializer */                                     \
 public:                                                                                                     \
  cls &name() { return name##_; }                                                                            \
  const cls &name() const { return name##_; }                                                                \
                                                                                                             \
 private:                                                                                                    \
  cls name##_;                                                                                               \
  [[maybe_unused]] bool name##Insert_ = [this]() {                                                           \
    using Self = std::decay_t<decltype(*this)>;                                                              \
    ConfigBase<Self>::sections_[#name] = reinterpret_cast<::hf3fs::config::IConfig Self::*>(&Self::name##_); \
    __VA_OPT__(__VA_ARGS__(name##_);)                                                                        \
    return true;                                                                                             \
  }()

// 声明一个固定容量的子节数组，并将每个元素以 "name#<index>" 注册为独立节；可选提供初始化器设置默认长度。
#define CONFIG_OBJ_ARRAY(name, cls, cap, ...) /* optional parameter: initializer */             \
 public:                                                                                        \
  cls &name(size_t idx) { return name##_[idx]; }                                                \
  const cls &name(size_t idx) const { return name##_[idx]; }                                    \
  size_t name##_length() const {                                                                \
    auto lock = std::unique_lock(mutex_);                                                       \
    return name##Length_;                                                                       \
  }                                                                                             \
  void set_##name##_length(size_t len) {                                                        \
    auto lock = std::unique_lock(mutex_);                                                       \
    if (len < k_capacity_##name) {                                                              \
      name##Length_ = len;                                                                      \
    }                                                                                           \
  }                                                                                             \
                                                                                                \
 private:                                                                                       \
  static constexpr size_t k_capacity_##name{cap};                                               \
  static_assert(k_capacity_##name <= 4096u, "array cap should less than 4096");                 \
  std::array<cls, k_capacity_##name> name##_;                                                   \
  size_t name##Length_ = [this]() {                                                             \
    using Self = std::decay_t<decltype(*this)>;                                                 \
    ConfigBase<Self>::lengths_[#name] = reinterpret_cast<size_t Self::*>(&Self::name##Length_); \
    auto base = reinterpret_cast<::hf3fs::config::IConfig Self::*>(&Self::name##_);             \
    static_assert(sizeof(base) == sizeof(uintptr_t), "sizeof(base) != sizeof(uintptr_t)");      \
    for (auto i = 0ul; i < k_capacity_##name; ++i) {                                            \
      ConfigBase<Self>::sections_[fmt::format(#name "#{}", i)] = base;                          \
      *reinterpret_cast<uintptr_t *>(&base) += sizeof(cls);                                     \
    }                                                                                           \
    auto length = 1ul;                                                                          \
    __VA_OPT__(length = __VA_ARGS__(name##_);)                                                  \
    return length;                                                                              \
  }()

// 使用内联类型方式声明一个子节：T<name> 作为内嵌配置类型，并注册为节。
#define CONFIG_SECT(name, section)                     \
 protected:                                            \
  struct T##name : public ConfigBase<T##name> section; \
  CONFIG_OBJ(name, T##name)

// 声明一个基础配置项并注册到当前配置类型的项表中：
// - defaultValue：默认值；支持原始类型、容器、IConfig 派生类型或可选类型
// - supportHotUpdated：是否允许热更新（true/false）
// - 可选 checker：用于值合法性校验，返回 true 表示合法
#define CONFIG_ADD_ITEM(name, defaultValue, supportHotUpdated, ...) /* optional parameter: checker */   \
 private:                                                                                               \
  using T##name = ::hf3fs::config::ValueType<std::decay_t<decltype(defaultValue)>>;                     \
  using R##name = ::hf3fs::config::ReturnType<T##name>;                                                 \
                                                                                                        \
 public:                                                                                                \
  auto name##_getter() const {                                                                          \
    return [this] { return name(); };                                                                   \
  }                                                                                                     \
  R##name name() const { return name##_.value(); }                                                      \
  bool set_##name(R##name value) { return name##_.checkAndSet(value); }                                 \
                                                                                                        \
 private:                                                                                               \
  ::hf3fs::config::Item<T##name> name##_ = ::hf3fs::config::Item<T##name>(#name, defaultValue, [this] { \
    using Self = std::decay_t<decltype(*this)>;                                                         \
    ConfigBase<Self>::items_[#name] = reinterpret_cast<::hf3fs::config::IItem Self::*>(&Self::name##_); \
    return supportHotUpdated;                                                                           \
  }() __VA_OPT__(, ) __VA_ARGS__)

// CONFIG_ITEM：不支持热更新；CONFIG_HOT_UPDATED_ITEM：支持热更新
#define CONFIG_ITEM(name, defaultValue, ...) CONFIG_ADD_ITEM(name, defaultValue, false, __VA_ARGS__)
#define CONFIG_HOT_UPDATED_ITEM(name, defaultValue, ...) CONFIG_ADD_ITEM(name, defaultValue, true, __VA_ARGS__)

// 为变体类型配置声明类型选择键（type），其值需与已注册的节名一致；toToml/toString 输出当前选中节。
#define CONFIG_VARIANT_TYPE(defaultType)                                        \
  CONFIG_ITEM(type, std::string{defaultType}, [this](const std::string &name) { \
    using Self = std::decay_t<decltype(*this)>;                                 \
    return ConfigBase<Self>::sections_.count(name);                             \
  });                                                                           \
                                                                                \
 public:                                                                        \
  constexpr static inline bool is_variant_type() { return true; }

namespace config {

// IItem 表示一个基础配置项（含值、校验、更新与序列化能力），由宏生成的 Item<T> 实现。
struct IItem {
  virtual ~IItem() = default;
  virtual Result<Void> validate(const std::string &path) const = 0;
  virtual Result<Void> update(const toml::node &node, bool isHotUpdate, const std::string &path) = 0;
  virtual void toToml(toml::table &table) const = 0;
  virtual bool isParsedFromString() const = 0;
  virtual bool operator==(const IItem &other) const = 0;
  virtual String toString() const = 0;
};

class ConfigCallbackGuard;

using KeyValue = std::pair<std::string, std::string>;

inline std::string tomlToString(const toml::node &node) {
  std::stringstream ss;
  ss << toml::toml_formatter(node, toml::toml_formatter::default_flags & ~toml::format_flags::indentation);
  return ss.str();
}

// IConfig 为所有配置类型的抽象接口。
// 生命周期与能力：
// - clonePtr/defaultPtr：以指针形式克隆当前配置或创建默认配置
// - validate/overallValidate：项级逐一校验与整体校验
// - update：从 TOML/字符串/文件/键值对更新；update(isHotUpdate) 控制是否按热更新规则执行
// - atomicallyUpdate：克隆 → 试更新 → 成功后提交，避免中间态
// - toToml/toString：序列化为 TOML/字符串；支持按键输出
// - find：按 "section.item" 或 "name#idx" 等键定位项
// - init：集成命令行解析（--config.<key>=<value>）、文件加载（--cfg）与默认校验
struct IConfig {
  virtual ~IConfig() = default;

  virtual std::unique_ptr<IConfig> clonePtr() const = 0;

  // return a default constructed object
  virtual std::unique_ptr<IConfig> defaultPtr() const = 0;

  // validate configuration items one by one.
  virtual Result<Void> validate(const std::string &path = {}) const = 0;

  // validate configuration items as a whole.
  virtual Result<Void> overallValidate() const { return Void{}; }

  // update configuration. [thread safe]
  virtual Result<Void> update(const toml::table &table, bool isHotUpdate = true, const std::string &path = {}) = 0;

  // update configuration from a string. [thread safe]
  Result<Void> update(std::string_view str, bool isHotUpdate);

  // update configuration from a file. [thread safe]
  Result<Void> update(const Path &path, bool isHotUpdate);

  // update configuration from a series of key-values. [thread safe]
  Result<Void> update(const std::vector<KeyValue> &updates, bool isHotUpdate);

  // remove callback guard.
  virtual void removeCallbackGuard(ConfigCallbackGuard *guard) const = 0;

  // convert configuration to TOML. [thread safe]
  virtual toml::table toToml() const = 0;

  // convert configuration of specific section/item to TOML. [thread safe]
  virtual Result<toml::table> toToml(std::string_view key) const = 0;

  // find item by a key.
  virtual Result<IItem *> find(std::string_view key) = 0;

  virtual bool operator==(const IConfig &other) const = 0;

  struct ItemDiff {
    String key;
    String left;
    String right;
  };

  size_t diffWith(const IConfig &other, std::span<ItemDiff> diffs, size_t pos = 0) const {
    return diffWith(other, {}, diffs, pos);
  }

  virtual size_t diffWith(const IConfig &other, String path, std::span<ItemDiff> diffs, size_t pos) const = 0;

  virtual Result<Void> atomicallyUpdate(std::string_view str, bool isHotUpdate) = 0;
  Result<Void> atomicallyUpdate(std::string_view str) { return atomicallyUpdate(str, /*isHotUpdate=*/true); }

  virtual Result<Void> atomicallyUpdate(const Path &path, bool isHotUpdate) = 0;
  Result<Void> atomicallyUpdate(const Path &path) { return atomicallyUpdate(path, /*isHotUpdate=*/true); }

  virtual Result<Void> atomicallyUpdate(const std::vector<KeyValue> &updates, bool isHotUpdate) = 0;
  Result<Void> atomicallyUpdate(const std::vector<KeyValue> &updates) {
    return atomicallyUpdate(updates, /*isHotUpdate=*/true);
  }

  virtual Result<Void> validateUpdate(std::string_view str, bool isHotUpdate) = 0;
  Result<Void> validateUpdate(std::string_view str) { return validateUpdate(str, /*isHotUpdate=*/true); }

  // convert configuration to string. [thread safe]
  std::string toString() const { return tomlToString(toToml()); }

  // initialize config from command line and config files.
  Result<Void> init(int *argc, char ***argv, bool follyInit = true);
};

template <class T>
struct ConfigValueToTomlNode {
  template <class V>
  auto operator()(V &&v) {
    if constexpr (std::is_enum_v<T>) {
      return magic_enum::enum_name(std::forward<V>(v));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return static_cast<int64_t>(v);
    } else if constexpr (std::is_same_v<T, Path>) {
      return v.string();
    } else if constexpr (std::derived_from<T, IConfig>) {
      return v.toToml();
    } else if constexpr (requires { std::string(v.toString()); }) {
      return v.toString();
    } else {
      return std::forward<V>(v);
    }
  }
};

Result<std::vector<KeyValue>> parseFlags(std::string_view prefix, int &argc, char *argv[]);

class ConfigCallbackGuard {
 public:
  explicit ConfigCallbackGuard(const config::IConfig *cfg, auto &&...f)
      : cfg_(cfg),
        func_(std::forward<decltype(f)>(f)...) {}
  ~ConfigCallbackGuard() { dismiss(); }

  void setCallback(auto &&f) { func_ = std::forward<decltype(f)>(f); }
  void callCallback() { func_ ? func_() : void(); }
  void dismiss() { cfg_ ? std::exchange(cfg_, nullptr)->removeCallbackGuard(this) : void(); }

 private:
  const config::IConfig *cfg_;
  std::function<void()> func_;
};

template <class T>
// TLSStore：为非原始、不可平凡拷贝的值提供线程本地的只读缓存视图。
// - 写入通过共享指针整体替换，递增版本号；读取线程使用 ThreadLocal 缓存减少原子加载频率。
class TLSStore {
 public:
  TLSStore(T &&value)
      : ptr_(std::make_shared<T>(std::move(value))) {}
  TLSStore(const TLSStore &o)
      : ptr_(o.ptr_.load(std::memory_order_acquire)) {}

  const T &value() const {
    auto &cache = *tlsCache_;
    size_t latest = version_.load(std::memory_order_acquire);
    if (UNLIKELY(cache.version != latest)) {
      cache.ptr = ptr_.load(std::memory_order_acquire);
      cache.version = latest;
    }
    return *cache.ptr;
  }

  template <class V>
  void setValue(V &&value) {
    ptr_.store(std::make_shared<T>(std::forward<V>(value)));
    ++version_;
  }

  TLSStore &operator=(const TLSStore &o) {
    ptr_.store(o.ptr_.load(std::memory_order_acquire));
    ++version_;
    return *this;
  }

 private:
  std::atomic<uint64_t> version_ = 1;
  folly::atomic_shared_ptr<T> ptr_{std::make_shared<T>()};
  struct Cache {
    std::shared_ptr<const T> ptr;
    uint64_t version = 0;
  };
  folly::ThreadLocal<Cache> tlsCache_;
};

template <class T>
using RemoveOptional = typename std::conditional_t<is_optional_v<T>, T, std::optional<T>>::value_type;
template <class T>
inline constexpr bool IsPrimitive = std::is_trivially_copyable_v<T> && sizeof(T) <= 8;
template <class T>
using ReturnType = std::conditional_t<IsPrimitive<T>, T, const T &>;
template <class T>
using StoreType = std::conditional_t<IsPrimitive<T>, AtomicValue<T>, TLSStore<T>>;
template <class T>
using ValueType = std::conditional_t<std::is_same_v<T, const char *>, std::string, T>;

template <typename T>
// 将 TOML 节点解析为期望的值类型 T，支持：原始类型、枚举、可从字符串构造的类型、IConfig 派生类型等。
inline Result<T> tomlNodeToValue(const toml::node &node) {
  return node.visit([&](auto &&el) -> Result<T> {
    using TE = std::decay_t<decltype(el)>;
    if constexpr (!toml::is_value<TE>) {
      return makeError(StatusCode::kConfigInvalidType, fmt::format("{} isn't value", typeid(TE).name()));
    } else {
      using E = typename TE::value_type;

      if constexpr (std::is_same_v<T, E>) {
        return *el;
      } else if constexpr (std::is_same_v<T, bool>) {
        // do not allow any implicit conversion to bool
        return makeError(StatusCode::kConfigInvalidType,
                         fmt::format("implicit conversion from {} to bool is not allowed", typeid(E).name()));
      } else if constexpr (std::is_floating_point_v<T>) {
        if constexpr (std::is_same_v<E, int64_t> || std::is_floating_point_v<E>) {
          return *el;
        } else {
          return makeError(StatusCode::kConfigInvalidType,
                           fmt::format("{} is not int64_t or floating types", typeid(E).name()));
        }
      } else if constexpr (std::is_enum_v<T>) {
        if constexpr (std::is_same_v<E, std::string>) {
          auto opt = magic_enum::enum_cast<T>(*el);
          if (opt) {
            return opt.value();
          } else {
            return makeError(StatusCode::kConfigInvalidValue, fmt::format("Value {}", *el));
          }
        } else {
          return makeError(StatusCode::kConfigInvalidType,
                           fmt::format("{} is enum but {} is not string", typeid(T).name(), typeid(E).name()));
        }
      } else if constexpr (requires { Result<T>{T::from(*el)}; }) {
        return T::from(*el);
      } else if constexpr (std::is_constructible_v<T, E>) {
        try {
          return T(*el);
        } catch (const std::exception &e) {
          return makeError(
              StatusCode::kConfigInvalidType,
              fmt::format("Throws when constructing T ({}), E is {}: ", typeid(T).name(), typeid(E).name(), e.what()));
        }
      } else {
        return makeError(StatusCode::kConfigInvalidType,
                         fmt::format("T is {}, E is {}", typeid(T).name(), typeid(E).name()));
      }
    }
  });
}

template <class T>
// Item<T>：单个配置项的通用实现。
// - 支持热更新标记、值检查器（checker）、容器/映射/可选值的专用更新与序列化逻辑。
// - update(node, isHotUpdate) 会在值不同且允许更新时提交；否则报错或忽略。
class Item : public IItem {
 public:
  Item(std::string name, T defaultValue, bool supportHotUpdate, std::function<bool(ReturnType<T>)> checker = nullptr)
      : value_(std::move(defaultValue)),
        name_(std::move(name)),
        supportHotUpdate_(supportHotUpdate),
        checker_(checker ? std::move(checker) : [](ReturnType<T>) { return true; }) {}

  ReturnType<T> value() const { return value_.value(); }

  template <class V>
  void setValue(V &&value) {
    value_.setValue(std::forward<V>(value));
  }

  bool operator==(const IItem &other) const final {
    if (typeid(*this) != typeid(other)) {
      return false;
    }
    return *this == *reinterpret_cast<const Item *>(&other);
  }

  bool operator==(const Item &other) const { return this == std::addressof(other) || value() == other.value(); }

  bool operator==(const T &val) const { return value() == val; }

  String toString() const final {
    toml::table table;
    toToml(table);
    auto *view = table.get(name_);
    if constexpr (is_optional_v<T>) {
      return view ? tomlToString(*view) : "nullopt";
    } else {
      return tomlToString(*view);
    }
  }

  bool checkAndSet(ReturnType<T> value) {
    if (checker_(value)) {
      setValue(value);
      return true;
    }
    return false;
  }

  Result<Void> update(const toml::node &node, bool isHotUpdate, const std::string &path) final {
    try {
      bool enableUpdate = supportHotUpdate_ || !isHotUpdate;
      if constexpr (is_vector_v<T> || is_set_v<T>) {
        return updateVectorOrSet(node, enableUpdate, path);
      } else if constexpr (is_map_v<T>) {
        return updateMap(node, enableUpdate, path);
      } else {
        return updateNormal(node, enableUpdate, path);
      }
    } catch (const std::exception &e) {
      return makeError(StatusCode::kConfigUpdateFailed, fmt::format("name: {}, error: {}", path, e.what()));
    }
  }

  Result<Void> validate(const std::string &path) const final {
    if (!checker_(value())) {
      return makeError(StatusCode::kConfigValidateFailed, fmt::format("Check failed: {}", path));
    }
    return Void{};
  }

  void toToml(toml::table &table) const override {
    if constexpr (is_vector_v<T> || is_set_v<T>) {
      toml::array array;
      for (const auto &item : value()) {
        array.emplace_back(ConfigValueToTomlNode<typename T::value_type>{}(item));
      }
      table.insert_or_assign(name_, std::move(array));
    } else if constexpr (is_map_v<T>) {
      toml::table inlineTable;
      for (const auto &pair : value()) {
        inlineTable.emplace(pair.first, ConfigValueToTomlNode<typename T::mapped_type>{}(pair.second));
      }
      inlineTable.is_inline(true);
      table.insert_or_assign(name_, std::move(inlineTable));
    } else if constexpr (is_optional_v<T>) {
      if (value().has_value()) {
        table.insert_or_assign(name_, ConfigValueToTomlNode<RemoveOptional<T>>{}(value().value()));
      }
    } else {
      table.insert_or_assign(name_, ConfigValueToTomlNode<T>{}(value()));
    }
  }

  bool isParsedFromString() const override {
    return std::is_constructible_v<RemoveOptional<T>, std::string> || std::is_enum_v<RemoveOptional<T>>;
  }

 protected:
  Result<Void> updateNormal(const toml::node &node, bool enableUpdate, const std::string &path) {
    try {
      auto res = tomlNodeToValue<RemoveOptional<T>>(node);
      if (res.hasError()) {
        return makeError(StatusCode::kConfigUpdateFailed, fmt::format("name: {}, error: {}", path, res.error()));
      }
      if (!checker_(res.value())) {
        return makeError(StatusCode::kConfigValidateFailed, fmt::format("Check failed: {}", path));
      }
      if (res.value() != value()) {
        if (enableUpdate) {
          setValue(std::move(res.value()));
        } else {
          return makeError(StatusCode::kConfigUpdateFailed, fmt::format("Not support hot update: {}", path));
        }
      }
      return Void{};
    } catch (const std::exception &e) {
      return makeError(StatusCode::kConfigUpdateFailed, fmt::format("throws when update {}: {}", path, e.what()));
    }
  }

  Result<Void> updateVectorOrSet(const toml::node &node, bool enableUpdate, const std::string &path) {
    using I = typename T::value_type;

    if (!node.is_array()) {
      return makeError(StatusCode::kConfigInvalidType, fmt::format("Not array: {}", path));
    }
    auto arr = node.as_array();
    if (arr == nullptr) {
      return makeError(StatusCode::kConfigInvalidValue, fmt::format("Empty array: {}", path));
    }

    T tmp;
    for (size_t i = 0; i < arr->size(); ++i) {
      const auto &e = (*arr)[i];
      auto res = [&]() -> Result<I> {
        if constexpr (std::derived_from<I, IConfig>) {
          if (!e.is_table()) {
            return makeError(StatusCode::kConfigInvalidType, fmt::format("Not table: {}[{}]", path, i));
          }
          I v;
          RETURN_ON_ERROR(v.update(*e.as_table(), /*isHotUpdate=*/false));
          return v;
        } else {
          return tomlNodeToValue<RemoveOptional<I>>(e);
        }
      }();
      if (res.hasError()) {
        return makeError(StatusCode::kConfigUpdateFailed, fmt::format("name: {}, error: {}", path, res.error()));
      }
      if constexpr (is_set_v<T>) {
        auto [it, succ] = tmp.emplace(std::move(res.value()));
        if (!succ) {
          return makeError(StatusCode::kConfigUpdateFailed, fmt::format("name: {}, set value repeats", path));
        }
      } else {
        tmp.push_back(std::move(res.value()));
      }
    }
    if (!checker_(tmp)) {
      return makeError(StatusCode::kConfigValidateFailed, fmt::format("Array check failed: {}", path));
    }
    if (tmp != value()) {
      if (enableUpdate) {
        value_.setValue(std::move(tmp));
      } else {
        return makeError(StatusCode::kConfigUpdateFailed, fmt::format("Not support hot update: {}", path));
      }
    }
    return Void{};
  }

  Result<Void> updateMap(const toml::node &node, bool enableUpdate, const std::string &path) {
    using I = typename T::mapped_type;

    if (!node.is_table()) {
      return makeError(StatusCode::kConfigInvalidType, fmt::format("Not map: {}", path));
    }
    auto table = node.as_table();
    if (table == nullptr) {
      return makeError(StatusCode::kConfigInvalidValue, fmt::format("Empty map: {}", path));
    }

    T tmp;
    for (const auto &e : *table) {
      auto res = tomlNodeToValue<I>(e.second);
      if (res.hasError()) {
        return makeError(StatusCode::kConfigUpdateFailed, fmt::format("name: {}, error: {}", path, res.error()));
      }
      auto [it, succ] = tmp.emplace(std::string{e.first.str()}, std::move(res.value()));
      if (!succ) {
        return makeError(StatusCode::kConfigRedundantKey,
                         fmt::format("name: {}, redundant key: {}", path, e.first.str()));
      }
    }
    if (!checker_(tmp)) {
      return makeError(StatusCode::kConfigValidateFailed, fmt::format("Array check failed: {}", path));
    }
    if (tmp != value()) {
      if (enableUpdate) {
        value_.setValue(std::move(tmp));
      } else {
        return makeError(StatusCode::kConfigUpdateFailed, fmt::format("Not support hot update: {}", path));
      }
    }
    return Void{};
  }

 private:
  StoreType<T> value_;
  std::string name_;
  bool supportHotUpdate_ = false;
  std::function<bool(ReturnType<T>)> checker_;
};

inline std::string concat(const std::string &a, const std::string &b) { return a.empty() ? b : a + "." + b; }

}  // namespace config

using config::ConfigCallbackGuard;

template <class Parent>
// ConfigBase<Parent>：配置类型的通用基类（CRTP）。
// 核心机制：
// - sections_/items_/lengths_ 记录成员偏移，用于运行时按键路由更新/序列化/查找
// - update(table)：递归处理子节与数组节（name#idx），对未知键报错；完成后触发回调并整体校验
// - toToml()：输出所有项与节；对变体类型仅输出当前选定节；数组节按长度输出为数组
// - atomicallyUpdate(...)：克隆副本后试更新，成功再提交到当前对象
// - validate(...)：逐节逐项调用其校验器；最终执行整体校验
// - diffWith(...)：计算两份配置的差异项（键、左右值）
class ConfigBase : public config::IConfig {
 protected:
  ConfigBase() = default;
  ConfigBase(const ConfigBase &o)
      : sections_(o.sections_),
        items_(o.items_),
        lengths_(o.lengths_) {}
  ConfigBase &operator=(const ConfigBase &) { return *this; }

 public:
  using config::IConfig::update;
  Result<Void> update(const toml::table &table, bool isHotUpdate = true, const std::string &path = {}) final {
    auto self = reinterpret_cast<Parent *>(this);
    auto lock = std::unique_lock(mutex_);

    for (auto &pair : table) {
      auto name = std::string(pair.first.str());
      auto &node = pair.second;

      if (lengths().count(name)) {
        if (!(node.is_array_of_tables() || (node.is_array() && node.as_array()->empty()))) {
          return makeError(StatusCode::kConfigInvalidType, fmt::format("Not array of table: {}", name));
        }
        auto array = node.as_array();
        auto size = array->size();
        for (auto i = 0ul; i < size; ++i) {
          auto itemName = fmt::format("{}#{}", name, i);
          auto itemTable = array->get_as<toml::table>(i);
          if (itemTable == nullptr) {
            return makeError(StatusCode::kConfigInvalidType, fmt::format("Not a table: {}", itemName));
          }
          if (!sections().count(itemName)) {
            return makeError(StatusCode::kConfigInvalidType, fmt::format("Array length exceed: {}", itemName));
          }
          RETURN_ON_ERROR(
              (self->*(sections().at(itemName))).update(*itemTable, isHotUpdate, config::concat(path, itemName)));
        }
        self->*(lengths().at(name)) = size;
      } else if (sections().count(name)) {
        if (!node.is_table()) {
          return makeError(StatusCode::kConfigInvalidType, fmt::format("Not table: {}", name));
        }
        RETURN_ON_ERROR(
            (self->*(sections().at(name))).update(*node.as_table(), isHotUpdate, config::concat(path, name)));
      } else if (items().count(name)) {
        RETURN_ON_ERROR((self->*(items().at(name))).update(node, isHotUpdate, config::concat(path, name)));
      } else {
        return makeError(StatusCode::kConfigRedundantKey, fmt::format("Invalid key: {}", config::concat(path, name)));
      }
    }

    // call callbacks after update.
    for (auto &callback : callbacks_) {
      callback->callCallback();
    }
    return overallValidate();
  }

  toml::table toToml() const final {
    auto lock = std::unique_lock(mutex_);

    auto self = reinterpret_cast<const Parent *>(this);
    toml::table table;
    for (auto &pair : items()) {
      (self->*pair.second).toToml(table);
    }
    if constexpr (requires { Parent::is_variant_type; }) {
      auto it = sections().find(self->type());
      if (it != sections().end()) {
        table.insert_or_assign(it->first, (self->*it->second).toToml());
      }
      return table;
    }
    for (auto &pair : sections()) {
      if (pair.first.find('#') != std::string::npos) {
        continue;
      }
      table.insert_or_assign(pair.first, (self->*pair.second).toToml());
    }
    for (auto &pair : lengths()) {
      toml::array array;
      for (auto i = 0ul; i < self->*pair.second; ++i) {
        array.emplace_back((self->*sections().at(fmt::format("{}#{}", pair.first, i))).toToml());
      }
      table.insert_or_assign(pair.first, std::move(array));
    }
    return table;
  }

  Result<toml::table> toToml(std::string_view key) const final {
    if (key.empty()) return toToml();

    auto self = reinterpret_cast<const Parent *>(this);

    auto pos = key.find('.');
    if (pos == std::string_view::npos) {
      auto iit = items().find(key);
      if (iit != items().end()) {
        toml::table table;
        (self->*(iit->second)).toToml(table);
        return table;
      }
      auto sit = sections().find(key);
      if (sit != sections().end()) {
        return (self->*(sit->second)).toToml();
      }
      return makeError(StatusCode::kConfigKeyNotFound, key);
    } else {
      auto section = key.substr(0, pos);
      key.remove_prefix(pos + 1);
      auto it = sections().find(section);
      if (it != sections().end()) {
        return (self->*(it->second)).toToml(key);
      }
      return makeError(StatusCode::kConfigKeyNotFound, section);
    }
  }

  Result<config::IItem *> find(std::string_view key) final {
    auto self = reinterpret_cast<Parent *>(this);

    auto pos = key.find('.');
    if (pos == std::string_view::npos) {
      auto it = items().find(std::string{key});
      if (it == items().end()) {
        return makeError(StatusCode::kConfigValidateFailed);
      }
      return &(self->*(it->second));
    } else {
      auto section = key.substr(0, pos);
      key.remove_prefix(pos + 1);
      auto it = sections().find(std::string{section});
      if (it == sections().end()) {
        return makeError(StatusCode::kConfigValidateFailed);
      }
      return (self->*(it->second)).find(key);
    }
  }

  std::unique_ptr<config::IConfig> clonePtr() const final { return std::make_unique<Parent>(clone()); }

  std::unique_ptr<config::IConfig> defaultPtr() const final { return std::make_unique<Parent>(); }

  // clone current configuration. [thread safe]
  Parent clone() const {
    auto lock = std::unique_lock(mutex_);
    return reinterpret_cast<const Parent &>(*this);
  }

  // copy from another configuration. [thread safe]
  void copy(const Parent &o) {
    std::scoped_lock lock(mutex_, o.mutex_);
    reinterpret_cast<Parent &>(*this) = o;
  }

  // atomically update configuration from a string.
  using IConfig::atomicallyUpdate;
  Result<Void> atomicallyUpdate(std::string_view str, bool isHotUpdate) final {
    Parent newConfig = clone();
    RETURN_ON_ERROR(newConfig.update(str, isHotUpdate));
    auto res = update(str, isHotUpdate);
    XLOGF_IF(FATAL, !res, "Unexpected update error: {}", res.error());
    return Void{};
  }

  // atomically update configuration from a file.
  Result<Void> atomicallyUpdate(const Path &path, bool isHotUpdate) final {
    Parent newConfig = clone();
    RETURN_ON_ERROR(newConfig.IConfig::update(path, isHotUpdate));
    auto res = update(path, isHotUpdate);
    XLOGF_IF(FATAL, !res, "Unexpected update error: {}", res.error());
    return Void{};
  }

  Result<Void> atomicallyUpdate(const std::vector<config::KeyValue> &updates, bool isHotUpdate) final {
    Parent newConfig = clone();
    RETURN_ON_ERROR(newConfig.IConfig::update(updates, isHotUpdate));
    auto res = update(updates, isHotUpdate);
    XLOGF_IF(FATAL, !res, "Unexpected update error: {}", res.error());
    return Void{};
  }

  Result<Void> validateUpdate(std::string_view str, bool isHotUpdate) final {
    Parent newConfig = clone();
    RETURN_ON_ERROR(newConfig.IConfig::update(str, isHotUpdate));
    return Void{};
  }

  Result<Void> validate(const std::string &path = {}) const final {
    auto self = reinterpret_cast<const Parent *>(this);
    for (auto &pair : sections()) {
      RETURN_ON_ERROR((self->*pair.second).validate(config::concat(path, pair.first)));
    }
    for (auto &pair : items()) {
      RETURN_ON_ERROR((self->*pair.second).validate(config::concat(path, pair.first)));
    }
    return overallValidate();
  }

  // add callback guard.
  std::unique_ptr<ConfigCallbackGuard> addCallbackGuard(auto &&...func) const {
    auto guard = std::make_unique<ConfigCallbackGuard>(this, std::forward<decltype(func)>(func)...);
    auto lock = std::unique_lock(mutex_);
    callbacks_.insert(guard.get());
    return guard;
  }

  bool operator==(const config::IConfig &other) const final {
    if (typeid(*this) != typeid(other)) {
      return false;
    }
    if (this == std::addressof(other)) {
      return true;
    }
    return *this == *reinterpret_cast<const Parent *>(&other);
  }

  bool operator==(const Parent &other) const {
    ItemDiff diffs[1];
    return diffWith(other, std::span(diffs)) == 0;
  }

  using config::IConfig::diffWith;
  size_t diffWith(const config::IConfig &other, String path, std::span<ItemDiff> diffs, size_t pos) const final {
    XLOGF_IF(FATAL,
             typeid(*this) != typeid(other),
             "Call diffWith on different config types: {} and {}",
             typeid(*this).name(),
             typeid(other).name());
    if (this == std::addressof(other)) {
      return pos;
    }
    return diffWith(*reinterpret_cast<const Parent *>(&other), std::move(path), diffs, pos);
  }

  size_t diffWith(const Parent &other, String path, std::span<ItemDiff> diffs, size_t pos) const {
    auto &self = *reinterpret_cast<const Parent *>(this);
    auto makePath = [&path](const String &name) { return path.empty() ? name : fmt::format("{}.{}", path, name); };

    for (const auto &[name, item] : items()) {
      auto &selfItem = self.*item;
      auto &otherItem = other.*item;
      if (!(selfItem == otherItem)) {
        diffs[pos++] = ItemDiff{makePath(name), selfItem.toString(), otherItem.toString()};
        if (pos >= diffs.size()) return pos;
      }
    }
    for (const auto &[name, sec] : sections()) {
      auto &selfSec = self.*sec;
      auto &otherSec = other.*sec;
      pos = selfSec.diffWith(otherSec, makePath(name), diffs, pos);
      if (pos >= diffs.size()) return pos;
    }
    return pos;
  }

 protected:
  // remove callback guard.
  void removeCallbackGuard(ConfigCallbackGuard *guard) const final {
    auto lock = std::unique_lock(mutex_);
    callbacks_.erase(guard);
  }

  const auto &sections() const noexcept { return sections_; }
  const auto &items() const noexcept { return items_; }
  const auto &lengths() const noexcept { return lengths_; }

 protected:
  mutable std::mutex mutex_;
  // offsets of sections.
  std::map<std::string, config::IConfig Parent::*, std::less<>> sections_;
  // offsets of items.
  std::map<std::string, config::IItem Parent::*, std::less<>> items_;
  // length of section arrays.
  std::map<std::string, size_t Parent::*, std::less<>> lengths_;
  // callbacks after update.
  mutable std::set<ConfigCallbackGuard *, std::less<>> callbacks_;
};

namespace ConfigCheckers {
template <typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

template <typename T>
concept Container = requires(T t) {
  t.empty();
  t.size();
};

template <Arithmetic T, T threshold>
inline bool checkGE(T val) {
  return val >= threshold;
}

struct CheckPositive {
  template <Arithmetic T>
  bool operator()(T val) const {
    return val > 0;
  }
};

inline constexpr CheckPositive checkPositive;

template <Arithmetic T>
inline bool isPositivePrime(T n) {
  if (n < 2) return false;
  for (T f = 2; f * f <= n; f++)
    if (n % f == 0) return false;
  return true;
}

template <Arithmetic T>
inline bool checkNotNegative(T val) {
  return val >= 0;
}

struct CheckNotEmpty {
  template <Container C>
  bool operator()(const C &c) const {
    return !c.empty();
  }
};

inline constexpr CheckNotEmpty checkNotEmpty;

template <Container C>
inline bool checkEmpty(const C &c) {
  return c.empty();
}
}  // namespace ConfigCheckers

}  // namespace hf3fs

FMT_BEGIN_NAMESPACE

template <>
struct formatter<hf3fs::config::IConfig> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const hf3fs::config::IConfig &config, FormatContext &ctx) const {
    return formatter<std::string_view>::format(config.toString(), ctx);
  }
};

FMT_END_NAMESPACE
