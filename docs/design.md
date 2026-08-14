# Cordis-RS 设计 Note（nucleus）

> 状态：P1/P2/P3 已实现并通过 /check 深度审查；Wasm 边界有真实 guest 组件的集成测试（见 §5.2 guest 工具链事实）。
> 来源：对 [deepseek-ai/deepseek-harness](https://github.com/deepseek-ai/deepseek-harness) 与 [cordiverse/cordis](https://github.com/cordiverse/cordis) 的研究（2026-08），以及据此制定的 Rust 移植方案 v2。
> 本文档是项目的唯一设计事实来源：实现与本文冲突时，要么改实现，要么改本文并说明原因。

---

## 1. 研究结论：cordis 是什么

dsh（DeepSeek Harness）是 TypeScript 编写的 agent harness，架构口号是 "everything is a plugin"——模型适配器、工具注册表、会话日志、agent loop 本身都是可替换插件，没有特权核心。其插件框架 cordis 以源码形式 vendor 进 dsh（`vendor/cordis/`，上游 cordiverse/cordis 4.0.0-rc.7，DeepSeek 叠加 18 处本地修改：fiber 生命周期加固、事务性 Loader 协调、HMR 精确 watch 等）。dsh 的 Rust 代码只有 `native/landlock-run` 沙箱启动器，框架层完全没有 Rust 实现——本项目的空白地带。

cordis 论文《A Programming Paradigm for Spatiotemporal Composability》把插件系统形式化为两个正交维度：

- **时间可组合性（temporal）**：组件移除时其副作用可被完全回滚（revertible effects）。
- **空间可组合性（spatial）**：组件间依赖被声明并响应式管理（reactive coeffects）。

### 1.1 核心机制盘点（Rust 版需要复刻的本体）

| 机制 | cordis (TS) 语义 |
|---|---|
| Fiber | 插件实例的运行时句柄；状态机 `PENDING → LOADING → ACTIVE → UNLOADING → DISPOSED`，可进入 `FAILED`；`dispose()` 递归卸载子插件并 await 所有异步清理 |
| 可逆 effects | 一切注册（事件监听、服务、子插件、外部资源）都是 effect，卸载时逆序回滚；外部资源用 `ctx.effect(() => { acquire; return disposer })` 包装 |
| inject / coeffects | 插件声明 `inject: ['tools']`，loader 持有它在 PENDING 直到服务存在；服务消失 → 依赖者被卸载并在服务回归时自动重载；加载顺序完全由依赖决定 |
| 服务 | 具名能力挂在 `ctx.<key>`；消费方按名查找，配置层可换 provider，依赖者对 新实现"干净地重启" |
| 事件 | 5 种分发模式：`emit`（同步广播）、`parallel`、`serial`（首个真值短路）、`bail`（同步 serial）、`waterfall`（around 中间件：`(args, next)`，可包装/否决；观察型必须调 `next()`） |
| Loader | `cordis.yml` 行式配置按 `id` diff：只 mount/unmount/reconfigure 变化的行；`disabled` 保留条目跳过挂载；groups + `isolate` 服务隔离 |
| HMR | 卸载回滚一切 + 加载跟随依赖 → 热替换 = unload + reload，文件 watch 触发 |

### 1.2 dsh 如何消费 cordis（对我们设计的校准）

- 子系统 = 服务：`ctx.sessions`、`ctx.tools`、`ctx.agents`、`ctx.llm`……
- 扩展点 = 事件：turn/step 为持久 session 事件，`agent/request`、`llm/stream`、`tools/*` 为 waterfall。
- 配置分层：bundle → profile patch → home patch → CLI overlay，全部落到 cordis.yml 行的协调。

---

## 2. 关键洞察：拆解"运行时动态可插拔"

Rust 里这是两个正交问题，架构必须分层：

1. **组合动态性**（cordis 真正的创新）：运行时挂载/卸载/重载插件实例、配置协调、依赖驱动激活、副作用可逆回滚。**纯安全 Rust 进程内完整实现**，不碰 ABI。
2. **代码动态性**：不停进程加载新机器码。Rust 无稳定 ABI，必须选宿主/插件边界：Wasm 组件模型 / C-ABI dylib / 子进程 RPC。

JS 版靠动态语言把两者混在一起免费拿到；Rust 版用 `PluginSource` trait 解耦：core 不关心插件代码从哪来。

## 3. 技术选型（已定）

- **Wasm 边界 = WASI 0.3 原生 async**（激进路线，用户确认）：WASI 0.3.0 已于 2026-06-11 批准，wasmtime ≥43 稳定支持；原生 `stream<T>`/`future<T>` 进 canonical ABI，`[async]` WIT 函数直接映射 Rust async。当前锁 **wasmtime 47**。
- 取消语义（drop future 即取消）曾有 DoS CVE（GHSA-xjhv-v822-pf94，drop `call_async` future 会 panic），**42.0.0 起修复**；锁 ≥43 同时拿到 WASI 0.3 与修复。取消 = fiber 卸载超时的地基。
- host 侧用 stable Rust（1.96），nightly 只允许出现在 guest 工具链。
- 服务句柄一律 `Arc<dyn Trait>`，绝不让 `&` 逃逸出 fiber 生命周期——级联重载时才能安全 drop 旧服务。
- 动态加载备选（dylib / 子进程 RPC）不实现，仅保留 `PluginSource` 扩展点。

## 4. 架构

```
┌─────────────────────────────────────────────────────┐
│  应用（agent harness 等）                             │
├─────────────────────────────────────────────────────┤
│  cordis-loader   yml 行式配置、按 id diff 协调、       │
│                  disabled/groups/isolate、HMR watch   │
├─────────────────────────────────────────────────────┤
│  cordis-core     Fiber 状态机、effect 回滚栈、          │
│                  (scope,name) 服务注册表、事件分发、     │
│                  依赖级联重载          （纯安全 Rust）   │
├─────────────────────────────────────────────────────┤
│  PluginSource trait  ┌─ StaticSource（编译期注册表）    │
│  load(name,config)   ├─ WasmSource（wasmtime47/W0.3）  │
│  -> Arc<dyn Plugin>  └─ （预留 dylib / 进程外）         │
└─────────────────────────────────────────────────────┘
```

### 4.1 概念映射表

| cordis (TS) | cordis-rs | 设计要点 |
|---|---|---|
| `ctx.greeter` 动态属性 | `ctx.service::<dyn Greeter>("greeter") -> Result<Arc<dyn Greeter>>` | `(ScopeId, name)` 双键 + `TypeId` 校验；`Box<dyn Any>` 内存 `Arc<T>`（T 可为 `dyn Trait`），downcast_ref 取回 |
| `ctx.effect(() => disposer)` | `ctx.effect(async { acquire; disposer_closure }).await` | 每 fiber 一个 LIFO 回滚栈；disposer 返回 `BoxFuture<'static, ()>`；卸载逆序发起并 await |
| Fiber 状态机 | `enum FiberState` 完整复刻 | `FAILED` 时也回滚已登记的部分 effects（dsh 加固点）；卸载整体带超时（默认 5s，`App::unload_timeout` 可调），超时后标记 Disposed 并记录错误（尽力而为语义，文档明示） |
| 依赖级联 | registry 记录 `服务 → provider fiber`，inject 扫描反查依赖者 | 服务注销 → 依赖者逆序卸载并**重置回 PENDING**（保留 entry）→ 服务回归 → `wake_pending` 自动重挂载 |
| `waterfall` | `TypedNext<A, O>` 中间件链 | listener `(ctx, args, next)`；`next.run()` 深入链条；不调 next = veto；`on_waterfall_prepend` 提供 `prepend: true` |
| `cordis.yml` | `cordis-loader` 同款字段 `id/name/source/config/disabled/group` | 按 id diff 出 Mount/Unmount/Reconfigure/Noop；无 id 行每次生成新 id（故意保留 cordis 行为：任何编辑都 remount，文档警告"总是写 id"） |
| isolate groups | `ScopeId` 作用于服务键 | 查找顺序：本 scope → 全局；provide 落在本 fiber scope |
| bail | 未实现 | 一切皆 async 的运行时里 bail≈serial；serial 返回首个非 null 真值（文档说明） |

### 4.2 模块归属

```
crates/cordis-core     error / fiber / events / context / plugin / app + tests/
crates/cordis-loader   RowSpec 解析、diff、协调执行、notify HMR + tests/
crates/cordis-wasm     bindgen 宿主、WasmSource、ChannelProducer、fuel/内存限制
wit/deps/host.wit      宿主接口包 cordis:host@0.1.0（唯一权威，升级=接口变更）
wit/plugin.wit         guest world 包 cordis:plugin@0.1.0
examples/wasm-guest/   guest 源码参考（不进 workspace，需 wasm32 p3 target）
```

> WIT 布局注意：world 与它 import 的接口必须在**两个不同的包**里
> （同包会报 dependency cycle）；bindgen 的目录解析要求接口放进
> `deps/` 子目录。这是从实现中踩出来的约束。

## 5. WIT 契约 v0.1（最终形态，已随实现定稿）

```wit
// wit/deps/host.wit — package cordis:host@0.1.0
interface host {
  record event { name: string, payload: string }
  register-effect: func(label: string) -> u64;               // 效果登记，返回 id
  subscribe: async func(pattern: string) -> stream<event>;    // 原生流订阅
  emit: func(name: string, payload: string);                  // fire-and-forget
  call-service: async func(service: string, method: string, arg: string)
    -> result<string, string>;                                // Invokable 服务代理
}
// wit/plugin.wit — package cordis:plugin@0.1.0
world plugin {
  import cordis:host/host@0.1.0;
  export init: async func(config: string) -> result;          // 即 cordis apply
  export dispose-effect: async func(handle: u64) -> result;   // LIFO 反注册回调
}
```

与草案的差异（实现驱动）：效果句柄从 resource 改为 `u64`（去掉 v0 用不上的
resource 方法负担，v0.2 升级路径清晰）；`waterfall-next`/waterfall-handle
移出 v0.1——契约里没有任何入口能把 handle 交给 guest，接一半的契约不如不
接；`subscribe` 声明为 `[async]`——见 5.1。

### 5.1 wasmtime 47 codegen 形态（实现确认的事实）

- **sync WIT import 落在 `Host` trait**（`&mut self`，不可失败）；**async
  import 落在 `HostWithStore<T>`**——关联函数，参数 `&Accessor<T, Self>`，
  实现写给 `HasSelf<HostState>`（满足 `HasData` 投影）。`subscribe` 声明
  async 正是为了让宿主拿到 store 访问权来调 `StreamReader::new`。
- **导出调用走 `Store::run_concurrent` + `Accessor`**；闭包必须用
  **async 闭包语法**（`async |accessor| {...}`，Rust 1.85+）——普通
  `|x| async move {}` 会触发 "AsyncFnOnce not general enough" 的 HRTB
  限制，急切构造 future 也绕不过。
- `Config::async_support` 已废弃，取而代之 `Config::concurrency_support(true)`。
- 宿主→guest 的流通过自定义 `StreamProducer` 实现：tokio mpsc Receiver 的
  `poll_recv` 桥进 `Destination`（`VecBuffer`），零长度读作为 readiness 探测
  返回 Pending。
- 事件扇出策略：`on_any` tap 同步 `try_send` 进有界 channel——慢 guest 丢
  事件也不阻塞宿主分发路径（防 reentrancy 的设计约束）。

### 5.2 guest 工具链事实（实测，2026-08）

- **不存在 `wasm32-wasip3` target**（stable/nightly 均无；WASI 0.3 是
  包/API 层变化而非 target）。`plugin` world 不 import 任何 WASI，故 guest
  用 **stable 的 `wasm32-unknown-unknown`** 即可构建。
- 构建链：wit-bindgen **0.51**（`generate!` 需要 `generate_all` 裸 flag；
  guest 侧 `Guest` trait 对 async 导出要求返回 Future（用 `async fn` 实现）；
  契约中无错误载荷的 `result` 映射为 `Result<(), ()>`）→ `wasm-tools
  component new`（1.256.0）封装为组件。全流程脚本化于
  `scripts/build-guest.sh`。
- 测试 fixture `tests/fixtures/hello.wasm` 由该脚本产出并入库（含来源
  README）；`tests/guest.rs` 用它验证完整生命周期：init（register-effect +
  emit）、异步 call-service 往返、卸载时 dispose-effect 以 LIFO 被调——
  实际观测事件序列 `["init effect=0", "calc=3", "dispose effect=0"]`。
- 错误类型是 `wasmtime::Error`（不再是 anyhow）。
- fuel（默认 10M/调用）+ ResourceLimiter 内存上限（64MiB）；epoch interruption
  留给 v0.2（fuel 已覆盖 init/dispose 的确定性预算）。
- 卸载语义：fiber 的 disposer 栈先跑 guest 的 `dispose-effect`（LIFO），
  最后 drop Store——在途 guest 调用被原生取消（依赖 ≥42 的 CVE 修复）。

## 6. 关键决策记录

1. **`Arc<dyn Trait>` 服务句柄**（不做 `&` 逃逸）：级联重载安全 drop 的前提。
2. **PluginSource 解耦两种动态性**：core 对代码来源零假设；StaticSource 零开销路径，WasmSource 第三方路径。
3. **WASI 0.3 原生 async，无 0.2 兼容层**：用户明确的激进路线；锁 wasmtime 47（WASI 0.3 + 取消 CVE 修复），升级 = 契约回归测试。
4. **卸载确定性优先**：disposer LIFO + 级联逆拓扑 + 整体超时；FAILED 也回滚部分 effects。
5. **wake_pending 用 AtomicBool 防重入风暴**：apply→provide→wake 的嵌套由外层循环统一收敛。
6. **不引入 nightly host**：激进只发生在 Wasm 边界。

## 7. 阶段与验收（全部完成 ✅）

| 阶段 | 交付 | 验收（测试名，全部通过） |
|---|---|---|
| P1 ✅ | cordis-core + WIT v0.1 | `unload_rewinds_everything_in_reverse_order`（LIFO 基线）；`failed_apply_unwinds_partial_effects`；`inject_pending_then_activation_and_cascade_reload`（空间基线）；`disposing_a_never_activated_fiber_removes_it`；`waterfall_transform_and_veto` / `waterfall_prepend_runs_before_ordinary`；`serial_first_truthy_short_circuits`；`scoped_services_shadow_and_fall_back`；`type_mismatch_is_reported` |
| P2 ✅ | cordis-wasm（wasmtime 47） | `real_guest_full_lifecycle` / `remount_gets_a_fresh_guest_instance` / `missing_wasm_file_is_a_mount_failure_not_a_hang`（真实 guest 组件）+ 4 项宿主单测（pattern 匹配、registry 扇出、内存限制、路径解析）；构建链 `scripts/build-guest.sh` |
| P3 ✅ | cordis-loader | `diff_by_id_only_touches_changed_rows`；`unchanged_rows_do_not_remount`；`disabled_keeps_entry_and_skips_mount`；`isolate_groups_get_private_service_instances`；`hmr_reload_on_file_change`（真实文件 watch） |

实现期间发现并修复：dispose 一个从未激活的 PENDING fiber 曾是 no-op
（条目泄漏）——已修复并有回归测试；子插件卸载最初是"先全部子级再父级"的
拓扑序，改为把子卸载作为 disposer 压入父的 LIFO 栈（更忠实 cordis：挂载
于两个父 effect 之间的子插件，回滚时也回退在这两者之间）。

**/check 深度审查（双专家并行）后的加固**，全部带回归测试或语义对齐：
- panicking apply 会把 `waking` 标志卡死为 true（调度器永久失效）→
  `catch_unwind` 包裹 apply + WakeGuard(Drop) + 满足性复检重入；
- apply 失败/回退后已 provide 的服务残留（幽灵服务满足 inject、依赖者卡
  Active）→ 错误路径集中移除服务并级联 repend 依赖者；
- apply 期间依赖者逃过级联过滤（只筛 Active）→ apply 完成后在锁内复检
  injects，不满足则回退 Pending 而非激活；
- 卸载超时丢弃未运行的 disposer → 路由/tap 泄漏 → 增加集中式
  `purge_fiber_registrations`（幂等，超时也清理）；
- 非存活 fiber 的 provide/监听/tap 注册泄漏 → 统一 FiberNotLive 拒绝
  （Loading/Active 之外）；孤儿子挂载（父已死）改为报错；
- Wasm：dispose-effect 从未运行（drop_session 注册在 init 之后，LIFO 先
  drop Store，Weak 升级失败静默跳过）→ 注册顺序修正；零长度读返回无 waker
  的 Pending 会永久挂起 guest → 改为 Completed（规范许可）；fuel 是只减不
  增的池 → 每次 guest 进入前重新装填；
- Loader：失败行进入 current 导致永久不再重试 → 失败行移出 current（下轮
  重试）；dispose 错误 `?` 中断整趟协调 → 记录并继续；isolate 组 scope 每
  轮重新分配导致 Reconfig 拆散组 → 组 scope 跨轮 memoization + diff 纳入
  scope 变化；HMR `with_poll_interval` 在事件后端是 no-op → 移除，防抖改为
  事后排水合并。

## 8. 风险与开放项

已确认并在 v0.1 接受的限制（审查后修订）：
- **repend-during-Loading 的双 apply 交错**：fiber 处于 Loading 时被级联
  repend，旧 apply 未被取消/等待，与新 apply 可能并发。防线：非存活 fiber
  的注册一律拒绝（FiberNotLive），后续注册被丢弃并告警。完整的 per-fiber
  apply 串行化（取消或互斥）留给 v0.2。
- 快照后分发可能把一个在途事件投递给刚被移除的监听器（cordis TS 同款语
  义，benign）；监听器的 scope 已随快照同锁捕获，不再有 GLOBAL 回退错配。
- 订阅模式过滤当前是广播（tap 侧 pattern 传下去了，`dispatch` 匹配在
  v0.1 已实现 exact / `prefix*` / `*`——但慢 guest 丢事件策略意味着
  `stream` 不保证投递，只保证不死锁）。
- Wasm 插件的 config 在 `PluginSource::load` 时固化进 WasmPlugin 实例，
  级联重挂载复用同一实例（config 稳定）；改配置走 loader Reconfig 重新
  load——与静态插件生命周期一致。

- wasmtime / wit-bindgen / wasm-component-ld 三者版本必须匹配（错配先例：wasmtime#12385 "invalid leading byte"）；升级视为接口变更。
- `Accessor` 并发 reentrancy 深度（guest 处理事件时再 call-service 再触发事件）：P2 用深度计数熔断，超界返回错误而非死锁。
- wasm guest target 需要时用 nightly，CI 固定版本号；host 不受影响。
- 卸载超时是尽力而为语义：超时后 fiber 标记 Disposed、错误入诊断，不保证 guest 内部状态干净（Wasm 情形整 Store 直接丢弃，天然干净）。
- `bail` 未实现（async 运行时里退化为 serial）；如未来出现同步热路径需求再补。

## 9. 参考资料

- dsh 仓库与文档：README / docs/architecture.md / docs/cordis-primer.md / docs/cordis-tutorial（01-07 章）/ vendor/README.md
- cordis：cordiverse/cordis；论文 cordiverse/paper《A Programming Paradigm for Spatiotemporal Composability》
- WASI 0.3：bytecodealliance.org/articles/WASI-0.3；wasi.dev/releases/wasi-p3；wasi.dev roadmap（wasmtime 43+）
- 取消 CVE：GHSA-xjhv-v822-pf94（≥42 修复）；Debian tracker CVE-2026-27195
- wasmtime 组件异步：docs.wasmtime.dev/api/wasmtime/component；wasmCloud "WASI P3"（Guest trait async 形态）
