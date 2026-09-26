# 长期运行与恢复手册

适用于单个专用账户/交易对的 Linux/POSIX 进程。本文提供操作步骤和模板，仓库中的工具不会自动配置账户、安装 systemd 服务或启动实盘。

## 1. 部署前人工核对

1. 两个账户位于选定部署，API 权限、交易 dex 保证金桶及合约均正确。Hyperliquid agent key 与主账户地址不能混淆；Lighter mainnet 与 Robinhood chain 的账户/key 独立。
2. 合约标的、数量单位、数量/价格精度、报价币及最小订单一致；配置费率与真实账户费率一致。USDG 和 USDC 不按无风险等价处理。
3. 为机器人使用专用账户，不混入人工交易、其他策略或定时转账。不得多进程操作同账户同市场；不同 state 路径不构成交易隔离。
4. 先采集有效数据并检查元数据、缺失/重复分钟和市场时段。资金费没有独立入场模型；不要把价差统计当成包含持仓成本的回测。
5. 检查实际依赖解析结果、官方 SDK 固定版本、发布 SHA 和依赖审计。只通过 import/单测不构成实盘验收。
6. 确保可用保证金、低仓位 cap、订单预算、磁盘余量和可靠 UTC 时钟同步。调整门槛后先运行 `--check-config`。

## 2. 文件与权限

推荐目录；路径为示例，需要操作员明确替换：

| 目录 | 内容 | 要求 |
| --- | --- | --- |
| `/opt/entropy-arb` | 固定版本源码、venv、只读工具 | 服务账户只读 |
| `/etc/entropy-arb/config.yaml` | 审核后的策略配置 | 不存私钥 |
| `/etc/entropy-arb/credentials.env` | 实盘凭证 | 服务账户可读、权限 0600、不进入 Git/备份明文 |
| `/var/lib/entropy-arb/state/<pair>/` | SQLite journal、WAL、SHM、lock | 本地持久磁盘、服务账户读写 |
| `/var/lib/entropy-arb/logs/<pair>/` | 日志、日行情 CSV、交易 CSV、health | 服务账户读写、有容量报警 |

将 systemd `WorkingDirectory` 指向 `/var/lib/entropy-arb`，则 config 中相对的 `state/`、`logs/` 落在持久目录。不要把 state 放到临时目录或不可靠网络盘。

离线检查：

```bash
/opt/entropy-arb/.venv/bin/entropy-rh-arb --check-config --symbol REPLACE_SYMBOL --hedge lighter-rh --config /etc/entropy-arb/config.yaml
```

此检查不读取凭证、不访问网络。它打印实际 `pair_id` 和运行文件路径；记录这些值供监控使用。另行人工确认凭证文件内容与权限。

## 3. 启动与 systemd

`deploy/entropy-arb.service.example` 是模板。替换用户、路径、标的和 hedge 部署后由操作员安装与启用。模板含 `--live`，启用服务就是实盘授权；采集服务改为 `--record-only` 并去掉凭证参数。

- `Restart=on-failure` 用于进程异常退出，设有限次数和退避。HALTED/RECOVERING 是业务状态，健康失败不触发重新提交订单或重建状态。
- `TimeoutStopSec` 必须至少覆盖 `execution.shutdown_timeout_sec + 15`；模板为 90 秒，默认应用预算为 30 秒。增加应用预算时同步修改服务值。
- SIGTERM/SIGINT 请求正常关闭；不要用 SIGKILL 作为日常停止方式。超时强杀仍可能发生，因此所有发送前的订单意图都必须留在 journal。
- 升级前停止服务，确认未决订单和实际仓位，保留状态及旧发布；使用新 venv 安装、离线校验后人工决定启动。

模板尚未安装或启动；跨平台 wheel 和 systemd 实机行为需要在目标 Linux 主机验收。

## 4. 健康报警

engine 约每 5 秒原子写入 `runtime.health_file`。schema 1 的字段包括时间戳、pair、状态、停机/恢复原因、未决订单数、净 base/USD、两边盘口 fresh 及 stopped；开仓 gate 和 recorder 状态也会输出。

```bash
/opt/entropy-arb/.venv/bin/python /opt/entropy-arb/tools/healthcheck.py /var/lib/entropy-arb/logs/REPLACE_PAIR_ID/health.json --max-age 15 --pair-id REPLACE_PAIR_ID
```

| 状态/检查 | 报警处理 |
| --- | --- |
| `READY` + 开仓允许、无未决单、盘口有效 | 健康；仍需外部账户与成交核对 |
| `RECORD_ONLY` + 两边有效行情 | 采集健康；不表示存在实盘能力 |
| `PAUSED` | 核查行情、余额/仓位时效、保证金、订单预算或磁盘；保持进程，避免重启清零限频 |
| `RECOVERING` | 停止新增敞口；观察订单读回、净敞口和超时，不重复提交同一意图 |
| `HALTED` | 操作员核对两边实际账户和未决单，按恢复流程处理 |
| `STOPPED`、health 过期/损坏/缺失 | 核查进程、日志、磁盘和持久状态；先确认是否仍有交易所订单 |

默认 `--max-age 15`。退出码 0 表示本地可观测条件满足；1 表示暂停/恢复/异常状态，2 表示文件或 schema 问题。都不应作为盲目 restart 的依据。接入已有报警系统时可对持续 PAUSED 去抖，RECOVERING/HALTED/未决单应保留即时通知。

## 5. 未决订单与重启恢复

1. 停止新的人工/自动操作，保存 engine 日志、health、交易记录、订单 ID、实际账户仓位及交易所成交历史。
2. 用只读工具检查 journal：

   ```bash
   python /opt/entropy-arb/tools/journal_status.py /var/lib/entropy-arb/state/REPLACE_PAIR_ID/orders.sqlite3
   ```

3. journal 的 `prepared`、`submitted`、`unknown` 与 `terminal` 分别表示本地意图、提交标记、未确认结果和已确认终态。仅未写提交标记的 prepared 意图可证明未发送；超时、零仓位或“未找到”不能证明已提交订单没有成交。
4. 使用订单身份和场所成交历史核实每个未决订单，核对两边合约仓位与保证金。保持数据库，重启后程序会按原身份进行读回；不要删除条目、重建 DB 或更换路径来绕过恢复。
5. 无法取得确定终态时保持 HALTED，联系交易所或人工核实。若必须人工减仓，保留成交证据，并确认机器人停止，避免与其 reduce-only 恢复并发。
6. 重启遇到 journal 身份不一致、锁被占用或损坏时先核查账户/部署/路径，不使用空 DB 绕过。

### 人工解除持久化 HALT

权益峰值和 halt 原因持久化，重启不会自动清除。确认所有未决订单已经终结、两边实际仓位与保证金核对完成，且原故障原因已处理后，操作员才可以在停止服务的情况下运行：

```bash
python /opt/entropy-arb/tools/journal_status.py /var/lib/entropy-arb/state/REPLACE_PAIR_ID/orders.sqlite3 --acknowledge-halt
```

该显式命令持同一个独占锁；有未决订单或运行中的持锁进程时拒绝操作。它原子清除 halt 原因并重置权益峰值基准，保留订单/仓位记录。它不读取交易所账户，不证明已经净平仓，也不恢复交易服务。执行后再次离线校验和检查账户，再由操作员决定启动。

## 6. 账户权益、资金费与回撤解释

`risk.max_session_loss_usd` 是专用账户相关交易 dex 保证金桶权益的持久化峰值回撤门槛。不同 HIP-3 dex 不应借用 core 或另一个 dex 的权益作为其保证金。相同账户/桶不要重复汇总。

权益变化包含真实资金费、费用、价格变化，以及外部入金、出金或人工操作；因此专用账户是理解回撤门槛的必要条件。独立的净仓位保护并不能消除报价币基差风险。余额缺失/过期或低于 free collateral 门槛会暂停新增敞口，不应通过关闭风控让服务“变绿”。

## 7. 日志、日文件和磁盘

- engine 日志使用 size-based rotation；默认单文件 10 MiB、保留 5 份备份，即约 60 MiB 上限。`--no-dashboard` 同时写控制台和文件。
- 行情按 UTC 日切分；不要压缩当天仍可能被写入的文件。跨 UTC 日的失败重试可能补写旧日分片，先确认 recorder 没有待写行，再归档旧日文件。
- 行情 CSV 行携带 schema/bar_id/pair_id/symbol/entropy_dex/hedge_venue，用于验证市场身份；没有报价币/费用字段，必须另行保留审核后的配置和实际费率。归档保持这些字段，不把旧无身份 CSV 插入新分片。
- 交易日志按 UTC 日切分为 `trades-YYYY-MM-DD.csv`；日交易分片与 journal 会增长；设置每个 pair 的保留政策、容量监控和备份。状态数据库及未决单不允许按普通日志保留政策删除。
- 默认 `runtime.min_disk_free_mb` 为 256 MiB；生产应预留更高阈值和操作系统空间。磁盘不足时新增敞口应停止；修复磁盘后核对 recorder backlog 与 journal，不用删除状态“腾空间”。

推荐每日检查日分片完整性并归档到备份存储，记录校验和；归档前后仍应能用 `tools/analyze.py --csv '.../minutes-*.csv'` 验证元数据一致性与重复分钟。

### SQLite 备份

活跃 WAL 模式下，不要只复制 `orders.sqlite3` 而遗漏 WAL。安全选择：停止机器人后确认退出，整体保留 DB、`-wal`、`-shm` 和 lock 文件；或者使用 SQLite 的 online backup API 创建一致性备份，并同时保留订单证据与发布/config 版本。不要在运行中修改/删除 WAL/SHM。

恢复备份前确认机器人停止及实际账户状态；较旧备份可能缺少已经发送的订单，不能自动重播缺失意图。

## 8. 依赖与升级边界

`requirements-lock.txt` 记录本次 CPython 3.13/macOS 基础与测试环境；`requirements-live-lock.txt` 记录干净的运行依赖环境（不包含审计/构建工具链）。目标 Linux 需要重新验证原生 wheel、签名库加载、`pip check` 和离线测试；签名 SDK 的 import smoke 不会初始化 signer，也不证明实盘成功。

WebSockets 可使用系统/环境代理；基础依赖包含 `python-socks[asyncio]>=2.8,<3`（本次固定测试版本 2.8.2）。遇到代理连接失败先检查运行账户的网络路径和代理认证；不要把代理变量或凭证打印进共享日志，不要为让进程连接而擅自改用户代理。

Lighter 固定 commit 的上游依赖将 `urllib3` 限制为 `<2.1`。这类依赖安全债需要逐条审核真实可达路径、上游修复和接口兼容性；不能据此宣称“没有漏洞”，也不能用 `--no-deps` 强制替换版本而跳过测试。上线前对完整部署环境运行独立依赖审计，记录未扫描/VCS 包及失败结果，保留可回退发布。

2026-09-26，本次独立审计在加入 SOCKS 后的基础/测试环境 23 包中没有发现已知记录；在隔离 SDK 环境 47 包中发现 `urllib3==2.0.7` 的 12 条记录，按 advisory ID 合并为 6 条：`CVE-2024-37891`、`CVE-2025-50181`、`CVE-2025-66418`、`CVE-2025-66471`、`CVE-2026-21441`、`CVE-2026-44431`。该次扫描无跳过包，但不能覆盖未知漏洞、运行时可达性或后来变化。

部分修复要求 urllib3 2.2.2/2.5/2.6.3/2.7，与上游 `<2.1` 约束冲突；未经补丁的上游 SDK 环境依赖安全审计未通过。默认安装现通过下面的可重复构建来修复过时的元数据约束，签名器 Python/native payload 保持不变；仍须完成目标环境审计和生产验收。官方示例公告：[认证头泄漏修复](https://github.com/urllib3/urllib3/security/advisories/GHSA-34jh-p97f-mpxf)。

### Lighter 的可重复依赖修复

```bash
python scripts/build_lighter_wheel.py
python -m pip install -r requirements-live-lock.txt
python -m pip check
```

构建脚本固定官方 commit `a38b6405f362fc14a562fe7a97df03f3ee756bc1`，校验 archive 哈希、原 wheel 和修改后 wheel；产物在 `.wheelhouse/`，失败会非 0 退出。它仅将 `METADATA` 的 urllib3 范围改为 `>=2.7,<3` 并重算 `RECORD`。所有 Python 与 native 文件与固定官方源码一致；主实盘要求固定 `urllib3==2.8.0`。

最终 wheel SHA-256 为 `04a886111d433ed97fa049c21c3f227e8511fe539bc20e797ccf84bb2683354a`；构建工具固定 setuptools 83.0.0、wheel 0.46.3、packaging 26.3。

最终干净 Python 3.13 环境的 `pip check`、无凭证 SDK import 和安装入口离线检查通过；`pip-audit` 共扫描 52 个条目，0 个已知漏洞记录，1 个跳过项是未发布到 PyPI 的本项目（其代码由审查/测试覆盖）。旧 urllib3 告警在该依赖图中消除，但不代表未知漏洞或实盘权限已经验证。

官方源码 archive SHA-256 为 `8f7fddb7a887bbf81450a08a5dd2366f5a79ccd8d23807d390ef179fc859e8f6`。构建结果/版本变更前不得跳过这些校验或复用不明来源 wheel；元数据修复不等于对交易所行为的保证。

建议使用新的隔离 venv。已有未经修复的同版本 SDK 不能靠包版本号区分，必须确认安装的是本地 wheel、依赖图无冲突，再按完整流程验收。`pip install .[live]` 仅包含 HL 与公共安全依赖；Lighter 需要这一步本地构建，打包元数据不隐式下载旧约束的 VCS SDK。

## 9. 验收记录

至少分别记录：离线配置检查、测试/静态检查、wheel 安装入口、SDK import/interface、目标主机进程与信号、实时行情与元数据、账户权益桶、极小规模订单身份/成交读回、未知结果恢复及停止后无新增单。任何一层未执行，都应明确标为未验证。
