# Entropy–Robinhood–Lighter Arbitrage

本仓库为 2026-09-26 23:04:53（北京时间）的独立加固版本，基于[原仓库](https://github.com/w343153618/Entropy-Robinhood-Lighter-Arbitrage)的 `b0f02e9` 完成审查与修复。详细记录见 [加固报告](review/2026-09-26-hardening.md)。

双场所永续合约价差交易工具：一条腿固定为 Hyperliquid 上的 **Entropy（`io` dex）**；对冲腿可以选择 Lighter mainnet、Lighter Robinhood chain 或 Hyperliquid trade.xyz。

| `--hedge` | 场所 | 报价币 | 默认 taker fee 配置 |
| --- | --- | --- | --- |
| `lighter` | Lighter mainnet | USDC | 0 bps |
| `lighter-rh` | Lighter Robinhood chain | USDG | 0 bps |
| `tradexyz` | Hyperliquid trade.xyz | USDC | 1 bps |

费用配置必须以你的真实账户为准。程序没有模拟成交模式；**只有显式 `--live` 才运行实盘策略**。`--record-only` 只收集行情，`--check-config` 只做离线配置校验，两者都不读取凭证文件、不初始化签名器。

> 原项目推荐链接：[Entropy](https://entropy.io/?r=satoshi)、[Lighter Robinhood chain](https://robinhoodchain.lighter.xyz/?referral=147343QY)。

## 快速开始

需要 CPython 3.13 或更新版本；本次验证目标为 3.13。Linux/POSIX 适合长期运行；状态数据库的独占进程锁使用 `fcntl`。

```bash
git clone https://github.com/w343153618/Entropy-Robinhood-Lighter-Arbitrage-20260926-230453.git
cd Entropy-Robinhood-Lighter-Arbitrage-20260926-230453
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp config.example.yaml config.yaml
entropy-rh-arb --check-config --symbol SNDK --hedge lighter-rh --config config.yaml
```

安装后的 `entropy-rh-arb` 与源码目录里的 `python main.py` 使用同一个入口。`--symbol`、`--hedge` 和运行模式必须明确指定；漏写模式会报错退出。

### 1. 先采集数据

```bash
entropy-rh-arb --record-only --symbol SNDK --hedge lighter-rh --config config.yaml
```

默认每秒采样、每分钟落盘；按 UTC 日期写入 `logs/<pair>/minutes-YYYY-MM-DD.csv`。`<pair>` 包含交易标的、Entropy dex、对冲部署和身份摘要。运行离线校验时会打印实际路径。

CSV 行携带 `schema_version`、`bar_id`、`pair_id`、`symbol`、`entropy_dex`、`hedge_venue`。这些字段验证市场身份；报价币和费率需要另行核对配置与真实账户，CSV 并不记录它们。旧版无身份字段的 CSV 只能显式用于诊断，不能与新分片混合。

```bash
python tools/analyze.py --csv 'logs/<pair>/minutes-*.csv' --hours 24
```

把 `<pair>` 替换成校验输出的实际目录。分析器拒绝混合市场/无效数字，处理重复分钟；低于 `--min-samples` 的分钟被过滤，样本少会提示。它不保证拒绝所有缺失分钟或不足数据，应人工检查覆盖范围。记录的 edge 是费用前数据，`--fees-bps` 需要传入两腿实际 taker fee 之和；默认 0。分钟统计不能重建毫秒级订单顺序、成交深度或完整资金费收益，输出的阈值是研究起点。

### 2. 配置账户，人工确认后实盘

先阅读 [长期运行与恢复手册](docs/OPERATIONS.md)。确认两边账户、合约、报价币、数量单位、实际费用及保证金，使用满足场所最小订单要求的低仓位上限。

```bash
python scripts/build_lighter_wheel.py
python -m pip install -r requirements-live-lock.txt
cp .env.example .env
# 人工填写 .env；不要提交到 Git。
entropy-rh-arb --live --symbol SNDK --hedge lighter-rh --config config.yaml --env-file .env
```

上述最后一条命令会发送真实订单。提交订单前两腿意图先写入本地 SQLite 日志；未知结果会暂停新增套利并进入恢复。**不应删除状态文件来“解除暂停”。**

## 信号与交易边界

```text
premium_bps = (Entropy price / hedge price - 1) × 10000
SELL Entropy + BUY hedge：清算后门槛为 midline + upper
BUY Entropy + SELL hedge：反向门槛为 lower - midline
```

`midline_bps` 表示你从可靠采样估计的正常价差；`upper_bps`、`lower_bps` 表示两方向的入场幅度。库存梯度在加仓方向提高门槛。负向门槛可以出现于非零中轴，但错误中轴会积累亏损仓位。

`inventory.floor_frac=0` 表示从非零库存开始提高同向加仓门槛；`0.5` 表示超过仓位上限的 50% 才开始提高。前者在相同仓位下要求的价差更高，库存累积限制更早生效。`inventory.scale_bps=0` 才会关闭库存梯度。

规划按可执行深度、实际两腿名义金额和剩余 base 仓位空间向下取整。提交前检查价差门槛、配置费用、订单保护价及各腿仓位 cap。盘口变化、部分成交、资金费、报价币偏离和恢复成本仍可能让实际结果亏损，**不存在收益保证**。

HL 的盘口新鲜度取决于市场快照及服务端时间，pong 仅表示连接活着。Lighter diff 必须严格连续；完整重复区间忽略，缺口/部分重叠/无效 nonce 会清空盘口重新取快照，静默行情还受到周期快照约束。

## 执行、恢复和风险控制

- 两腿并发提交。HL 使用 IOC 限价；Lighter 使用带平均价保护的市场 IOC。平均价保护与逐笔价格上限并非同一个语义。
- 每个订单有持久化身份、提交标记及最终结果。未知结果通过订单身份读回，不能以零仓位或“查不到订单”直接推断未成交。
- 非对称成交和部分对冲会进入恢复；恢复过程停止新增套利，采用 reduce-only 补单与权威仓位核对。
- 新增敞口受盘口、余额、仓位可信度、可用保证金、订单预算、磁盘容量及持久化权益回撤约束。
- 资金费尚未作为独立信号或未来持仓收益模型；账户权益监控会反映实际费用和资金费变化。
- SIGINT/SIGTERM 请求停止新增订单，保留结算与恢复机会；超过关闭预算仍未确认的订单由持久化日志留给重启恢复。
- 状态日志带账户/场所/合约身份，并持独占锁。不要同时运行多个交易进程操作同一账户的同一市场，即使配置了不同日志目录。

详细参数及默认值见 [config.example.yaml](config.example.yaml)。交易审计 CSV 也按 UTC 日切分为 `trades-YYYY-MM-DD.csv`。原配置需要核对新风险门槛及运行路径；不满足门槛时程序可能暂停开仓。

| 配置 | 默认值 |
| --- | --- |
| `execution.premium_persist_sec` | 0.3 秒 |
| `inventory.floor_frac` / `scale_bps` | 0 / 10 bps |
| `execution.leg_slippage_bps` | 每腿 1 bps |
| `execution.submit_timeout_sec` / `settle_timeout_sec` | 5 / 10 秒 |
| `execution.recovery_poll_sec` / `recovery_timeout_sec` | 2 / 120 秒 |
| `execution.shutdown_timeout_sec` | 30 秒 |
| `risk.max_session_loss_usd` | 持久化权益峰值回撤 50 美元 |
| `runtime.state_db` | `state/{pair}/orders.sqlite3` |
| `runtime.health_file` | `logs/{pair}/health.json` |
| `logging.file` | `logs/{pair}/engine.log`，10 MiB × 6 个文件 |
| `recorder.csv` | `logs/{pair}/minutes.csv`，默认按 UTC 日切分 |

## 终端和监控

终端支持 Rich dashboard，`--cn` 显示中文。`--no-dashboard` 和非终端运行同时保留控制台与轮转文件日志；dashboard 显示实际方向门槛、市场快照年龄、恢复/停机原因及未决订单。

```bash
python tools/healthcheck.py 'logs/<pair>/health.json' --max-age 15 --pair-id '<pair>'
python tools/journal_status.py 'state/<pair>/orders.sqlite3'
```

健康快照约每 5 秒写入。只有及时、完整、盘口新鲜且符合开仓条件的 `READY`，以及及时有效的 `RECORD_ONLY` 返回 0。`PAUSED`、`RECOVERING`、`HALTED`、`STOPPED`、数据过期或文件损坏返回非 0；接入报警，**不要把健康检查失败直接连接到自动重启**。恢复订单的查询和重复提交不是等价操作。

[deploy](deploy) 提供占位 systemd 模板；本项目不会安装或启动它们。启动账户和服务的具体步骤见 [OPERATIONS.md](docs/OPERATIONS.md)。

## 依赖与验证范围

- [requirements-lock.txt](requirements-lock.txt) 是 CPython 3.13/macOS arm64 的基础与离线开发快照；[requirements-live-lock.txt](requirements-live-lock.txt) 是干净实盘依赖环境快照。目标 Linux 仍须验证对应原生 wheel 和兼容性。`requirements-live.txt` 提供有界主依赖，部署示例使用已测完整锁。
- 官方签名依赖固定为 Hyperliquid `0.24.0`、eth-account `0.13.7` 和 [Lighter 官方 commit](https://github.com/elliottech/lighter-python/tree/a38b6405f362fc14a562fe7a97df03f3ee756bc1)。Lighter 安装使用 `.wheelhouse/` 中经检查的本地 wheel，仅修正依赖元数据约束，不修改签名代码或 native 库。构建脚本核对官方 archive 和 payload；`.[live]` 只安装 HL 及公共依赖，完整 Lighter 安装必须执行上述构建与 `requirements-live.txt`。见 [Hyperliquid 官方 PyPI 元数据](https://pypi.org/project/hyperliquid-python-sdk/0.24.0/)。
- 在隔离环境完成过无凭证 import、接口签名检查及 `pip check`。这不能证明真实账户权限、交易所成交或链上读回行为。
- 2026-09-26 独立 `pip-audit`：加入 SOCKS 后基础/测试环境 23 包无已知告警；原始 SDK 环境的 `urllib3==2.0.7` 有 12 条告警记录（6 个不同 advisory，重复记录合并后计数）。上游固定版本的 `urllib3<2.1` 是原因；默认安装流程现使用经过专门兼容检查的 metadata-only wheel 和 `urllib3==2.8.0`，上游未修约束前不直接安装该 VCS 依赖。最终干净环境复审 52 个条目，无已知告警；其中本项目未发布 PyPI，被明确跳过，其代码验证由 review/测试承担。这不覆盖未知漏洞或真实成交，详见手册。
- 基础依赖包含 SOCKS asyncio 支持，兼容 WebSockets 读取系统/环境代理；程序不打印代理凭证，不替操作员更改代理配置。
- `.gitignore` 排除 `.env` 及运行数据；这不代表整个 Git 历史已经接受秘密扫描。泄漏的密钥应立即撤销并更换。

```bash
python -m pytest tests/ -q
ruff check entropy_robinhood_lighter_arbitrage tests main.py tools scripts
```

CI 执行离线测试、静态检查、wheel 构建及安装入口 smoke，另构建可重复 SDK wheel 并做无凭证 import；不读取交易凭证、不发送订单。实盘验收应由操作员在确认部署与账户后完成，并保留独立成交/仓位读回证据。

复核代码时请同时记录仓库 URL 与提交 SHA。原仓库与独立加固仓库的差异、已运行的 CI 和反馈核对见 [2026-09-27 版本复核记录](review/2026-09-27-peer-review-verification.md)。

## 已知策略风险

正常价差会漂移；USDG/USDC 基差会变化；两个场所资金费和合约口径可能不同；股票永续的市场时段与 oracle 机制也可能不同。对冲路径可能无法立即成交。仓位 cap 控制敞口规模，不保证本金或利润。

## License

[MIT](LICENSE)
