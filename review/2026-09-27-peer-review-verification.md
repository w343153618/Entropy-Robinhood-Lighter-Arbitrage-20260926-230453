# 版本与外部复核反馈核对

核对日期：2026-09-27。外部反馈未附 Git remote 或提交 SHA，因此不能确定对方的实际检出版本；其 48 项测试、Python >=3.10、包内无 main.py 等特征与下面的原始版本一致。

## 固定版本与证据

| 项目 | 原始版本 | 首次发布的加固版本 |
| --- | --- | --- |
| 仓库 | [Entropy-Robinhood-Lighter-Arbitrage](https://github.com/w343153618/Entropy-Robinhood-Lighter-Arbitrage) | [Entropy-Robinhood-Lighter-Arbitrage-20260926-230453](https://github.com/w343153618/Entropy-Robinhood-Lighter-Arbitrage-20260926-230453) |
| 提交 SHA | `b0f02e9e116624e603273c9a52db5cdef559e571` | `e4efc3f44e61ae82fae210cd1302c81a145c6c43` |
| Python 声明 | >=3.10 | >=3.13 |
| 测试结果 | 原始审核时 48 passed | 本地与 GitHub CI 均 193 passed |
| 包内 main.py | 不存在 | 存在 |
| 安装后入口验收 | 原始源码检查已发现入口模块缺失 | CI 构建 wheel，在独立 venv 安装，切换到 /tmp 执行 --help 成功 |

原始审核报告 [E03](2026-09-26-code-review.md#e03--p2--声明的安装命令入口不存在) 已记录入口模块缺失及修复建议。加固代码首次发布的 [CI 运行 36251137744](https://github.com/w343153618/Entropy-Robinhood-Lighter-Arbitrage-20260926-230453/actions/runs/36251137744) 的 headSha 为上表 `e4efc3f...`，结论 success；2026-09-27 通过 GitHub API 和日志再次读取确认。其日志包含 `193 passed in 6.53s`、Ruff 通过、配置校验通过、安装后的 CLI usage 以及 SDK imports passed。

## 五项反馈逐条核对

以下“首次发布”均指 `e4efc3f...`，避免把本次补充追溯成以前已完成。

| 反馈 | 核对结果 |
| --- | --- |
| 安装后的 entropy-rh-arb 入口损坏 | 原始版本真实缺陷，原始审核 E03 已发现。首次发布已有包内 main.py、根入口薄包装、安装 wheel 后在仓库外运行 CLI 的 CI 检查。 |
| 缺少包 __init__.py | 首次发布确实仍使用隐式命名空间包。Python 支持这种导入，缺少该文件本身不等于入口损坏；原始缺陷的直接原因是包内 main.py 不存在。本次新增仅含说明文字的 __init__.py，将本项目明确为普通包。 |
| persistence / inventory 默认值不一致 | 原始配置存在不一致。首次发布的 example 与 config 默认 persistence 均为 0.3 秒，floor_frac 均为 0。floor=0 提前提高同向加仓门槛，不能解释为关闭保护；scale_bps=0 才关闭库存梯度。本次补充 README 的库存默认值及含义。 |
| entropy.dex 空字符串得到非法 asset ID | 原始版本缺校验。首次发布在 load_config 中拒绝空值、空白字符、冒号；原生空 dex 不会进入适配器。asset ID 的两个公式代数等价，不改正确公式。 |
| recorder high/low 冗余 | 原始 max([prem[0]] + prem) / min(...) 确实冗余。首次发布已使用 max(prem) / min(prem)。 |

Python 的 [namespace package 官方说明](https://docs.python.org/3/reference/import.html#namespace-packages) 明确支持没有 __init__.py 的命名空间包。增加包标记是包结构明确化，不应被描述为修复尚未存在的 CLI 故障。

库存示例：scale=10 bps，向同方向增加仓位，当前仓位为上限的 25%。floor=0 时附加门槛为 2.5 bps，floor=0.5 时为 0 bps。当前仓位为上限的 75% 时，两者分别为 7.5 和 5 bps。这里只比较库存附加门槛，不推导交易收益。

## 其余反馈

- Hyperliquid orderStatus 的 oid 字段允许 u64 订单号或 16 字节十六进制 client order ID；此用法有[官方接口定义](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#query-order-status-by-oid-or-cloid)支持。协议核对不替代真实账户集成测试。
- 首次发布已分离 HL 心跳与市场快照新鲜度；Lighter 增加严格 nonce 连续性及周期快照。相关变化及边界见加固报告。
- session_pnl、基差、资金费和实际账户费率仍需按加固报告中的边界理解。账户权益变化也会受出入金、人工交易等影响，不能无条件视作本策略净利润。
- `.cross-arb-data/` 和 `**/api_token` 忽略项是旧命名残留，保留排除不会影响执行；本次没有把任何 .commandcode 或凭证目录加入发布内容。

## 交付说明修正

加固报告在上传后仍写“未推送 GitHub”和 Linux CI 仅已配置，属于未更新的交付状态说明。本次按已读取的远端提交与成功 CI 修正；原始审核报告保持为历史基线记录。

本次补充后的本地复验：

- Python 3.13.15：`pytest tests/ -q` 为 **193 passed in 3.93s**；Ruff 与 diff 空白检查通过。
- 用 setuptools 83.0.0 从发布目录重新构建 wheel，将 wheel 安装到隔离运行环境，在 `/private/tmp` 执行 `entropy-rh-arb --help` 和 `--check-config`，均返回 0。
- 在仓库外检查模块来源：包的 origin 为隔离环境 `site-packages/.../__init__.py`，CLI 来源为同一安装目录的 `main.py`；无须额外设置 PYTHONPATH。
- 使用已安装 wheel 读取临时无凭证配置：空 `entropy.dex` 被明确拒绝。本次所有命令均未发送交易订单。

复核者应在自己的代码目录先执行：

```bash
git remote get-url origin
git rev-parse HEAD
git status --short
```

私有仓库需使用已获得访问权限的 GitHub 账号，浏览器也必须登录。原仓库不会自动同步这个独立仓库的修改；读取本地旧目录时要注意未提交文件与 HEAD 的区别。

已完成的验证是离线回归、打包安装、无凭证 SDK 检查及此前的公开行情读取。真实下单、真实账户恢复、报警接入、生产部署和跨日持续运行仍未完成验收；不能宣称已证明长期无人值守实盘安全。
