# 南极越冬站多路径块设备层（Go 模拟实现）

用纯 Go 标准库实现的多路径块设备原型。阵列、双控制器、磁盘、四条服务器到控制器路径和故障都在进程内模拟；自带内嵌网页、JSON/CSV 导出和固定种子的 48 盘一致性自测。

## 快速开始

```bash
# 启动模拟阵列 + 多路径层 + 演示混合流量 + 网页
go run ./cmd/patharray -addr 127.0.0.1:8080

# 浏览器打开
open http://127.0.0.1:8080
```

常用参数：

```bash
go run ./cmd/patharray \
  -addr 0.0.0.0:8080 \
  -disks 48 \
  -seed 20261127 \
  -auto-faults=true
```

页面每秒刷新，可直接注入：

- 单条路径链路断路、黑洞超时、恢复；
- 整台交换机 `switch-A/B` 冻坏或恢复（一次断两条路，验证另外两条继续）；
- 磁盘主控制器切换；
- 磁盘繁忙/可纠正介质错误；
- 在线扩容；
- 空间使用率预警；
- 主机休眠恢复后的全路径重确认；
- 模拟阵列主动上报路径状态。

## 验收自测

```bash
# 默认：固定种子 20261127、48 块盘、7 秒随机故障/混合读写
go run ./cmd/selftest

# 快速跑法
go run ./cmd/selftest -duration=2s

# Go 测试（包含同一套 48 盘场景）
go test ./...

# 并发/锁检查
go test -race ./...
```

自测会：

1. 用固定种子创建 48 块盘，并从模拟后端复制初始内容到独立内存参照；
2. 每块盘一个顺序 worker，混合读/写 4 KiB～512 KiB 请求；
3. 随机注入单路断路、探测/IO 超时黑洞、整交换机故障、链路一次性报错；
4. 随机切换磁盘主控制器、注入磁盘繁忙和可纠正介质错误；
5. 在线扩容若干磁盘并直接读写新增容量；
6. 注入空间阈值预警；
7. 中途调用主机恢复钩子，重新探测全部路径；
8. 故障结束后恢复所有路径；
9. 全量读取每块盘，逐 4 KiB 块与内存参照比较；
10. 校验故障原因、事件、挂起统计、优先级、容量、空间预警和小时导出。

一次实际输出（机器不同，操作数和毫秒数会变化）：

```text
PASS=true disks=48 ops=7857 reads=4303 writes=3554 bytes=1233574912 verified4K=181596 mismatches=0 ioErrors=0
paths up/check/down=4/0/0 suspensions=2274 maxSuspend=49ms totalSuspendWait=3136ms maxRequest=256ms
events=380 hourlyRows=4 elapsed=7439ms
```

也可以输出报告：

```bash
go run ./cmd/selftest \
  -json=/tmp/selftest-report.json \
  -csv=/tmp/hourly-stats.csv
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 内嵌仪表盘 |
| GET | `/api/state` | 路径、磁盘、事件、切换、挂起、小时统计完整快照 |
| GET | `/api/hourly?format=csv` | 导出按小时、按路径统计的 CSV |
| GET | `/api/hourly?format=json` | 导出 JSON |
| POST | `/api/host/resume` | 模拟主机休眠/挂起恢复，重新确认所有路径 |
| POST | `/api/control/path` | `{"path_id":"...","fault":"link_down|timeout|up"}` |
| POST | `/api/control/path-notify` | 模拟阵列主动通知路径状态 |
| POST | `/api/control/switch` | `{"switch":"switch-A","fault":"link_down|up"}` |
| POST | `/api/control/owner` | 切换某盘主控制器 |
| POST | `/api/control/array-error` | 注入繁忙/介质错误 |
| POST | `/api/control/expand` | 在线扩容 |
| POST | `/api/control/space` | 设置空间使用率并触发阈值预警 |

## 需求对应关系

1. **大请求拆分与对齐**：`internal/mpio/io.go` 的 `PlanChunks` 按阵列报告的 `optimal_io_size`（默认 64 KiB）拆分；完整分块恰好为最佳单位且边界对齐，首/尾只可能因上层请求本身不对齐而变短。多个 chunk 由 goroutine 并行下发，任一 chunk 失败会取消同请求其余 chunk。
2. **主控制器/非主路径**：模拟阵列中走非主控制器会增加延迟；MPIO 对主控制器路径给优先级 100，非主路径 50，并结合在途数和 EWMA 延迟选路。收到 `owner_changed` 通知后更新优先级，并在短暂主控稳定窗口内挂起该盘新分块。
3. **状态变化原因**：路径状态保存在 `pathState`，事件进入环形历史并展示在网页。原因码包括 `probe_timeout`、`link_error`、`io_timeout`、`array_notification`、`probe_recovered`、`host_resume` 等。
4. **在线扩容**：`sim.Array.ExpandDisk` 扩容内存盘并发送 `capacity_changed`；Manager 更新 `Device.Capacity()`，已有设备句柄无需重建/重新挂载即可访问新 LBA。
5. **繁忙不等同断路**：`FaultBusy` 和可重试 `FaultMedia` 只增加 `busy_retries/media_errors`，不会把路径置为 down；按 `Retry-After` 或指数上限短暂等待后重试。有专门单测验证。
6. **小时统计与导出**：`HourBucket` 按本地小时和路径记录 IO、读/写字节、总字节、路径错误、繁忙重试、介质错误；支持 CSV/JSON 导出。
7. **空间预警上抛**：达到阵列阈值（默认 90%）时产生 `space_alert` 事件，更新 `Device.SpaceAlert()`，并通过 `Manager.SubscribeAlerts` 向上层订阅者推送；网页顶部也会显示预警。
8. **休眠恢复重确认**：`Manager.Resume` / `POST /api/host/resume` 将所有路径置为 `checking` 并并行探测，完成前新 IO 等待；另外内置 1 秒 tick 的墙钟跳变检测，检测到超过 5 秒跳变会自动按主机恢复处理。

## 代码结构

```text
cmd/patharray/          网页/演示启动入口
cmd/selftest/           固定种子验收自测 CLI
internal/sim/           模拟阵列、双控、48 盘、四路径和故障注入
internal/mpio/          多路径状态机、拆分、选路、重试、挂起、统计
internal/selftest/      48 盘一致性和故障场景
internal/web/           HTTP API 与 embed 内嵌仪表盘
```

## 拓扑

```text
                 switch-A                控制器 ctl0
 nic-0 ────────── p0 ───────────────────►  (部分盘主)
    └──────────── p1 ───────────────────► 控制器 ctl1

 nic-1 ────────── p2 ── switch-B ───────► 控制器 ctl0
    └──────────── p3 ── switch-B ───────► 控制器 ctl1
```

`switch-A` 故障会断 `p0/p1`，`switch-B` 故障会断 `p2/p3`；任何单台交换机损坏都至少保留分别通往两个控制器的两条路径。

## 说明

这是用户态模拟和算法原型，不直接注册 Linux 块设备或对接真实 SCSI/NVMe-oF 阵列；真实环境中应把 `sim.Array` 后端替换为 SCSI/ALUA、NVMe ANA 或阵列私有通知客户端，并将主机恢复钩子接到操作系统电源/网卡恢复事件。
