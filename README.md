# restart-permit-api

存储集群值班员用的**分批重启许可 API**。FastAPI + SQLite，单容器即可运行。

核心规则：

- 服务启动时读取**固定的分片投票副本清单**（`manifest.json`）。
- 节点探活报告携带**采样时间、健康状态、启动标识**；每个节点以**采样时间最新**的一份为准。
- 探活上报资格：**当前清单中的节点始终可上报**；**清单外的节点只有在仍被至少一条未结束许可占用时**才可上报（用于节点被移出清单后完成恢复探活并结束旧许可）。批次中只要有一个无资格节点，**整批拒收**（422），同批合规节点也不会落库。
- 值班员提交**申请号 + 拟重启的一批节点**，满足以下条件才批准整批：
  1. 本批每个节点最新探活均健康；
  2. 本批节点未被任何**未结束许可**占用；
  3. 扣除 *本批节点 + 未结束许可占用节点 + 健康未知/异常节点* 后，本批涉及的**每个分片**剩余可用投票副本均**过半**；否则拒绝并列出受影响分片。
- **并发申请**在写事务（`BEGIN IMMEDIATE`）中串行判定，守住多数票。
- **同号同批重发**返回原许可（HTTP 200）；同号不同批返回 409。
- **新申请只接受当前清单中的节点**；同号重发/冲突判定先于清单校验，因此旧许可的
  节点即使已被移出清单，同号同批重发仍返回原许可、同号换批仍报 409。
- 许可与探活记录落盘 SQLite，**服务重启后保留**。
- **结束许可**要求本批每个节点的最新探活：在获批后采集、健康、启动标识与获批时不同；
  对已移出清单的节点同样按此规则判定。
- 探活资格判定与探活写入在**同一写事务**内完成：若结束许可先于探活落库生效，
  清单外节点按**结束后的状态**判定为无资格并整批拒收。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

- SQLite 数据在命名卷 `permit-data`（容器内 `/data/permits.db`），重启不丢。
- 分片清单 `./manifest.json` 只读挂载进容器；修改后需 `docker compose restart` 生效。

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DB_PATH=./data/permits.db MANIFEST_PATH=./manifest.json \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pytest tests/ -q
```

## 分片清单格式

```json
{
  "shards": {
    "shard-a": ["n1", "n2", "n3"],
    "shard-b": ["n2", "n3", "n4"],
    "shard-c": ["n3", "n4", "n5"]
  }
}
```

每个分片列出全部投票副本节点；过半线 = `floor(票数/2)+1`（3 票需 2 可用）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 服务健康检查 |
| GET | `/manifest` | 查看分片清单与各分片过半线 |
| POST | `/liveness` | 批量上报探活（201） |
| GET | `/liveness` | 全部节点最新探活 |
| GET | `/liveness/{node}` | 单节点最新探活 |
| POST | `/permits` | 申请许可（201 新批 / 200 幂等重发 / 409 拒绝） |
| GET | `/permits?status=open\|finished` | 许可列表 |
| GET | `/permits/{request_id}` | 单个许可 |
| POST | `/permits/{request_id}/finish` | 结束许可（200 / 409 条件不满足） |

## 调用示例

### 1. 上报探活

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports": [
      {"node_id":"n1","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n2","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n3","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n4","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n5","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"}
    ]}'
# => {"accepted":5}
```

### 2. 申请许可

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","nodes":["n1"],"status":"open",
#  "approved_at":"...","finished_at":null,
#  "boot_ids_at_approval":{"n1":"boot-1"}}
```

同号同批重发，返回原许可（200，`approved_at` 不变）；同号不同批返回 409。

### 3. 多数票不足被拒（列出受影响分片）

`req-001` 已占用 n1 时再申请 n2（同属 shard-a，3 票最多扣 1 票）：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n2"]}'
# 409 =>
# {"detail":{"ok":false,"error":"permit_rejected",
#   "affected_shards":[{"shard":"shard-a","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n3"],
#     "excluded":{"n1":"occupied_by:req-001","n2":"in_batch"}}]}}
```

其它拒绝类别：`health_unknown_nodes`（健康未知）、`unhealthy_nodes`（异常）、
`occupied_nodes`（被未结束许可占用）。新申请中的清单外节点一律 422
（`{"detail":{"error":"unknown_nodes","unknown_nodes":[...]}}`）。

探活上报中若有**无资格节点**（不在当前清单且未被未结束许可占用），整批拒收：

```json
{"detail":{"error":"unknown_nodes","unknown_nodes":["n1"]}}
```

### 4. 节点重启后上报新探活，结束许可

```bash
# 节点重启完成：采样时间晚于获批时间、健康、启动标识已变
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-19T10:20:00Z",
                "healthy":true,"boot_id":"boot-2"}]}'

curl -X POST http://localhost:8000/permits/req-001/finish
# 200 => {"request_id":"req-001",...,"status":"finished","finished_at":"..."}
```

结束条件不满足时 409 并给出逐节点原因，例如：

```json
{"detail":{"error":"finish_conditions_not_met",
           "nodes":{"n1":"boot_id_unchanged"}}}
```

可能原因：`no_liveness_report`、`not_sampled_after_approval`、`unhealthy`、`boot_id_unchanged`。
许可结束后节点被释放，可被后续申请占用。

### 5. 节点已移出清单：完成恢复探活并结束旧许可

值班员把节点移出投票清单后，沿用**同一个 SQLite 库**重启 API。旧许可仍未结束时，
被占用的清单外节点仍可上报恢复探活，结束条件与在清单内时完全一致。

用 Docker Compose 演示（数据在命名卷 `permit-data`，重启不丢）：

```bash
# 1) 按上面第 1~2 节让 n1 获批（req-001 open），然后改清单：从 shard-a 删掉 n1
#    编辑 ./manifest.json 为：
#    {"shards":{"shard-a":["n2","n3"],"shard-b":["n2","n3","n4"],"shard-c":["n3","n4","n5"]}}
#    清单是只读挂载，重启服务即生效，SQLite 数据卷保持不变
docker compose restart permit-api
curl -s http://localhost:8000/manifest   # shard-a 已无 n1
```

```bash
# 2) 旧许可仍在（服务重启后保留）
curl -s http://localhost:8000/permits/req-001
# {"request_id":"req-001","nodes":["n1"],"status":"open",...}

# 3) n1 重启完成：在获批后采集、健康、启动标识已变
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s -X POST http://localhost:8000/liveness -H 'Content-Type: application/json' \
  -d "{\"reports\":[{\"node_id\":\"n1\",\"sampled_at\":\"$NOW\",
                    \"healthy\":true,\"boot_id\":\"boot-2\"}]}"
# 201 => {"accepted":1}    ← 修复前这里会被 422 拒收

# 4) 结束旧许可仍按原有条件判定
curl -s -X POST http://localhost:8000/permits/req-001/finish
# 200 => {...,"status":"finished","finished_at":"..."}

# 5) 许可结束后，清单外的 n1 不再被占用，探活被整批拒收
curl -s -X POST http://localhost:8000/liveness -H 'Content-Type: application/json' \
  -d "{\"reports\":[{\"node_id\":\"n2\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-2\"},
                    {\"node_id\":\"n1\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-3\"}]}"
# 422 => {"detail":{"error":"unknown_nodes","unknown_nodes":["n1"]}}
#        同批的 n2 也没有落库（整批拒收）

# 6) 幂等与清单校验的顺序
curl -s -X POST http://localhost:8000/permits -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'   # 200 原许可（同号同批）
curl -s -X POST http://localhost:8000/permits -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n4"]}'   # 409 request_id_conflict（同号换批）
curl -s -X POST http://localhost:8000/permits -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n1"]}'   # 422 unknown_nodes（新申请只收清单内节点）
```

本地不用 Docker 时，用同一 `DB_PATH`、换 `MANIFEST_PATH` 重启 uvicorn 即可复现：
`DB_PATH=./data/permits.db MANIFEST_PATH=/tmp/manifest-b.json uvicorn app.main:app --port 8000`。

若结束许可先于某份探活落库生效（即探活请求开始前许可已被结束），该探活在写事务内
读到的是结束后的占用状态，清单外节点即判为无资格，整批拒收——资格始终按落库时的
最新状态判定，不存在"已结束但仍可上报"的窗口。

## 设计说明

- **幂等**：`request_id` 是许可表主键。节点集合规范化（去重排序）后比较，
  同号同批重放直接返回原许可；同号不同批 409。被拒绝的申请不落库，整改后可用同号重试。
- **并发**：所有写路径走单连接 + 可重入锁 + `BEGIN IMMEDIATE` 事务，
  "检查多数票 → 写入许可"是原子的；并发申请串行判定，不会超批。
- **持久化**：许可与全部探活报告存 SQLite（WAL 模式），最新探活按
  `max(sampled_at)` 实时计算，服务重启后状态完整恢复。
- **探活资格**：`POST /liveness` 在 `BEGIN IMMEDIATE` 写事务内先读未结束许可、
  判定清单外节点是否仍被占用，再写入报告。资格判定与写入不可分割，
  结束许可与探活上报并发时结果确定。
- **结束判定**：批准时快照各节点 `boot_id`；结束时要求最新报告
  `sampled_at > approved_at` 且健康且 `boot_id` 不同。

## 已知限制 / 未完成项

- 单 worker 运行（SQLite 单写者）；如需多副本需换外部数据库。
- 未做认证鉴权，假定部署在受信内网；如需可前置网关或加 token。
- 探活无"过期"概念：只要最新一份健康即视为健康，不判断采样时间新旧。
- 结束许可比较的是节点上报的采样时钟与服务端获批时钟，依赖各节点时钟基本同步（NTP）。
- 许可无过期/自动回收机制；值班员需显式 finish。
