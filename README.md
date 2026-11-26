# 同步观影房间的分支投票与时间承诺

节目图由片段、分岔和可重新定位位置构成。逻辑到达时间 `at` 和 playhead 媒体位置分别记录。

本仓库在原有 JSONL 读取器之外，交付了一个可直接被房间服务、节目事件系统和仿真播放器调用的持久化中间件。它只使用 Python 3.11 标准库。

## 交付内容

- `branch_room/service.py`：核心服务 `RoomEventService`（别名 `RoomService`、`RoomEventMiddleware`、`Middleware`）。
- `branch_room/clients.py`：`RoomClient`、`ProgramClient`、`PlayerClient` 三个角色封装。
- `branch_room/scenario.py`：把演练 JSONL 按到达顺序送入服务。
- `branch_room/server.py`：标准库 HTTP/JSON 服务。
- `room_service.py` / `service.py`：便于 `from room_service import RoomEventService` 的根级兼容入口。
- SQLite 数据库：保存状态快照、原始命令、处理回执和公开领域事件，重启后可继续服务。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 `at`、消息名称 `kind` 与对象 `record`。`at` 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 `record` 中。

- **branch**　节点、来源 segment、options 与 fallback。options 对应目标 segment，order 是公布的并列优先顺序。cut_playhead 是原片段的最迟切换位置，缺省只从分岔处起切换，重定位在新片段可播放起点。
- **ready**　节目方声明目标 segment 是否具备播放条件。显式 `available:false` 才触发不可用/备用规则；未报告 false 不被当作已公布不可用。
- **member**　房间 room 中的 account 加入或退出。
- **start**　主持人开启 room 的 node，round 唯一，close_at 是逻辑收票截止秒点。
- **ballot**　账号选择 option，device 为发送端，票按账户。
- **close**　主持人请求在 close_at 后结束本轮收票；`at == close_at` 视为已到截止点，同秒内严格按流顺序处理。
- **position**　device 指定 account 已播放 segment 与 playhead；只更新该设备遥测，不覆写账户级已确认进度。
- **switched**　device 确认 round 的目标 segment 已应用；这是更新账户恢复点的唯一来源。
- **resume**　设备重连，携带 segment 与 playhead；返回账户检查点和该设备的追赶/重定位指令。
- **inspect**　查询 room、round 或 node。

## 快速使用

### Python API

```python
from room_service import RoomEventService

service = RoomEventService("room.sqlite3")  # ":memory:" 用于临时测试

service.handle({
    "at": 2,
    "kind": "start",
    "record": {"room": "rm", "node": "c1", "round": "q1", "close_at": 8},
}, command_id="host-start-q1")

round_view = service.inspect_round("q1")
room_view = service.inspect_room("rm")
```

也可以使用角色客户端：

```python
from branch_room import RoomClient, PlayerClient, ProgramClient

program = ProgramClient(service)
room = RoomClient(service, "rm")
player = PlayerClient(service, "rm", "a", "phone")

program.announce_branch(node="c1", segment="intro", options={"r": "r"},
                        fallback="r", order=["r"])
room.set_member("a", True, at=1)
room.start_round(node="c1", round_id="q1", close_at=8, at=2)
player.ballot("q1", "r", at=3)
room.close_round("q1", at=8)
```

`handle()` 返回统一信封：

```json
{
  "ok": true,
  "accepted": true,
  "command_id": "cmd-...",
  "receipt": {"receipt_id": "cmd-...", "immutable": false},
  "result": {},
  "events": []
}
```

### 运行演练文件

```bash
python3 -m branch_room.scenario examples/fallback-and-late-join.jsonl --db /tmp/room.sqlite3
python3 main.py scenario examples/device-outside-the-branch.jsonl --db /tmp/room.sqlite3
```

不加 `--db` 时使用内存库；指定 `.sqlite3` 文件后，进程退出再启动仍可查询和继续接收命令。演练运行器使用“文件名:行号”作为幂等 command id，因此同一文件向同一数据库重放时，所有命令均返回原始回执（响应中带 `idempotent_replay: true`）。

### HTTP 服务

```bash
python3 -m branch_room.server --db room.sqlite3 --host 127.0.0.1 --port 8000
# 或
python3 main.py serve --db room.sqlite3
# 或
python3 server.py --port 8000
```

通用事件入口：

```bash
curl -sS http://127.0.0.1:8000/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"at":3,"kind":"ballot","record":{"round":"q1","account":"a","device":"phone","option":"river"}}'
```

便捷入口为 `POST /v1/{branch,ready,member,start,ballot,close,position,switched,resume}`。

查询：

- `GET /v1/rooms/<room>`
- `GET /v1/rounds/<round>`
- `GET /v1/accounts/<account>/progress?room=<room>`
- `GET /health`

## 已落实的业务规则

### 资格与投票

- `start` 时对当时 `active:true` 的房间正式成员做不可变快照：`eligible_accounts` 与 `eligible_count`。
- 之后加入的账户可以观看、追赶和确认切换，但本轮投票以 `not_eligible` 拒绝并记入 `rejected_ballots`；下一轮 `start` 才会进入资格快照。
- 投票按 `(round, account)` 计一票；同一账户的多设备/改选只替换当前票，`ballot_history` 保留每次选择、设备、时间和版本。
- 截止后、显式 `close` 后、无效轮次、无效选项或设备未取得可用位置的票均不计入，并保留拒绝原因。
- 平票按 branch 公布的 `order` 决定，而不是按字典序猜测。

### 不可用与备用

关闭收票时先固定公开的计票承诺 `vote_collection_committed`：

- `tally.counts`：每个公布选项的票数；
- `winner_option` / `original_winner_option`：原始中标选项；
- 每张原始选票和完整改选历史均保留。

若中标选项目标 segment 已有公开的 `available:false` 报告，则：

- `diverted=true`；
- `diversion_reason=winner_segment_unavailable`；
- `diversion` 保存不可用 segment、报告时间和备用选项；
- `effective_option` 改为节目方公布的 `fallback`；
- 发出第二个公开承诺 `switch_committed`，其中含不可变 `decision_id`、目标 segment 和重定位点。

若备用目标也被明确报告为不可用，轮次进入 `switch_status=waiting_assets`，不生成切换决定，也不会编造选项。之后收到该 segment 的 `ready available:true` 时，才补做并公开 `switch_committed`。

### 追赶、重定位与下一轮设备资格

服务用节目图做 segment 可达性判断，并结合上一轮 `cut_playhead`：

- 未发现播放位置异常的设备默认允许投票，避免没有 position 记录的正常首轮设备被误拒。
- 设备仍在上一轮 source segment 且 playhead 已超过 `cut_playhead`、又未确认该轮切换时，下一轮访问状态为 `missed_prior_switch_point`。
- 当前可见 segment 无法沿节目图到达下一轮 source 时，状态为 `segment_not_connected`；`start` 生成的邀请中 `invited:false`。
- 此类设备在下一轮取得可用位置前送来的票以 `device_without_usable_position` 拒绝。
- `resume` 会领取恢复/重定位点：
  - 未到切换点：`continue_then_switch`，到公布的合适位置再应用已承诺决定；
  - 已越过或接不上：`relocate` 到已决定目标 segment 的可播放起点（默认 playhead 0）；
  - 素材不可用：`wait_for_assets`；
  - 已在目标点：`confirm_switch` 或 `up_to_date`。
- `resume` 给出的可用点会写入 `admitted_rounds`，之后该设备的下一轮票可计入。

房间查询提供：

- `players[].catchup_status` / `catchup`
- `players_needing_catchup`
- `behind_players`
- `pending_switch_players`
- `players_unknown_position`

### 账户恢复与防覆写

- 只有 segment 与不可变切换决定一致的有效 `switched` 才更新 `rooms.<room>.accounts.<account>.canonical`。
- 更高轮次的确认会推进账户恢复点；旧轮次确认只记录到该设备的 `confirmed_rounds`，返回 `account_progress_updated:false`，不会把账户进度向后覆盖。
- `position` 和 `resume` 永远不写 canonical；它们只更新对应设备的遥测，并在响应中返回账户检查点和该设备可另取的重定位指令。
- 主屏、新设备或重连设备调用 `resume` / `account_progress()` 即可按账户恢复。

### 节目预告版本

- 同一 node 的每次 `branch` 预告都会保存版本。
- `start` 把当时的 options/fallback/order/cut 位置快照进 round。
- 之后节目预告更新同一 node，只影响新打开的轮次；已开票轮次仍使用自己的 `branch_snapshot`。
- 已作出的 `switch_decision.immutable=true`，后续回执重送或状态查询不会重算覆盖。

### 重启与幂等

`RoomEventService("room.sqlite3")` 使用 SQLite 保存：

- `state`：当前完整状态；
- `commands`：command id、消息哈希、原始消息和首次响应；
- `domain_events`：轮次开启、计票承诺、改道、切换承诺、确认等公开事件。

推荐调用方生成稳定 command id，或在 `record.request_id` / `record.receipt_id` / `record.receipt.receipt_id` 中带回回执 id。相同 id 且相同核心消息重送时返回首次响应并标记 `idempotent_replay`；相同 id 但业务内容不同会抛出 `IdempotencyConflict`。

即使没有客户端幂等 id，`close` 重复提交也只会返回已有结果，投票关闭后的新选票会被拒绝，切换决定不会被 `switched` 回执改写。

## 查询示例

主持人查房：

```python
view = service.inspect_room("rm")
view["current_eligible_count"]       # 当前/最新轮次合资格人数
view["current_result"]["diversion_reason"]
view["players_needing_catchup"]
view["behind_players"]
```

查单轮凭据：

```python
round_view = service.inspect_round("q1")
round_view["eligible_accounts"]
round_view["ballot_history"]
round_view["rejected_ballots"]
round_view["tally"]
round_view["switch_decision"]
round_view["public_events"]
```

## 测试

```bash
python3 scenario_reader.py examples
python3 -m unittest discover -v
```

测试覆盖两个示例、账户多设备投票、迟到成员、平票顺序、截止同秒顺序、不可用/备用/等待素材、节目预告快照、图不可达设备、resume 后恢复投票资格、落后设备防覆写、重启和回执重放。
