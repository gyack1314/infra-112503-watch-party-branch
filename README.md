# 同步观影房间的分支投票与时间承诺

节目图由片段、分岔和可重新定位位置构成。逻辑到达时间和 playhead 媒体位置分别记录。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **branch**　节点、来源 segment、options 与 fallback。options 对应目标 segment，order 是公布的并列优先顺序。 cut_playhead 是原片段的最迟切换位置，缺省只从分岔处起切换，重定位在新片段可播放起点。
- **ready**　节目方声明目标 segment 是否具备播放条件。
- **member**　房间 room 中的 account 加入或退出。
- **start**　主持人开启 room 的 node，round 唯一，close_at 是逻辑收票截止秒点。
- **ballot**　账号选择 option，device 为发送端，票按账户。
- **close**　主持人请求在 close_at 后结束本轮收票。
- **position**　device 指定 account 已播放 segment 与 playhead。
- **switched**　device 确认 round 的目标 segment 已应用。
- **resume**　设备重连，携带 segment 与 playhead。
- **inspect**　查询 room 或 round。

## 示例

- fallback-and-late-join.jsonl
- device-outside-the-branch.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
