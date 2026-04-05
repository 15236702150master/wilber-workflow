# Legacy Workflow Notes

这份说明记录了 `/home/winner/project/codes/download` 中现有脚本与新项目阶段的对应关系，方便后续继续迁移或做网页封装。

## 现有脚本的核心职责

### `match_wilber3_events.py`

- 输入一份已有事件清单 CSV。
- 调 EarthScope/IRIS 事件服务。
- 以时间、距离、深度、震级综合评分，找到最接近的 Wilber 事件 ID。

新项目里的替代方式：

- 直接从事件服务按条件搜索，不再要求先准备“人工补查事件清单”。

### `submit_wilber3_requests.py`

- 加载事件与 Wilber 事件 ID。
- 调 `wilber3/services/stations_for_time/<event_time>` 获取台站树。
- 依据网络、通道、震中距、方位角、location 优先级筛站。
- 用 TauP 计算窗口。
- 生成请求体并提交 `submit_data_request`。

新项目里的替代方式：

- 仍保留同样的筛站和窗口逻辑。
- 统一写入 `03_requests/request_plan.csv` 和每事件请求体文件。

### `fetch_wilber_success_links.py`

- 登录 IMAP。
- 搜索 `[Success]` 邮件。
- 从正文提取 tar 下载链接。

新项目里的替代方式：

- 增加“等待直到全部请求标签都收齐或超时”的轮询能力。
- 单独输出 `pending_request_labels.csv` 便于排查。

### `extract_wilber_tar_packages.py`

- 安全解压 tar 包。
- 写 manifest。

新项目里的替代方式：

- 基本沿用原始逻辑。
- 输出目录换成统一的阶段目录。

### `normalize_wilber_sac_to_final_event.py`

- 读取解压目录。
- 匹配 `SACPZ.*`。
- 本机 `sac` 去响应，失败时回退 IRIS/FDSN 元数据。
- 写标准化 SAC。

新项目里的替代方式：

- 保留主逻辑。
- 新增 `skipped_extra_channels.csv`，显式记录被过滤掉的多余通道文件。

## 新项目阶段化后的好处

- 一个配置文件即可重复运行。
- 用户只需要给一个工作根目录。
- 每个阶段都留统计文件，便于断点检查。
- 更容易封装成 CLI、服务端任务，或后续网页端。

