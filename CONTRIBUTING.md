# Contributing

感谢你愿意改进 WilberFlow。

这个项目的目标不是做一个只在作者机器上能跑的脚本，而是做成一个新用户也能按 README 直接上手的工具。所以对这个仓库来说，代码、文档、截图、默认值和发布前清理同样重要。

## 适合提交的内容

- Bug 修复
- README 与截图改进
- 配置台交互优化
- 断点续跑、缓存、统计文件相关改进
- 去仪器响应与最终目录整理相关修复
- 新的测试、示例配置、案例说明

## 本地开发

先准备基础环境：

```bash
git clone https://github.com/15236702150master/wilber-workflow.git
cd wilber-workflow
bash scripts/setup.sh
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

启动本地配置台：

```bash
bash scripts/start-studio.sh
```

停止本地配置台：

```bash
bash scripts/stop-studio.sh
```

## 提交前建议

如果你改了后端逻辑，至少做一次小批量冒烟验证：

```bash
source .venv/bin/activate
python -m pytest
```

如果你改了前端配置台：

- 检查主要配置区是否还能正常填写
- 检查状态栏、断点续跑、导入导出是否仍可用
- 更新 README 中对应截图

发布或开 PR 前，建议跑一次敏感信息扫描：

```bash
bash scripts/check-sensitive-files.sh
```

## 不要提交这些内容

- `.env.local`
- 真实邮箱、授权码、Token、Cookie
- `*.local.toml`
- `output/` 下的本地运行结果
- `.wilberflow-studio/` 下的本地 profile 与缓存
- 仅用于临时调试的截图、日志、批次目录

## 文档约定

如果你修改了以下内容，请同步更新 README：

- 配置区字段
- 默认行为
- 输出目录结构
- 断点续跑逻辑
- 安装或启动方式

如果界面外观变化明显，建议同时更新：

- `docs/readme-assets/studio-top.png`
- `docs/readme-assets/studio-events.png`
- `docs/readme-assets/studio-stations.png`
- `docs/readme-assets/studio-request-batch.png`

## Pull Request 建议

- 标题直接说明改了什么
- 描述中写清楚改动动机、影响范围、手动验证方式
- 如果改了 UI，附一张截图
- 如果改了输出目录或配置字段，说明 README 是否已同步

## 运行边界

当前仓库默认支持的是：

- 服务运行在 `WSL/Linux`
- 浏览器可在 Windows 或 Linux 中打开本地页面
- 输出目录可写到 Linux 路径，也可写到 `/mnt/d/...` 这类 Windows 挂载路径

纯 Windows Python 直跑服务目前不在正式支持范围内。
