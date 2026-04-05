# Release Checklist

在把 WilberFlow 仓库公开发布到 GitHub 前，建议完整过一遍下面这份清单。

## 1. 仓库身份

- [ ] 仓库名称、简介、标签已经确定
- [ ] README 顶部 badge、仓库克隆地址、许可证说明已经同步
- [ ] `LICENSE` 已加入仓库
- [ ] `CONTRIBUTING.md` 已加入仓库

## 2. 敏感信息与隐私

- [ ] `.env.local`、`*.local.toml`、本地 profile、输出目录没有被跟踪
- [ ] README、示例配置、截图、日志示例里没有真实邮箱、授权码、Token、Cookie
- [ ] 没有把 Windows 私有路径、个人目录、临时文件写进公开文档
- [ ] 运行一次：

```bash
bash scripts/check-sensitive-files.sh
```

## 3. README 与截图

- [ ] README 的安装、启动、停止命令都能直接执行
- [ ] README 中的图片使用相对路径，GitHub 页面能正常显示
- [ ] README 已明确写出运行边界：服务运行环境为 `WSL/Linux`
- [ ] README 已写清楚 QQ 邮箱、IMAP 和授权码要求
- [ ] README 已给出首次测试建议：先小批量、先 `metadata only`
- [ ] README 已包含真实案例或输出目录示例，便于新用户理解 `07_final`

## 4. 示例文件

- [ ] `config.example.toml` 不包含真实账号信息
- [ ] `examples/demo.metadata-only.toml` 可作为首次测试参考
- [ ] `examples/demo-output-tree.txt` 与当前输出结构一致
- [ ] `.env.local.example` 仍然保留为占位符示例

## 5. 运行脚本

- [ ] `bash scripts/setup.sh` 可正常创建 `.venv` 并安装依赖
- [ ] `bash scripts/start-studio.sh` 可正常启动本地服务
- [ ] `bash scripts/stop-studio.sh` 可正常停止本地服务
- [ ] `bash scripts/check-sensitive-files.sh` 能正常执行

## 6. 功能冒烟验证

- [ ] 事件搜索正常
- [ ] 台站筛选正常
- [ ] `metadata only` 正常
- [ ] 新建批次正常
- [ ] 旧批次补跑正常
- [ ] 缓存 / 复用统计正常
- [ ] 邮件轮询与下载正常
- [ ] 去仪器响应回退逻辑正常
- [ ] 最终 `07_final` 目录结构正常

## 7. 发布前清理

- [ ] 删除无关测试输出、临时截图和临时日志
- [ ] `output/` 下的本地运行结果不参与发布
- [ ] `.wilberflow-studio/` 下的本地 profile 与缓存不参与发布
- [ ] `.gitignore` 已覆盖虚拟环境、输出目录、本地配置和缓存目录
- [ ] 至少在一个新的本地环境里按 README 重新走通一次安装与启动

## 8. 最后确认

- [ ] 仓库首页一句话就能让新用户理解工具用途
- [ ] README 中没有“作者视角”的临时备注或内部口吻
- [ ] 发布版本号、tag、release note 已准备好
