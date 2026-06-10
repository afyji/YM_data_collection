# YM_data_collection 协作约定

本仓库属于 `/mnt/mac_quant_system/` 量化系统工作区，默认遵循父级 `/mnt/mac_quant_system/AGENTS.md` 的项目协作规则。

## Git on SMB 固定规则

本仓库工作区位于 macOS SMB/CIFS 挂载目录：

- 工作区：`/mnt/mac_quant_system/YM_data_collection`
- Git 元数据：`/data/gitdirs/YM_data_collection.git`
- 工作区 `.git` 必须是文本指针文件：`gitdir: /data/gitdirs/YM_data_collection.git`

不要把真正的 `.git/` 目录放在 SMB 挂载目录中。macOS SMB/CIFS 对 Git object 的原子写入/link/rename 支持不完整，可能导致 `git add`/`git commit` 失败或 `.git` 损坏。

## 自动同步规则

每次完成稳定功能、文档更新、bug fix、阶段性 checkpoint 后，默认执行：

```bash
git status
git add <safe files>
git commit -m "<agent-written message>"
git push
```

推送前必须检查没有 staged 以下内容：

- `.env`、密钥、证书
- 数据 dump、parquet/csv/zip/tar 等大文件
- SQLite/DB 文件
- `__pycache__`、pytest cache、egg-info 等生成物

除非用户明确说“先不要推送”，否则 agent 认为需要同步时应自动提交并推送到 GitHub。
