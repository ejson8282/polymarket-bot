# 已冻结的 Latitude Dashboard 历史副本

> **禁止开发、禁止部署。**

本目录只为历史追溯和旧测试兼容而暂时保留。Latitude 统一 Dashboard 的唯一源码、部署脚本、
systemd unit 和回滚文档均位于
[`ejson8282/latitude-alpha`](https://github.com/ejson8282/latitude-alpha)。

不得：

- 修改这里的 `console_app.py` 或 `console.html`；
- 执行本目录的旧部署脚本；
- 把生产 worktree 中的页面差异回传到本仓库；
- 从本仓库重启或覆盖 `latitude-console.service`。

Dashboard 需要新的 Polymarket 数据时，应在本仓库提供稳定状态/API 契约，然后在
`latitude-alpha` 的 `deploy/latitude-console/` 中实现和发布页面。

本目录后续只有在确认没有测试、恢复流程或历史引用后才能整体归档；在此之前保持只读。
