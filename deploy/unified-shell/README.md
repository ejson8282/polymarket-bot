# 统一导航壳(nginx + 总览聚合器)· 部署说明(施工包05·5C)

**本目录只是部署配置与文档,不改任何系统代码;所有部署命令由运维手动执行。**

组成:
- `nginx.conf` —— Tailscale IP:80 统一入口,路径路由到各 dashboard
- `overview_app.py` —— FastAPI 总览聚合器(只读,127.0.0.1:8600)
- `STATUS_CONVENTION.md` —— status.json 字段约定
- `examples/` —— 聚合器示例配置与两个假 status 文件(本地验收用)

## 1. nginx

```bash
# 1) 替换占位符(Tailscale IP 与核算台 upstream 按实际填)
sudo cp deploy/unified-shell/nginx.conf /etc/nginx/unified-shell.conf
sudo sed -i 's/TAILSCALE_IP/100.122.255.98/' /etc/nginx/unified-shell.conf
sudo sed -i 's/ACCOUNT_OPS_UPSTREAM/127.0.0.1:8700/' /etc/nginx/unified-shell.conf

# 2) 校验并装载(独立配置方式;也可把 server{} 块并入现有 nginx.conf)
sudo nginx -t -c /etc/nginx/unified-shell.conf
sudo nginx -c /etc/nginx/unified-shell.conf      # 或 systemctl reload nginx
```

路由表:`/vardec/`→8503(varia)、`/alpha/`→8501(pmbot)、
`/account-ops/`→核算台(自带前缀,不改写)、`/`→8600(聚合器)。
已含 Streamlit 必需的 WebSocket upgrade 头与长超时。

## 2. 两个 Streamlit 启动参数(手动改各自 systemd,本包不改)

反代到子路径要求 Streamlit 知道自己的前缀:

```bash
# varia dashboard(8503)启动参数追加:
--server.port 8503 --server.baseUrlPath=vardec

# pmbot dashboard(8501)启动参数追加:
--server.port 8501 --server.baseUrlPath=alpha
```

## 3. 总览聚合器

```bash
pip install fastapi uvicorn          # 仅聚合器所在 venv
cp deploy/unified-shell/examples/unified_shell_config.json deploy/unified-shell/
# 编辑 systems[].target 指向各系统真实 status 文件/端点(schema 见 STATUS_CONVENTION.md)
uvicorn overview_app:app --host 127.0.0.1 --port 8600 \
    --app-dir deploy/unified-shell
```

systemd 模板:

```ini
[Unit]
Description=Latitude unified-shell overview aggregator
After=network-online.target
[Service]
WorkingDirectory=/home/ubuntu/polymarket-bot/deploy/unified-shell
ExecStart=<VENV_PYTHON_REPLACE_ME> -m uvicorn overview_app:app --host 127.0.0.1 --port 8600
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
```

## 4. 本地验收(不部署)

```bash
# 聚合器对两个假 status 文件渲染两张卡(examples 内已备):
cd deploy/unified-shell && uvicorn overview_app:app --port 8600
# 浏览器打开 http://127.0.0.1:8600 → 应见「Var/Decibel(在线,含告警)」
# 与「Polymarket(在线)」两张卡 + 合并事件流;把 examples 配置里某个
# target 改成不存在的路径 → 该卡变灰「离线」。

# nginx 语法(本地把占位符 sed 成任意合法值即可):
sed -e 's/TAILSCALE_IP/127.0.0.1/' -e 's/ACCOUNT_OPS_UPSTREAM/127.0.0.1:8700/' \
    nginx.conf > /tmp/unified-shell-test.conf && nginx -t -c /tmp/unified-shell-test.conf
```

## 安全边界

聚合器**只读**:无任何操作按钮/POST 路由;只绑定 127.0.0.1,经 nginx 暴露到
Tailscale 网段;不读任何密钥,只消费各系统主动暴露的 status 数据。
