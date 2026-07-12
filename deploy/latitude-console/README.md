# Latitude Alpha 统一控制台(HTML shell + 只读数据 API)

施工包05 呈现层的**方向修正**:原 5A/5B 在 Streamlit 里模仿模板,受框架限制无法
还原 `docs/dashboard_spec/latitude_console_full.html` 的样子。本目录直接以该模板为
前端,配一个 FastAPI 只读数据服务喂真数据——**所见即模板,数字变真**。

## 构成

- `console.html` — 模板本体(6 页:总览/Var·Decibel/Polymarket/Predict.fun/Single Account/打新)。
  关键数值带 `data-k` 钩子,前端每 15s 拉 `/api/state` 覆盖真数据;无真数据或数据过期时
  显示“未知/待核验”,绝不保留模板示例值。
- `console_app.py` — FastAPI:`/` 返回 console.html;`/api/state` 只读现有状态文件/库返回 JSON。
读取与呈现路径保持只读。受控写端点另受 `LATITUDE_ENABLE_WRITES` 总闸和审计日志约束。

## 真实性规则

- Var/Decibel 只有在同一主机的两家交易所读取均成功且快照未过期时，才判断空仓、对冲或单腿。
- 过期/缺源快照只显示“仓位未知”，可附上次看到的币种，但不计入当前持仓或单腿告警。
- Polymarket 引擎停止或状态文件过期时，活跃挂单显示“待核验”，不复用历史状态文件里的数量。
- Mac mini signer 使用 TCP 可达性作为控制台健康信号；launchd 进程字段只作辅助信息。
- Var/Decibel 与 Polymarket 生产面板的初始 HTML 不含仓位、事件或账号运行样例；
  页面首次加载和接口失败时统一显示“读取中/未知”。

## 本地运行

```bash
cd deploy/latitude-console
LATITUDE_DATA_DIR=../../data <venv>/bin/python -m uvicorn console_app:app --port 8610
# 打开 http://127.0.0.1:8610
```

## VPS 部署(与 5C 统一入口合并)

数据目录指向生产:`LATITUDE_DATA_DIR=/home/ubuntu/polymarket-bot/data`。
建议 systemd 常驻,绑 `127.0.0.1:8600`,nginx 把 `/` 从当前的 Streamlit(8501)
改指向本服务:

```nginx
location / {
    proxy_pass http://127.0.0.1:8600;   # 原为 127.0.0.1:8501(Streamlit),改为控制台
}
```

现有 Streamlit 页保留为**二级详情页**(模板 subnav 指向):`/varia/`→8503、
pmbot 引擎控制页仍可从 `/legacy/`(或保留 8501 某路径)进入。**本次不动 Streamlit,
只加一个新入口;确认控制台无误后再切 nginx 的 `/`,可随时切回。**

## 数据接入进度(有真数据先接,其余占位)

已接真数据(`/api/state`):
- **Polymarket**:engine_state_N.json 聚合(运行账号/挂单/今日量/PnL/sibling 统计)
- **Single Account**:paper 状态 JSON(候选/可执行/最高分)+ 模拟器库虚拟权益
- **Research 数据**:记录器心跳 + market.db 存在性
- **Var/Decibel**:本机 ops_state.json(host/时间)

占位待接(模板示例值,数据表/字段建成后接入):
- Var/Decibel 双 VPS 权益/损耗聚合(需 peer 快照目录 + 四源不混算聚合)
- Single Account 战绩矩阵/权益曲线/持仓(需 strategy_daily/positions_closed/equity_snapshots——
  施工包01 已建表,记录器出数据后即可接)
- Predict.fun 风险闸门、打新核算台明细

**四源不混算**:PM 账号矩阵按 account_idx 逐个读各自 engine_state,不跨账号复制;
Var/Decibel 聚合接入 peer 快照时须逐 host 相加,不拿 VPS1 冒充 VPS2(见仓库审计结论)。
