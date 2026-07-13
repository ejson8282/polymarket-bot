# HANDOFF — 交接文档(2026-07-09)

给下一个接手的 AI。本文件是当前工作的**权威快照**:做了什么、真实状态、已知 bug、
待主人决策的事。写作原则同项目一贯要求:**诚实优先,样例/假设与真实测量分清**。

> 一句话现状:两个项目并行。①`latitude-alpha` 统一控制台(本仓库,已在 GitHub,持续部署到
> 生产 VPS)——只读监控 + 受控写操作,PM 多VPS 已就绪待主人 Start;②`quant_project`
> (本地 git,`~/Desktop/claude code/quant_project`)——funding 套利 edge 研究,已出定论。

---

## 0. 最重要的两条纠错(避免重蹈)

1. **控制台"对冲仓位"面板在用模板样例冒充真数据。** 截图里看到的 `BTC -$0.01 / ETH +$0.15 /
   SOL -$36.42 DEC 裸腿` **全是 console.html 里硬编码的示例行**,不是真实持仓。
   **真实持仓(2026-07-09 核实,交易所 ops_state.json + /api/state `single_leg=[]`):
   全部 flat,size 0,没有任何裸腿/开仓。** 这是个待修 bug:该面板没接真数据覆盖,
   违反本项目"宁可空不可假"原则(P7 遮罩没盖到这个面板)。**别再拿这个面板的数字当真。**
2. **控制台 Var/Decibel 页的操作按钮(暂停自动化/一键平仓/手动开仓)是死的模板占位**
   (console.html ~292-295,无 id 无事件无接口)。PM 页操作是真接线的,VD 页不是。
   主人点了"一键平仓"实际什么都没发生——这是待补的接线工作(见 §4)。

---

## 1. 访问与环境

- **机器(Tailscale tailnet)**:VPS1 `100.122.255.98`(poly-vps1-hk)、VPS2 `100.101.50.40`
  (poly-vps2-tok)、mac-mini `100.91.159.54`(密钥/签名机)、Windows `100.82.86.62`(打新核算台)。
- **SSH**:`ssh ubuntu@100.122.255.98`(Tailscale SSH,偶尔需浏览器重认证,给主人链接即可)。
  VPS1→VPS2 用 `~/.ssh/id_ed25519` 可 `sudo -n systemctl`(控制台远程控账号2靠它)。
  mac-mini 用 `ssh 100.91.159.54`(~/.ssh/config 里 User=kevinsmacmini)。
  **auto 模式分类器会拦 SSH 上密钥机/自改权限——需要主人在非 auto 会话点允许。**
- **部署链**:GitHub main → 推 VPS1 bare(`ubuntu@100.122.255.98:/home/ubuntu/repos/polymarket-bot.git`)
  → VPS1/VPS2 working tree `git pull` → restart 对应 service。**碰交易/风控/下单的改动走 PR**
  (auto 分类器也会强制这一点)。
- **控制台入口**:`http://100.122.255.98:8502/`(新控制台,唯一总览)、`:8502/alpha/`(pmbot 操作后台 Streamlit)、
  `:8502/varia/`(varia 旧 dashboard,真能平仓的工具在这)。tailnet 免密,公网带 basic auth。

## 2. latitude-alpha 控制台(本仓库 `deploy/latitude-console/`)

- **console_app.py**:FastAPI,`GET /api/state`(只读聚合六源)+ `GET /`(console.html)+ 一批受控写端点。
  写端点由 `LATITUDE_ENABLE_WRITES=1` 总闸 + `console_write_audit.jsonl` 审计 + Cloudflare 公网只读。
  **四源不混算铁律**:vps1/decibel、vps1/var、vps2/decibel、vps2/var 逐源独立读,缺失标缺失,绝不复制顶替。
- **console.html**:单页,`data-k` 绑定 + 原生渲染器(bindPM/bindVD/bindSA/…),15s 拉一次 /api/state。
- **已交付并上线**(git log 可查):PM 账号矩阵、原生子视图(零 iframe)、周预算编辑、自动运行开关、
  P1 延迟优化(4s→26ms 后台预取)、P3 权益曲线、P5 停摆徽章+告警、P7 缺源遮罩、
  收益核算面板(账户-日粒度)、PM 多VPS 一账号模型(账号1@VPS1 本地 / 账号2@VPS2 远程 SSH systemctl)、
  启动预检(/api/pm/precheck:签名器 TCP+每账号 derive-creds+markets 新鲜度)、双通道告警(见 §5)。
- **服务**:`latitude-console.service`(:8600,User=ubuntu)。VPS2 的账号2 = `polymarket-engine.service`
  跑 `engine.py config_2.json`(.venv2),`remote_accounts.json` 定义路由。

### PM(Polymarket)现状
- **是 LP 做市返利收割机**,不是方向交易:贴最优买价挂被动单赚每日流动性奖励,核心"挂单赚钱、躲成交";
  成交即触发 kill-switch 撤单+unwind。下单量随可用抵押金走(非固定 $100)。
- **万事俱备,等主人按 Start**:signer 已修复常驻(mac-mini `ai.codex.polymarket-signer` launchd,
  别的 AI 交接过)、config 已应用 3 个 A 区市场(2026 世界杯夺冠盘)、预检四项全绿、
  账号1@VPS1 + 账号2@VPS2 双机就绪。**首启建议只勾一个账号试水。Start 永远是主人的动作。**
- 引擎提升清单(主人已表态):收益核算=要(已做);rewardsMinSize 陷阱诊断=要做
  (一份本金可无限挂只要不成交,但低于市场 minSize 则奖励=0 风险照担);顶腿激进一档=不要;
  退出阶梯降价=主人另有想法,暂缓;session-confirm 闭环=暂不做(且 `session.enabled=true` 但
  `confirm_required=false`,闸门实际不拦)。

### Polymarket + Predict.fun 共用 Rust 内核(2026-07-13,第一阶段)
- 仓库内 `rust-maker/` 已加入共用领域模型、确定性订单对账、风险限制、行情新鲜度检查、
  同一标准化 instrument 的跨账号自成交保护，以及两个平台的纯数据适配器。
- 当前唯一可执行程序是 `maker-dry-run`，只读 JSON 并输出 create/keep/cancel/replace 计划；
  **没有签名、HTTP 写请求或实盘执行能力，不替换现有 Python worker。**
- 外部/手工订单没有 `managed_slot` 时只报告、不撤单。Polymarket 现有 sibling/cross-side sentinel
  继续负责互补 outcome 语义，Rust 第一阶段不得绕过。
- 下一步只能先做 Python shadow 对比和历史 replay；达到计划一致性后再单独评审执行接口。
  详细边界、命令和迁移顺序见 `rust-maker/README.md`。
- 离线 shadow 不要求原 worker 启动：`scripts/maker_shadow_compare.py` 会把同一 JSON 快照交给
  标准库 Python reference oracle 与 Rust CLI，对比风险结论和订单动作；任何差异都返回非零。

### var/decibel(旧系统)现状 —— 主人已决定"该停了,后面新系统接"
- **它是活的、全自动真钱交易**:`auto_strategy_state` enabled=True/full_auto,周亏 $15/VPS 封顶,
  VPS1+VPS2 各跑 `varia-decibel-dashboard-worker`(job_worker,真实下单),VPS1 另有只读 dashboard(:8503)。
  Var 买/Decibel 卖 的 delta-neutral 对冲刷分。
- **主人已让暂停自动开仓**(enabled 已设 False),**当前无任何持仓(全 flat)**。主人要求"平掉持仓再停旧系统"——
  但核实后**没有持仓要平**(那 -$36 是模板假数据)。**下一步 = 停掉两台 job_worker(+ 可选停 :8503 dashboard),
  由新系统接管。停之前跟主人再确认一次即可(已无持仓风险)。**
- 真要平仓的正规工具在 varia dashboard(`:8502/varia/`)Trade 页:双腿用"一键平仓(reduce-only)"、
  单腿用手动 Close(reduce-only)。`close_all` 只平双腿,单腿设计上要人工。

## 3. quant_project(本地 `~/Desktop/claude code/quant_project`,**无 GitHub 远程**)

- 交接自 zip 的量化系统骨架(core/ + strategies/ + research/)。**项目内 CLAUDE.md 是其记忆核心,必先读。**
- **funding 套利 edge 已出定论(2026-07-09,真实测量)**:扫全部 140 个 HL×OKX 共同永续(45天)。
  漏斗 **140→34(数据上有价差)→2(容量+持续性过滤,CHIP/TRUMP)→1(真盘口深度,只剩 TRUMP)→
  且仅作 maker 才成立**(taker 跨价差吃光;maker 费 ~7bps,1.4 天回本)。复现了 Akey 论文
  "赢家 maker/输家 taker"。**卡点 = maker 填充率+单腿敞口,是实盘执行问题,历史数据答不了。**
- Binance 从本网络 451 地理封锁,按规矩不绕(CEX 腿用 OKX)。
- **待主人**:B 方向(接 Variational 只读接口/文档,测主人真实系统的边 + 用其成交验 maker 填充率)——
  主人已选此路,待给接口。或 A(本地写 order-book 成交模拟验 TRUMP)/ C(转预测市场套利)。
- **注意**:此项目只有本地 git,若要交接到 GitHub 需主人建 repo / 授权;否则下一个 AI 在本机读 CLAUDE.md + git log。

## 4. 已知 bug / 未完成(优先处理)

1. **控制台"对冲仓位"面板显示模板假数据**(§0.1)——需接真实 ops_state 持仓,拉不到就盖遮罩。
2. **控制台 Var/Decibel 操作按钮是死占位**(§0.2)——需原生接线:暂停自动化(按 VPS 路由,
   /api/varia/auto 目前只写 VPS1)、一键平仓(复用 varia 的 `_close_all_plan`+`_enqueue_dashboard_job`,
   建议在 varia 侧写个 CLI 供控制台调,别在控制台重造真钱逻辑)、单腿平仓。**真钱代码,主人点最后一下。**
3. **Discord webhook 失效(403)**:主人两次给的是同一串已失效的。日常通道(warn/早报/写操作)因此不通,
   飞书已改为不回退(warn 不再淹飞书)。**需主人生成一个新的 Discord webhook**(URL 会变),
   放 VPS1 `data/discord_webhook.txt`(chmod 600,不入 git)。飞书 webhook 正常(只收 crit)。

## 5. 告警/通知(已降噪)

- 飞书=只收持续存在的 crit(单腿、预算告急、引擎在跑却掉签名器);Discord=warn/解除/每日早报/写操作记录。
- `alert_pusher.py`:600s 防抖(告警须熬过一周期才推,自愈的短暂闪断永不打扰)、只对推过的报解除、
  慢源保留 5min 内上次好值(不再单次超时就翻"不可达")。timer:alert-pusher(5min)、alert-digest(每日09:00)。

## 6. 铁律(跨会话)

- 碰交易/风控/下单 → 走 PR,不直推 main(auto 分类器也强制)。
- 四源不混算;宁可空不可假(不拿样例/假设冒充真数据)。
- 真钱下单不替主人执行——建工具、备好、主人点最后一下。
- 不绕地域限制(Binance VPN 不碰);不读 mac-mini 密钥内容。
- 展示收益数字标可信度层级(quant_project 尤其)。

---

*最后更新:2026-07-09,由 Claude(Opus 4.8 / Fable 5 交替)。git log 是更细的时间线。*
