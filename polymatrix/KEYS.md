# PolyLPS 私钥管理模板（安全）

> 重要：**不要把真实私钥写进这个文件**。
> 这个文件只保存“别名映射”和操作步骤。

## 1) 别名映射（仅记录环境变量名）

- ACC1 -> `POLY_KEY_ACC1`
- ACC2 -> `POLY_KEY_ACC2`
- ACC3 -> `POLY_KEY_ACC3`
- ACC4 -> `POLY_KEY_ACC4`
- ACC5 -> `POLY_KEY_ACC5`

你可以继续加：ACC6...ACC10。

---

## 2) 首次写入私钥（在 PowerShell 执行）

> 把 `<...>` 替换成你的真实私钥（不要截图，不要发群）。

```powershell
setx POLY_KEY_ACC1 "<acc1_private_key>"
setx POLY_KEY_ACC2 "<acc2_private_key>"
setx POLY_KEY_ACC3 "<acc3_private_key>"
```

执行后**重开终端**。

---

## 3) 运行前选择当前私钥

```powershell
$env:POLY_PRIVATE_KEY = REDACTED
```

然后启动 dashboard / engine。

---

## 4) 快速检查是否已加载（仅检查有无，不打印密钥）

```powershell
if ($env:POLY_PRIVATE_KEY) { "KEY_OK" } else { "KEY_MISSING" }
```

---

## 5) 配置文件约束

`config.json` 里保持：

```json
"account": {
  "private_key": "REDACTED"
}
```

即：私钥不落盘。

---

## 6) 轮换建议

- 使用独立子钱包
- 子钱包只放可承受资金
- 定期轮换（如每月/每季度）
- 若怀疑泄露，立刻更换并停机排查
