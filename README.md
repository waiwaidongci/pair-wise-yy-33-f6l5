# 电网事故应急与恢复调度系统

标准库 Python 3.11+ + SQLite。系统管理停运事故、重要用户、备用容量、恢复步骤及安全依赖；接受现场离线报告并区分已合并、版本冲突和受保护记录，异常遥测单独隔离。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

默认端口 `8215`。身份使用 `X-Actor` 和 `X-Role`，角色为 `dispatcher`、`operator`、`field`。可用 `--port`、`--db` 覆盖。

## 主要接口

- `POST /api/assets`、`POST /api/facilities`：登记线路资产和医院等重要用户。
- `POST /api/power-sources`、`GET /api/power-sources`：登记/查询可借调备用电源（容量、区域、联系人、归属）。
- `POST /api/outages`：创建或幂等接收同一事故。
- `POST /api/telemetry`：记录并隔离错误遥测。
- `POST /api/plans`、`/submit`、`/approve`、`/activate`：创建、提交、审批并启用安全恢复计划。
- `POST /api/plans/{id}/change`：在不修改已确认步骤的前提下创建新计划版本（已派电源随版本迁移）。
- `POST /api/plans/{id}/assign`、`/unassign`：给步骤指定/撤回电源、联系人、可用起止时间和优先级（1/2/3，3 最高），需带 `expected_resource_revision` 乐观锁；同一电源时段重叠时保留优先级更高（同级先登记）的一项，被挤掉的一项返回占用步骤 `occupied_by` 和可改派时段 `reassignable_slots`；已确认步骤拒绝换电。
- `POST /api/field-reports`：合并现场离线报告，重复客户端编号不会重复写入。
- `POST /api/plans/{id}/confirm`：调度员确认步骤，依赖未满足时拒绝。
- `GET /api/outages/{id}/resources`：按停电区域查看资源是否齐备（缺电源、冲突、无恢复步骤均列出阻止原因）。
- `POST /api/status`：发布当前恢复状态；资源未齐备时状态为 `blocked` 并在 `resource_blocking_reasons` 中说明阻止原因。
- `GET /api/plans/{id}`、`GET /api/state`、`GET /api/health`：详情、状态和健康检查。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

当前为原型：容量和依赖是静态安全模型，不包含潮流计算、SCADA/EMS 协议、实时遥测质量码或生产级多实例锁；离线合并通过客户端编号和计划版本完成。
