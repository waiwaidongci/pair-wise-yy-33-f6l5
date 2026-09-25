#!/usr/bin/env python3
"""Grid outage restoration planning, field-report merge and status publishing demo."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = Path(__file__).with_name("data.db")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def j(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class ApiError(Exception):
    def __init__(self, status: int, message: str, details: dict | None = None):
        super().__init__(message); self.status, self.message, self.details = status, message, details


class Store:
    def __init__(self, path: str | Path = DB_PATH):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False); self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL"); self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS assets (
          id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
          asset_type TEXT NOT NULL, capacity_mw REAL NOT NULL, parent_id INTEGER REFERENCES assets(id), region TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS facilities (
          id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, facility_type TEXT NOT NULL,
          asset_id INTEGER NOT NULL REFERENCES assets(id), priority INTEGER NOT NULL, backup_power_mw REAL NOT NULL,
          connected INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS outages (
          id INTEGER PRIMARY KEY AUTOINCREMENT, incident_code TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('reported','assessing','restoring','restored')),
          affected_regions_json TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
          opened_by TEXT NOT NULL, opened_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS plans (
          id INTEGER PRIMARY KEY AUTOINCREMENT, outage_id INTEGER NOT NULL REFERENCES outages(id),
          version INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('draft','submitted','approved','active','superseded')),
          steps_json TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL,
          created_at TEXT NOT NULL, approved_by TEXT, approved_at TEXT, activated_at TEXT,
          UNIQUE(outage_id,version)
        );
        CREATE TABLE IF NOT EXISTS confirmations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES plans(id), step_no INTEGER NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('confirmed','blocked')), confirmed_by TEXT NOT NULL, confirmed_at TEXT NOT NULL,
          note TEXT, UNIQUE(plan_id,step_no)
        );
        CREATE TABLE IF NOT EXISTS field_reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT, client_report_id TEXT UNIQUE NOT NULL, plan_id INTEGER NOT NULL REFERENCES plans(id),
          step_no INTEGER NOT NULL, expected_plan_version INTEGER NOT NULL, status TEXT NOT NULL,
          note TEXT, merge_status TEXT NOT NULL CHECK(merge_status IN ('merged','conflict','protected')),
          conflict_reason TEXT, reported_by TEXT NOT NULL, received_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS telemetry (
          id INTEGER PRIMARY KEY AUTOINCREMENT, asset_id INTEGER NOT NULL REFERENCES assets(id), load_mw REAL NOT NULL,
          voltage_kv REAL NOT NULL, timestamp TEXT NOT NULL, valid INTEGER NOT NULL, anomaly TEXT,
          recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS published_status (
          id INTEGER PRIMARY KEY AUTOINCREMENT, outage_id INTEGER NOT NULL REFERENCES outages(id),
          plan_id INTEGER NOT NULL REFERENCES plans(id), version INTEGER NOT NULL, status_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS power_resources (
          id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
          resource_type TEXT NOT NULL, capacity_mw REAL NOT NULL, owner TEXT NOT NULL,
          region TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'available'
            CHECK(status IN ('available','retired')), registered_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS resource_assignments (
          id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES plans(id), step_no INTEGER NOT NULL,
          resource_id INTEGER NOT NULL REFERENCES power_resources(id), contact_name TEXT NOT NULL,
          contact_phone TEXT, available_from TEXT NOT NULL, available_to TEXT NOT NULL,
          priority INTEGER NOT NULL CHECK(priority BETWEEN 1 AND 3),
          state TEXT NOT NULL DEFAULT 'allocated' CHECK(state IN ('allocated','displaced')),
          occupied_by_plan_id INTEGER, occupied_by_step_no INTEGER,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(plan_id,step_no)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        """)
        self.conn.commit()

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def close(self) -> None: self.conn.close()


class GridService:
    def __init__(self, store: Store): self.store, self.conn = store, store.conn

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role not in allowed: raise ApiError(403, "角色无权执行此操作")
        return actor

    def _row(self, table: str, identity: int) -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
        if not row: raise ApiError(404, "对象不存在")
        return row

    def register_asset(self, actor: str | None, role: str | None, code: str, name: str, asset_type: str, capacity_mw: float, region: str, parent_id: int | None = None) -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        if not code.strip() or not region.strip() or capacity_mw <= 0: raise ApiError(400, "线路资产参数不合法")
        if parent_id is not None: self._row("assets", int(parent_id))
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO assets(code,name,asset_type,capacity_mw,parent_id,region) VALUES(?,?,?,?,?,?)",
                                        (code, name, asset_type, float(capacity_mw), parent_id, region))
                self.store.audit(actor, "asset.register", "asset", cur.lastrowid, {"code": code, "capacity_mw": capacity_mw, "region": region})
        except sqlite3.IntegrityError as exc: raise ApiError(409, "资产代号已存在") from exc
        return {"id": cur.lastrowid, "code": code, "name": name, "asset_type": asset_type, "capacity_mw": capacity_mw, "region": region, "parent_id": parent_id}

    def register_facility(self, actor: str | None, role: str | None, name: str, facility_type: str, asset_id: int, priority: int, backup_power_mw: float) -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        self._row("assets", asset_id)
        if priority not in {1, 2, 3} or backup_power_mw < 0: raise ApiError(400, "重要用户参数不合法")
        with self.conn:
            cur = self.conn.execute("INSERT INTO facilities(name,facility_type,asset_id,priority,backup_power_mw) VALUES(?,?,?,?,?)", (name, facility_type, asset_id, priority, backup_power_mw))
            self.store.audit(actor, "facility.register", "facility", cur.lastrowid, {"name": name, "priority": priority})
        return {"id": cur.lastrowid, "name": name, "facility_type": facility_type, "asset_id": asset_id, "priority": priority, "backup_power_mw": backup_power_mw}

    def create_outage(self, actor: str | None, role: str | None, incident_code: str, title: str, affected_regions: list[str]) -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        if not incident_code.strip() or not affected_regions: raise ApiError(400, "事故编号和影响区域不能为空")
        existing = self.conn.execute("SELECT * FROM outages WHERE incident_code=?", (incident_code,)).fetchone()
        if existing:
            if existing["title"] == title and json.loads(existing["affected_regions_json"]) == affected_regions:
                return self._outage_dict(existing)
            raise ApiError(409, "事故编号已存在但内容不同")
        with self.conn:
            cur = self.conn.execute("INSERT INTO outages(incident_code,title,state,affected_regions_json,opened_by,opened_at,updated_at) VALUES(?,?,'reported',?,?,?,?)",
                                    (incident_code, title, j(affected_regions), actor, now(), now()))
            self.store.audit(actor, "outage.create", "outage", cur.lastrowid, {"incident_code": incident_code})
        return self._outage_dict(self._row("outages", cur.lastrowid))

    def record_telemetry(self, actor: str | None, role: str | None, asset_id: int, load_mw: float, voltage_kv: float, timestamp: str) -> dict:
        actor = self._actor(actor, role, {"operator"})
        asset = self._row("assets", asset_id)
        anomaly = None
        if load_mw < 0 or voltage_kv <= 0: anomaly = "负荷或电压超出物理范围"
        elif load_mw > float(asset["capacity_mw"]) * 1.2: anomaly = "负载超过额定容量20%"
        elif voltage_kv > 500: anomaly = "电压测量值超出本地范围"
        with self.conn:
            cur = self.conn.execute("INSERT INTO telemetry(asset_id,load_mw,voltage_kv,timestamp,valid,anomaly,recorded_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                                    (asset_id, load_mw, voltage_kv, timestamp, int(anomaly is None), anomaly, actor, now()))
            self.store.audit(actor, "telemetry.record", "asset", asset_id, {"valid": anomaly is None, "anomaly": anomaly})
        return {"id": cur.lastrowid, "asset_id": asset_id, "load_mw": load_mw, "voltage_kv": voltage_kv, "valid": anomaly is None, "anomaly": anomaly}

    def create_plan(self, actor: str | None, role: str | None, outage_id: int, steps: list[dict]) -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        outage = self._row("outages", outage_id)
        normalized = self._validate_steps(steps)
        version = self.conn.execute("SELECT COALESCE(MAX(version),0)+1 FROM plans WHERE outage_id=?", (outage_id,)).fetchone()[0]
        with self.conn:
            cur = self.conn.execute("INSERT INTO plans(outage_id,version,state,steps_json,created_by,created_at) VALUES(?,?, 'draft',?,?,?)",
                                    (outage_id, version, j(normalized), actor, now()))
            self.conn.execute("UPDATE outages SET state='assessing',revision=revision+1,updated_at=? WHERE id=?", (now(), outage_id))
            self.store.audit(actor, "plan.create", "plan", cur.lastrowid, {"outage_id": outage_id, "version": version, "steps": len(normalized)})
        return self._plan_dict(self._row("plans", cur.lastrowid))

    def submit_plan(self, actor: str | None, role: str | None, plan_id: int, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"dispatcher"}); plan = self._row("plans", plan_id)
        if plan["state"] != "draft": raise ApiError(409, "只有草稿计划可以提交")
        self._plan_update(plan, "submitted", expected_revision, actor, "plan.submit", {})
        return self._plan_dict(self._row("plans", plan_id))

    def approve_plan(self, actor: str | None, role: str | None, plan_id: int, expected_revision: int, note: str = "") -> dict:
        actor = self._actor(actor, role, {"dispatcher"}); plan = self._row("plans", plan_id)
        if plan["state"] != "submitted": raise ApiError(409, "只有已提交计划可以批准")
        self._validate_safety(plan)
        self._plan_update(plan, "approved", expected_revision, actor, "plan.approve", {"note": note})
        self.conn.execute("UPDATE plans SET approved_by=?,approved_at=? WHERE id=?", (actor, now(), plan_id))
        self.conn.commit()
        return self._plan_dict(self._row("plans", plan_id))

    def activate_plan(self, actor: str | None, role: str | None, plan_id: int, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"dispatcher"}); plan = self._row("plans", plan_id)
        if plan["state"] != "approved": raise ApiError(409, "计划尚未批准")
        with self.conn:
            self.conn.execute("UPDATE plans SET state='superseded' WHERE outage_id=? AND state='active'", (plan["outage_id"],))
            updated = self.conn.execute("UPDATE plans SET state='active',revision=revision+1,activated_at=? WHERE id=? AND revision=?",
                                        (now(), plan_id, expected_revision))
            if updated.rowcount != 1: raise ApiError(409, "计划版本冲突")
            self.conn.execute("UPDATE outages SET state='restoring',revision=revision+1,updated_at=? WHERE id=?", (now(), plan["outage_id"]))
            self.store.audit(actor, "plan.activate", "plan", plan_id, {"outage_id": plan["outage_id"], "version": plan["version"]})
        return self._plan_dict(self._row("plans", plan_id))

    def make_plan_change(self, actor: str | None, role: str | None, base_plan_id: int, steps: list[dict], expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"dispatcher"}); base = self._row("plans", base_plan_id)
        if base["state"] not in {"approved", "active"}: raise ApiError(409, "只有已批准或执行中的计划可以变更")
        if int(expected_revision) != int(base["revision"]): raise ApiError(409, "计划已被修改，请刷新版本")
        normalized = self._validate_steps(steps)
        confirmed = {row["step_no"]: row for row in self.conn.execute("SELECT * FROM confirmations WHERE plan_id=? ORDER BY step_no", (base_plan_id,))}
        base_steps = {int(step["seq"]): step for step in json.loads(base["steps_json"])}
        for seq in confirmed:
            if seq not in {int(step["seq"]) for step in normalized} or normalized[[int(x["seq"]) for x in normalized].index(seq)] != base_steps[seq]:
                raise ApiError(409, "新计划不能改动已确认步骤")
        outage = self._row("outages", base["outage_id"])
        version = int(base["version"]) + 1
        with self.conn:
            cur = self.conn.execute("INSERT INTO plans(outage_id,version,state,steps_json,created_by,created_at) VALUES(?,?, 'draft',?,?,?)",
                                    (outage["id"], version, j(normalized), actor, now()))
            self.conn.execute("UPDATE plans SET state='superseded' WHERE id=?", (base_plan_id,))
            self._copy_confirmations(base_plan_id, cur.lastrowid, normalized, confirmed)
            self._copy_assignments(base_plan_id, cur.lastrowid, normalized)
            self.store.audit(actor, "plan.change_create", "plan", cur.lastrowid, {"base_plan": base_plan_id, "version": version, "carried_confirmations": len(confirmed)})
            for row in self.conn.execute("SELECT DISTINCT resource_id FROM resource_assignments WHERE plan_id=?", (cur.lastrowid,)):
                self._recompute_resource(int(row["resource_id"]))
        return self._plan_dict(self._row("plans", cur.lastrowid))

    def field_report(self, actor: str | None, role: str | None, plan_id: int, step_no: int, client_report_id: str, expected_plan_version: int, status: str, note: str = "") -> dict:
        actor = self._actor(actor, role, {"field"})
        if status not in {"started", "completed", "blocked"}: raise ApiError(400, "现场状态不合法")
        plan = self._row("plans", plan_id); steps = {int(x["seq"]): x for x in json.loads(plan["steps_json"])}
        if step_no not in steps: raise ApiError(400, "计划中没有该步骤")
        duplicate = self.conn.execute("SELECT * FROM field_reports WHERE client_report_id=?", (client_report_id,)).fetchone()
        if duplicate: return dict(duplicate)
        merge_status, conflict = "merged", None
        if plan["state"] != "active": merge_status, conflict = "conflict", "计划尚未激活"
        elif int(expected_plan_version) != int(plan["version"]): merge_status, conflict = "conflict", "现场报告基于旧计划版本"
        elif self.conn.execute("SELECT id FROM confirmations WHERE plan_id=? AND step_no=?", (plan_id, step_no)).fetchone():
            merge_status, conflict = "protected", "已确认记录不能由普通现场报告覆盖"
        with self.conn:
            cur = self.conn.execute("""INSERT INTO field_reports(client_report_id,plan_id,step_no,expected_plan_version,status,note,merge_status,conflict_reason,reported_by,received_at)
                                     VALUES(?,?,?,?,?,?,?,?,?,?)""", (client_report_id, plan_id, step_no, expected_plan_version, status, note, merge_status, conflict, actor, now()))
            if merge_status == "merged" and status in {"completed", "blocked"}:
                self.store.audit(actor, "field_report.merged", "plan", plan_id, {"step_no": step_no, "status": status, "client_report_id": client_report_id})
            self.store.audit(actor, "field_report.received", "plan", plan_id, {"step_no": step_no, "merge_status": merge_status, "conflict": conflict})
        return dict(self._row("field_reports", cur.lastrowid))

    def confirm_step(self, actor: str | None, role: str | None, plan_id: int, step_no: int, decision: str, note: str = "") -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        if decision not in {"confirmed", "blocked"}: raise ApiError(400, "确认状态不合法")
        plan = self._row("plans", plan_id)
        if plan["state"] != "active": raise ApiError(409, "只有执行中的计划可以确认")
        steps = {int(x["seq"]): x for x in json.loads(plan["steps_json"])}
        if step_no not in steps: raise ApiError(400, "计划中没有该步骤")
        report = self.conn.execute("SELECT * FROM field_reports WHERE plan_id=? AND step_no=? AND merge_status='merged' ORDER BY id DESC LIMIT 1", (plan_id, step_no)).fetchone()
        if not report: raise ApiError(409, "没有可确认的现场报告")
        if decision == "confirmed" and report["status"] != "completed": raise ApiError(409, "现场步骤尚未完成")
        for dependency in steps[step_no].get("depends_on", []):
            found = self.conn.execute("SELECT * FROM confirmations WHERE plan_id=? AND step_no=? AND status='confirmed'", (plan_id, int(dependency))).fetchone()
            if not found: raise ApiError(409, f"前置步骤 {dependency} 尚未确认")
        with self.conn:
            self.conn.execute("""INSERT INTO confirmations(plan_id,step_no,status,confirmed_by,confirmed_at,note) VALUES(?,?,?,?,?,?)
                               ON CONFLICT(plan_id,step_no) DO UPDATE SET status=excluded.status,confirmed_by=excluded.confirmed_by,confirmed_at=excluded.confirmed_at,note=excluded.note""",
                              (plan_id, step_no, decision, actor, now(), note))
            self.store.audit(actor, "plan.confirm_step", "plan", plan_id, {"step_no": step_no, "status": decision, "note": note})
        return dict(self.conn.execute("SELECT * FROM confirmations WHERE plan_id=? AND step_no=?", (plan_id, step_no)).fetchone())

    def register_power_resource(self, actor: str | None, role: str | None, code: str, name: str, resource_type: str,
                                capacity_mw: float, owner: str, region: str, contact: str = "", contact_phone: str = "") -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        if not code.strip() or not name.strip() or not region.strip(): raise ApiError(400, "电源代号、名称和区域不能为空")
        if capacity_mw <= 0: raise ApiError(400, "电源容量必须为正")
        try:
            with self.conn:
                cur = self.conn.execute("""INSERT INTO power_resources(code,name,resource_type,capacity_mw,owner,region,registered_by,created_at)
                                         VALUES(?,?,?,?,?,?,?,?)""",
                                        (code, name, resource_type or "generator", float(capacity_mw), owner, region, actor, now()))
                self.store.audit(actor, "resource.register", "power_resource", cur.lastrowid,
                                 {"code": code, "capacity_mw": capacity_mw, "region": region})
        except sqlite3.IntegrityError as exc: raise ApiError(409, "电源代号已存在") from exc
        return self._power_resource_dict(self._row("power_resources", cur.lastrowid))

    def assign_resource(self, actor: str | None, role: str | None, plan_id: int, step_no: int, resource_id: int,
                        contact_name: str, available_from: str, available_to: str, priority: int,
                        expected_revision: int, contact_phone: str = "") -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        plan = self._row("plans", plan_id)
        if plan["state"] == "superseded": raise ApiError(409, "计划已被新版本替代，不能再排定资源")
        if int(expected_revision) < 0 or int(expected_revision) != int(plan["revision"]):
            raise ApiError(409, "计划版本已变化，请重新加载后再保存", {"current_revision": plan["revision"]})
        steps = {int(x["seq"]): x for x in json.loads(plan["steps_json"])}
        if step_no not in steps: raise ApiError(400, "计划中没有该步骤")
        if self.conn.execute("SELECT id FROM confirmations WHERE plan_id=? AND step_no=?", (plan_id, step_no)).fetchone():
            raise ApiError(409, f"步骤 {step_no} 已确认，不能换电", {"protected_step": step_no})
        resource = self._row("power_resources", resource_id)
        if resource["status"] != "available": raise ApiError(409, "该电源已停用，不能排定")
        if not str(contact_name).strip(): raise ApiError(400, "联系人不能为空")
        if int(priority) not in {1, 2, 3}: raise ApiError(400, "优先级必须为 1（高）、2（中）或 3（低）")
        win_from = self._parse_ts(available_from, "可用开始时间")
        win_to = self._parse_ts(available_to, "可用结束时间")
        if win_to <= win_from: raise ApiError(400, "可用结束时间必须晚于开始时间")
        required_mw = float(steps[step_no]["required_mw"])
        if required_mw > float(resource["capacity_mw"]):
            raise ApiError(400, f"电源容量 {resource['capacity_mw']}MW 不足以支撑步骤需求 {required_mw}MW",
                           {"required_mw": required_mw, "capacity_mw": resource["capacity_mw"]})
        with self.conn:
            existing = self.conn.execute("SELECT * FROM resource_assignments WHERE plan_id=? AND step_no=?", (plan_id, step_no)).fetchone()
            if existing:
                old_resource_id = int(existing["resource_id"])
                self.conn.execute("""UPDATE resource_assignments SET resource_id=?,contact_name=?,contact_phone=?,available_from=?,
                                   available_to=?,priority=?,state='allocated',occupied_by_plan_id=NULL,occupied_by_step_no=NULL,updated_at=?
                                   WHERE id=?""",
                                  (resource_id, contact_name, contact_phone, self._fmt_ts(win_from), self._fmt_ts(win_to),
                                   int(priority), now(), existing["id"]))
                assignment_id = existing["id"]
            else:
                cur = self.conn.execute("""INSERT INTO resource_assignments(plan_id,step_no,resource_id,contact_name,contact_phone,
                                       available_from,available_to,priority,state,created_at,updated_at)
                                       VALUES(?,?,?,?,?,?,?,?,'allocated',?,?)""",
                                        (plan_id, step_no, resource_id, contact_name, contact_phone,
                                         self._fmt_ts(win_from), self._fmt_ts(win_to), int(priority), now(), now()))
                assignment_id = cur.lastrowid
                old_resource_id = None
            self._recompute_resource(resource_id)
            if old_resource_id is not None and old_resource_id != resource_id: self._recompute_resource(old_resource_id)
            updated = self.conn.execute("UPDATE plans SET revision=revision+1 WHERE id=? AND revision=?",
                                        (plan_id, int(expected_revision)))
            if updated.rowcount != 1: raise ApiError(409, "计划版本冲突，请重新加载", {"current_revision": self._row("plans", plan_id)["revision"]})
            self.store.audit(actor, "resource.assign", "plan", plan_id,
                             {"step_no": step_no, "resource_id": resource_id, "priority": int(priority)})
        return {"assignment": self._assignment_dict(self._row("resource_assignments", assignment_id)),
                "plan_revision": self._row("plans", plan_id)["revision"]}

    def _recompute_resource(self, resource_id: int) -> None:
        """对同一电源的全部排定按 已确认锁定 > 优先级 > 先排定 重新仲裁，败者标记为被占用。"""
        rows = [dict(row) for row in self.conn.execute("""SELECT a.* FROM resource_assignments a JOIN plans p ON a.plan_id=p.id
                                                         WHERE a.resource_id=? AND p.state!='superseded' ORDER BY a.id""", (resource_id,))]
        def key(row: dict) -> tuple:
            protected = 0 if self.conn.execute("SELECT id FROM confirmations WHERE plan_id=? AND step_no=?",
                                               (row["plan_id"], row["step_no"])).fetchone() else 1
            return (protected, int(row["priority"]), int(row["id"]))
        rows.sort(key=key)
        holders: list[dict] = []
        for row in rows:
            a_from, a_to = self._parse_ts(row["available_from"], ""), self._parse_ts(row["available_to"], "")
            occupier = None
            for kept in holders:
                k_from, k_to = self._parse_ts(kept["available_from"], ""), self._parse_ts(kept["available_to"], "")
                if a_from < k_to and k_from < a_to: occupier = kept; break
            if occupier is None:
                self.conn.execute("UPDATE resource_assignments SET state='allocated',occupied_by_plan_id=NULL,occupied_by_step_no=NULL,updated_at=? WHERE id=?",
                                  (now(), row["id"]))
                holders.append(row)
            else:
                self.conn.execute("UPDATE resource_assignments SET state='displaced',occupied_by_plan_id=?,occupied_by_step_no=?,updated_at=? WHERE id=?",
                                  (occupier["plan_id"], occupier["step_no"], now(), row["id"]))

    def resource_readiness(self, outage_id: int, plan_id: int | None = None) -> dict:
        outage = self._row("outages", outage_id)
        regions = json.loads(outage["affected_regions_json"])
        if plan_id is None:
            row = self.conn.execute("SELECT * FROM plans WHERE outage_id=? ORDER BY version DESC LIMIT 1", (outage_id,)).fetchone()
            if row is None: return {"outage_id": outage_id, "plan_id": None, "ready": False,
                                    "regions": [{"region": region, "ready": False, "block_reasons": ["该区域尚无恢复计划步骤"]} for region in regions]}
            plan = row
        else:
            plan = self._row("plans", plan_id)
            if plan["outage_id"] != outage_id: raise ApiError(400, "计划不属于该事故")
        steps = json.loads(plan["steps_json"])
        assets = {row["code"]: dict(row) for row in self.conn.execute("SELECT * FROM assets")}
        assignments = {(int(row["plan_id"]), int(row["step_no"])): dict(row)
                       for row in self.conn.execute("SELECT * FROM resource_assignments WHERE plan_id=?", (plan["id"],))}
        resources = {int(row["id"]): dict(row) for row in self.conn.execute("SELECT * FROM power_resources")}
        grouped: dict[str, list[dict]] = {region: [] for region in regions}
        for step in steps:
            region = assets.get(step["asset"], {}).get("region", "未登记区域")
            grouped.setdefault(region, [])
            entry = {"seq": int(step["seq"]), "asset": step["asset"], "required_mw": float(step["required_mw"]),
                     "ready": False, "block_reasons": []}
            assignment = assignments.get((int(plan["id"]), entry["seq"]))
            if assignment is None:
                entry["block_reasons"].append("步骤未指定备用电源")
            else:
                resource = resources.get(int(assignment["resource_id"]))
                if resource is None:
                    entry["block_reasons"].append("指定电源已注销")
                elif resource["status"] != "available":
                    entry["block_reasons"].append(f"电源 {resource['code']} 已停用")
                elif entry["required_mw"] > float(resource["capacity_mw"]):
                    entry["block_reasons"].append(f"电源 {resource['code']} 容量 {resource['capacity_mw']}MW 不足（需 {entry['required_mw']}MW）")
                if assignment["state"] == "displaced":
                    occ_plan, occ_step = assignment["occupied_by_plan_id"], assignment["occupied_by_step_no"]
                    entry["block_reasons"].append(f"电源时段被计划#{occ_plan}步骤{occ_step}占用")
                win_from, win_to = self._parse_ts(assignment["available_from"], ""), self._parse_ts(assignment["available_to"], "")
                moment = datetime.now(timezone.utc)
                if not (win_from <= moment <= win_to):
                    entry["block_reasons"].append(f"当前不在电源可用时段内（{assignment['available_from']} ~ {assignment['available_to']}）")
            entry["ready"] = not entry["block_reasons"]
            grouped[region].append(entry)
        region_summaries = []
        for region in regions:
            entries = grouped.get(region, [])
            reasons = [f"步骤 {e['seq']}：{reason}" for e in entries for reason in e["block_reasons"]]
            if not entries: reasons = ["该区域尚无恢复计划步骤"]
            region_summaries.append({"region": region, "ready": not reasons, "steps": entries, "block_reasons": reasons})
        return {"outage_id": outage_id, "plan_id": plan["id"], "plan_version": plan["version"], "plan_state": plan["state"],
                "ready": all(item["ready"] for item in region_summaries), "regions": region_summaries}

    def publish_status(self, actor: str | None, role: str | None, outage_id: int, plan_id: int) -> dict:
        actor = self._actor(actor, role, {"dispatcher"})
        plan = self._row("plans", plan_id); outage = self._row("outages", outage_id)
        if plan["outage_id"] != outage_id: raise ApiError(400, "计划不属于该事故")
        readiness = self.resource_readiness(outage_id, plan_id)
        if not readiness["ready"]:
            blockers = [{"region": item["region"], "reasons": item["block_reasons"]}
                        for item in readiness["regions"] if not item["ready"]]
            raise ApiError(409, "备用电源尚未齐备，不能发布恢复完成状态", {"blocked_regions": blockers})
        confirmations = {row["step_no"]: dict(row) for row in self.conn.execute("SELECT * FROM confirmations WHERE plan_id=?", (plan_id,))}
        steps = json.loads(plan["steps_json"])
        completed = sum(1 for step in steps if confirmations.get(int(step["seq"]), {}).get("status") == "confirmed")
        status = {"outage_id": outage_id, "incident_code": outage["incident_code"], "plan_id": plan_id, "plan_version": plan["version"],
                  "state": "restored" if completed == len(steps) else "restoring", "completed_steps": completed, "total_steps": len(steps),
                  "critical_blocked": [x for x in confirmations.values() if x["status"] == "blocked"],
                  "resource_ready": True, "resource_regions": readiness["regions"]}
        with self.conn:
            cur = self.conn.execute("INSERT INTO published_status(outage_id,plan_id,version,status_json,created_at) VALUES(?,?,?,?,?)",
                                    (outage_id, plan_id, plan["version"], j(status), now()))
            if status["state"] == "restored": self.conn.execute("UPDATE outages SET state='restored',revision=revision+1,updated_at=? WHERE id=?", (now(), outage_id))
            self.store.audit(actor, "status.publish", "outage", outage_id, {"plan_id": plan_id, "state": status["state"]})
        return {"id": cur.lastrowid, "status": status}

    def _validate_steps(self, steps: list[dict]) -> list[dict]:
        if not steps: raise ApiError(400, "恢复计划至少需要一个步骤")
        normalized = []; seqs = set()
        for raw in steps:
            try:
                seq, asset_code, required = int(raw["seq"]), str(raw["asset"]), float(raw.get("required_mw", 0))
            except (KeyError, ValueError, TypeError) as exc: raise ApiError(400, "步骤字段不完整") from exc
            if seq in seqs or required <= 0: raise ApiError(400, "步骤序号重复或容量不合法")
            asset = self.conn.execute("SELECT * FROM assets WHERE code=?", (asset_code,)).fetchone()
            if not asset: raise ApiError(400, f"步骤资产不存在：{asset_code}")
            if required > float(asset["capacity_mw"]): raise ApiError(409, f"步骤 {seq} 超过资产安全容量")
            deps = [int(x) for x in raw.get("depends_on", [])]
            if any(dep >= seq for dep in deps): raise ApiError(400, "依赖步骤必须位于当前步骤之前")
            seqs.add(seq); normalized.append({"seq": seq, "action": str(raw.get("action", "energize")), "asset": asset_code,
                                                  "required_mw": required, "depends_on": deps, "critical": bool(raw.get("critical", False))})
        available = {row["seq"]: set(row["depends_on"]) for row in normalized}
        for seq, deps in available.items():
            if not deps.issubset(seqs): raise ApiError(400, f"步骤 {seq} 含有未知依赖")
        return sorted(normalized, key=lambda x: x["seq"])

    def _validate_safety(self, plan: sqlite3.Row) -> None:
        for step in json.loads(plan["steps_json"]):
            for dependency in step.get("depends_on", []):
                if int(dependency) >= int(step["seq"]): raise ApiError(409, "计划依赖顺序不安全")

    def _copy_confirmations(self, old_plan_id: int, new_plan_id: int, steps: list[dict], confirmed: dict[int, sqlite3.Row]) -> None:
        for step in steps:
            seq = int(step["seq"])
            if seq in confirmed:
                old = confirmed[seq]
                self.conn.execute("INSERT INTO confirmations(plan_id,step_no,status,confirmed_by,confirmed_at,note) VALUES(?,?,?,?,?,?)",
                                  (new_plan_id, seq, old["status"], old["confirmed_by"], old["confirmed_at"], old["note"]))

    def _plan_update(self, plan: sqlite3.Row, state: str, expected_revision: int, actor: str, action: str, details: dict) -> None:
        if int(expected_revision) != int(plan["revision"]): raise ApiError(409, "计划版本冲突")
        with self.conn:
            cur = self.conn.execute("UPDATE plans SET state=?,revision=revision+1 WHERE id=? AND revision=?", (state, plan["id"], expected_revision))
            if cur.rowcount != 1: raise ApiError(409, "并发计划更新冲突")
            self.store.audit(actor, action, "plan", plan["id"], details)

    def _copy_assignments(self, old_plan_id: int, new_plan_id: int, steps: list[dict]) -> int:
        kept_seqs = {int(step["seq"]) for step in steps}
        rows = self.conn.execute("SELECT * FROM resource_assignments WHERE plan_id=? AND state='allocated'", (old_plan_id,)).fetchall()
        count = 0
        for row in rows:
            if int(row["step_no"]) not in kept_seqs: continue
            self.conn.execute("""INSERT INTO resource_assignments(plan_id,step_no,resource_id,contact_name,contact_phone,
                                 available_from,available_to,priority,state,created_at,updated_at)
                                 VALUES(?,?,?,?,?,?,?,?,'allocated',?,?)""",
                              (new_plan_id, row["step_no"], row["resource_id"], row["contact_name"], row["contact_phone"],
                               row["available_from"], row["available_to"], row["priority"], now(), now()))
            count += 1
        return count

    @staticmethod
    def _parse_ts(value: str, label: str) -> datetime:
        text = str(value or "").strip()
        if not text: raise ApiError(400, f"{label or '时间'}不能为空")
        try:
            if text.endswith("Z"): text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
        except ValueError as exc: raise ApiError(400, f"{label or '时间'}格式应为 ISO 8601（如 2026-09-25T08:00:00Z）") from exc
        if parsed.tzinfo is None: parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _fmt_ts(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="minutes").replace("+00:00", "Z")

    def _power_resource_dict(self, row: sqlite3.Row) -> dict:
        used = self.conn.execute("""SELECT COUNT(*) FROM resource_assignments a JOIN plans p ON a.plan_id=p.id
                                    WHERE a.resource_id=? AND p.state!='superseded' AND a.state='allocated'""", (row["id"],)).fetchone()[0]
        return {"id": row["id"], "code": row["code"], "name": row["name"], "resource_type": row["resource_type"],
                "capacity_mw": row["capacity_mw"], "owner": row["owner"], "region": row["region"],
                "status": row["status"], "allocated_steps": used}

    def _assignment_dict(self, row: sqlite3.Row) -> dict:
        resource = self.conn.execute("SELECT * FROM power_resources WHERE id=?", (row["resource_id"],)).fetchone()
        item = {"id": row["id"], "plan_id": row["plan_id"], "step_no": row["step_no"],
                "resource_id": row["resource_id"], "resource_code": resource["code"] if resource else None,
                "resource_name": resource["name"] if resource else None,
                "contact_name": row["contact_name"], "contact_phone": row["contact_phone"],
                "available_from": row["available_from"], "available_to": row["available_to"],
                "priority": row["priority"], "state": row["state"]}
        if row["state"] == "displaced":
            occ = self.conn.execute("SELECT * FROM resource_assignments WHERE plan_id=? AND step_no=?",
                                    (row["occupied_by_plan_id"], row["occupied_by_step_no"])).fetchone()
            item["occupied_by"] = None if occ is None else {"plan_id": occ["plan_id"], "step_no": occ["step_no"],
                                                            "resource_id": occ["resource_id"],
                                                            "window": {"from": occ["available_from"], "to": occ["available_to"]}}
            item["reassignable_windows"] = self._free_windows(row) if occ is not None else []
        return item

    def _free_windows(self, displaced: sqlite3.Row) -> list[dict]:
        """被挤掉的排定：在其申请时段内，列出该电源仍可改派的空档。"""
        want_from, want_to = self._parse_ts(displaced["available_from"], ""), self._parse_ts(displaced["available_to"], "")
        blockers = []
        for other in self.conn.execute("""SELECT a.* FROM resource_assignments a JOIN plans p ON a.plan_id=p.id
                                         WHERE a.resource_id=? AND p.state!='superseded' AND a.state='allocated' AND a.id!=?""",
                                       (displaced["resource_id"], displaced["id"])):
            o_from, o_to = self._parse_ts(other["available_from"], ""), self._parse_ts(other["available_to"], "")
            if want_from < o_to and o_from < want_to:
                blockers.append((max(want_from, o_from), min(want_to, o_to),
                                 {"plan_id": other["plan_id"], "step_no": other["step_no"]}))
        blockers.sort(key=lambda x: x[0])
        windows, cursor = [], want_from
        for b_from, b_to, _meta in blockers:
            if b_from > cursor: windows.append({"from": self._fmt_ts(cursor), "to": self._fmt_ts(b_from)})
            if b_to > cursor: cursor = b_to
        if cursor < want_to: windows.append({"from": self._fmt_ts(cursor), "to": self._fmt_ts(want_to)})
        return windows


        if int(expected_revision) != int(plan["revision"]): raise ApiError(409, "计划版本冲突")
        with self.conn:
            cur = self.conn.execute("UPDATE plans SET state=?,revision=revision+1 WHERE id=? AND revision=?", (state, plan["id"], expected_revision))
            if cur.rowcount != 1: raise ApiError(409, "并发计划更新冲突")
            self.store.audit(actor, action, "plan", plan["id"], details)

    def plan_detail(self, plan_id: int) -> dict:
        plan = self._plan_dict(self._row("plans", plan_id))
        return {"plan": plan, "confirmations": [dict(row) for row in self.conn.execute("SELECT * FROM confirmations WHERE plan_id=? ORDER BY step_no", (plan_id,))],
                "assignments": [self._assignment_dict(row) for row in self.conn.execute("SELECT * FROM resource_assignments WHERE plan_id=? ORDER BY step_no", (plan_id,))],
                "field_reports": [dict(row) for row in self.conn.execute("SELECT * FROM field_reports WHERE plan_id=? ORDER BY id", (plan_id,))]}

    def _outage_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "incident_code": row["incident_code"], "title": row["title"], "state": row["state"],
                "affected_regions": json.loads(row["affected_regions_json"]), "revision": row["revision"]}

    def _plan_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "outage_id": row["outage_id"], "version": row["version"], "state": row["state"],
                "steps": json.loads(row["steps_json"]), "revision": row["revision"]}

    def state(self) -> dict:
        outages = [self._outage_dict(row) for row in self.conn.execute("SELECT * FROM outages ORDER BY id DESC")]
        for outage in outages:
            latest = self.conn.execute("SELECT id FROM plans WHERE outage_id=? ORDER BY version DESC LIMIT 1", (outage["id"],)).fetchone()
            outage["resource_ready"] = None if latest is None else self.resource_readiness(outage["id"], latest["id"])["ready"]
        return {"assets": [dict(row) for row in self.conn.execute("SELECT * FROM assets ORDER BY id")],
                "facilities": [dict(row) for row in self.conn.execute("SELECT * FROM facilities ORDER BY priority,id")],
                "power_resources": [self._power_resource_dict(row) for row in self.conn.execute("SELECT * FROM power_resources ORDER BY id")],
                "outages": outages,
                "plans": [self._plan_dict(row) for row in self.conn.execute("SELECT * FROM plans ORDER BY id DESC")],
                "assignments": [self._assignment_dict(row) for row in self.conn.execute("SELECT * FROM resource_assignments ORDER BY id")],
                "telemetry_anomalies": [dict(row) for row in self.conn.execute("SELECT * FROM telemetry WHERE valid=0 ORDER BY id DESC LIMIT 20")],
                "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM assets LIMIT 1").fetchone():
            a = self.register_asset("dispatcher-demo", "dispatcher", "SUB-1", "中心站", "substation", 200, "城区")
            self.register_asset("dispatcher-demo", "dispatcher", "LINE-1", "一号线", "line", 120, "城区", a["id"])
            self.register_facility("dispatcher-demo", "dispatcher", "市医院", "hospital", a["id"], 1, 50)
            self.register_power_resource("dispatcher-demo", "dispatcher", "GEN-M1", "移动发电车1号", "mobile_generator", 100,
                                         "应急保障中心", "城区", "王工", "13800000001")


class Handler(BaseHTTPRequestHandler):
    service: GridService

    def log_message(self, fmt: str, *args: object) -> None: sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))
    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def _send_error(self, exc: ApiError) -> None:
        body = {"error": exc.message}
        if exc.details: body["details"] = exc.details
        self._send(exc.status, body)
    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try: return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc: raise ApiError(400, "JSON 请求体无效") from exc
    def _parts(self) -> list[str]: return [p for p in urlparse(self.path).path.strip("/").split("/") if p]

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]): out = {"status": "ok"}
            elif p == ["api", "state"]: out = self.service.state()
            elif len(p) == 3 and p[:2] == ["api", "plans"]: out = self.service.plan_detail(int(p[2]))
            elif p == ["api", "resources"]:
                out = {"resources": [self.service._power_resource_dict(row)
                                     for row in self.service.conn.execute("SELECT * FROM power_resources ORDER BY id")]}
            elif len(p) == 3 and p[:2] == ["api", "outages"] and p[2].isdigit():
                out = self.service.resource_readiness(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(page))); self.end_headers(); self.wfile.write(page); return
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send_error(exc)
        except Exception as exc: self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, b = self._parts(), self._body(); actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role")
            if p == ["api", "assets"]: out = self.service.register_asset(actor, role, b.get("code", ""), b.get("name", ""), b.get("asset_type", "line"), float(b.get("capacity_mw", 0)), b.get("region", ""), b.get("parent_id"))
            elif p == ["api", "facilities"]: out = self.service.register_facility(actor, role, b.get("name", ""), b.get("facility_type", "hospital"), int(b.get("asset_id", 0)), int(b.get("priority", 1)), float(b.get("backup_power_mw", 0)))
            elif p == ["api", "resources"]: out = self.service.register_power_resource(actor, role, b.get("code", ""), b.get("name", ""), b.get("resource_type", "mobile_generator"), float(b.get("capacity_mw", 0)), b.get("owner", ""), b.get("region", ""), b.get("contact", ""), b.get("contact_phone", ""))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "assign-resource": out = self.service.assign_resource(actor, role, int(p[2]), int(b.get("step_no", 0)), int(b.get("resource_id", 0)), b.get("contact_name", ""), b.get("available_from", ""), b.get("available_to", ""), int(b.get("priority", 2)), int(b.get("expected_revision", -1)), b.get("contact_phone", ""))
            elif p == ["api", "outages"]: out = self.service.create_outage(actor, role, b.get("incident_code", ""), b.get("title", ""), b.get("affected_regions", []))
            elif p == ["api", "telemetry"]: out = self.service.record_telemetry(actor, role, int(b.get("asset_id", 0)), float(b.get("load_mw", 0)), float(b.get("voltage_kv", 0)), b.get("timestamp", ""))
            elif p == ["api", "plans"]: out = self.service.create_plan(actor, role, int(b.get("outage_id", 0)), b.get("steps", []))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "submit": out = self.service.submit_plan(actor, role, int(p[2]), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "approve": out = self.service.approve_plan(actor, role, int(p[2]), int(b.get("expected_revision", -1)), b.get("note", ""))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "activate": out = self.service.activate_plan(actor, role, int(p[2]), int(b.get("expected_revision", -1)))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "change": out = self.service.make_plan_change(actor, role, int(p[2]), b.get("steps", []), int(b.get("expected_revision", -1)))
            elif p == ["api", "field-reports"]: out = self.service.field_report(actor, role, int(b.get("plan_id", 0)), int(b.get("step_no", 0)), b.get("client_report_id", ""), int(b.get("expected_plan_version", 0)), b.get("status", ""), b.get("note", ""))
            elif len(p) == 4 and p[:2] == ["api", "plans"] and p[3] == "confirm": out = self.service.confirm_step(actor, role, int(p[2]), int(b.get("step_no", 0)), b.get("decision", "confirmed"), b.get("note", ""))
            elif p == ["api", "status"]: out = self.service.publish_status(actor, role, int(b.get("outage_id", 0)), int(b.get("plan_id", 0)))
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send_error(exc)
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc: self._send(400, {"error": str(exc)})
        except Exception as exc: self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path); service = GridService(store)
    if seed: service.seed()
    Handler.service = service
    print(f"grid restoration listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--port", type=int, default=8215); parser.add_argument("--db", default=str(DB_PATH)); parser.add_argument("--init", action="store_true"); parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init: Store(args.db).close()
    if args.seed or not args.init: run(args.port, args.db, args.seed)


if __name__ == "__main__": main()
