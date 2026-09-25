import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, GridService, Store


class ResourceSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.s = GridService(Store(Path(self.tmp.name) / "g.db"))
        self.s.register_asset("dispatcher", "dispatcher", "SUB", "中心站", "substation", 200, "A")
        self.s.register_asset("dispatcher", "dispatcher", "LINE", "线路", "line", 100, "A")
        self.s.register_asset("dispatcher", "dispatcher", "LINE-B", "二号线", "line", 80, "B")
        self.gen = self.s.register_power_source("dispatcher", "dispatcher", "GEN", "移动发电车", "mobile_generator", 100, "A", "王队 13800000001", "应急中心")
        self.small = self.s.register_power_source("dispatcher", "dispatcher", "GEN-S", "小型发电机", "diesel_generator", 60, "B", "李班 13800000002", "城北")

    def tearDown(self): self.s.store.close(); self.tmp.cleanup()

    def _active_plan(self, code="OUT-R", regions=None):
        regions = regions or ["A", "B"]
        outage = self.s.create_outage("dispatcher", "dispatcher", code, "线路跳闸", regions)
        plan = self.s.create_plan("dispatcher", "dispatcher", outage["id"], [
            {"seq": 1, "action": "检查", "asset": "SUB", "required_mw": 60, "critical": True},
            {"seq": 2, "action": "送电", "asset": "LINE", "required_mw": 70, "depends_on": [1], "critical": True},
            {"seq": 3, "action": "转供", "asset": "LINE-B", "required_mw": 50, "depends_on": [1]}])
        plan = self.s.submit_plan("dispatcher", "dispatcher", plan["id"], plan["revision"])
        plan = self.s.approve_plan("dispatcher", "dispatcher", plan["id"], plan["revision"])
        return outage, self.s.activate_plan("dispatcher", "dispatcher", plan["id"], plan["revision"])

    def _assign(self, plan_id, step_no, source="GEN", start="2026-09-25T08:00:00Z", end="2026-09-25T09:00:00Z", priority=2, rev=None):
        if rev is None: rev = self.s.plan_detail(plan_id)["plan"]["resource_revision"]
        return self.s.assign_power("dispatcher", "dispatcher", plan_id, step_no, source, "王队", start, end, priority, rev)

    def test_register_source_and_board_ready(self):
        outage, plan = self._active_plan("OUT-R1")
        board = self.s.resource_status(outage["id"])
        self.assertFalse(board["resources_ready"])
        missing = {r["region"]: r["missing_power_steps"] for r in board["regions"]}
        self.assertEqual([1, 2], missing["A"])
        self.assertEqual([3], missing["B"])
        self._assign(plan["id"], 1)
        self._assign(plan["id"], 2, start="2026-09-25T09:00:00Z", end="2026-09-25T10:00:00Z")
        self._assign(plan["id"], 3, source="GEN-S")
        board = self.s.resource_status(outage["id"])
        self.assertTrue(board["resources_ready"])
        self.assertTrue(all(r["ready"] for r in board["regions"]))

    def test_overlap_keeps_higher_priority_and_shows_holder_and_slots(self):
        outage, p1 = self._active_plan("OUT-R2")
        low = self._assign(p1["id"], 1, start="2026-09-25T08:00:00Z", end="2026-09-25T12:00:00Z", priority=1)["assignment"]
        self.assertEqual("active", low["status"])
        high = self._assign(p1["id"], 2, start="2026-09-25T09:00:00Z", end="2026-09-25T10:00:00Z", priority=3)["assignment"]
        self.assertEqual("active", high["status"])
        detail = self.s.plan_detail(p1["id"])
        low_now = next(a for a in detail["assignments"] if a["step_no"] == 1)
        self.assertEqual("displaced", low_now["status"])
        self.assertEqual(p1["version"], low_now["occupied_by"]["plan_version"])
        self.assertEqual(2, low_now["occupied_by"]["step_no"])
        self.assertEqual([{"from": "2026-09-25T08:00:00Z", "to": "2026-09-25T09:00:00Z"},
                          {"from": "2026-09-25T10:00:00Z", "to": "2026-09-25T12:00:00Z"}], low_now["reassignable_slots"])

    def test_confirmed_step_cannot_be_reassigned_or_unassigned(self):
        outage, plan = self._active_plan("OUT-R3")
        self._assign(plan["id"], 1)
        self.s.field_report("field", "field", plan["id"], 1, "r3-1", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        rev = self.s.plan_detail(plan["id"])["plan"]["resource_revision"]
        with self.assertRaises(ApiError) as ctx:
            self._assign(plan["id"], 1, source="GEN", priority=3, rev=rev)
        self.assertIn("已确认", ctx.exception.message)
        with self.assertRaises(ApiError):
            self.s.unassign_power("dispatcher", "dispatcher", plan["id"], 1, rev)

    def test_stale_resource_reversion_requires_reload(self):
        outage, plan = self._active_plan("OUT-R4")
        self._assign(plan["id"], 1)
        with self.assertRaises(ApiError) as ctx:
            self._assign(plan["id"], 2, rev=1)
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("重新加载", ctx.exception.message)

    def test_unassign_releases_source_and_revives_displaced(self):
        outage, plan = self._active_plan("OUT-R5")
        self._assign(plan["id"], 1, start="2026-09-25T08:00:00Z", end="2026-09-25T10:00:00Z", priority=1)
        self._assign(plan["id"], 2, start="2026-09-25T09:00:00Z", end="2026-09-25T11:00:00Z", priority=3)
        detail = self.s.plan_detail(plan["id"])
        self.assertEqual("displaced", next(a for a in detail["assignments"] if a["step_no"] == 1)["status"])
        rev = detail["plan"]["resource_revision"]
        self.s.unassign_power("dispatcher", "dispatcher", plan["id"], 2, rev)
        detail = self.s.plan_detail(plan["id"])
        self.assertEqual("active", next(a for a in detail["assignments"] if a["step_no"] == 1)["status"])

    def test_capacity_shortage_rejected(self):
        outage, plan = self._active_plan("OUT-R6")
        with self.assertRaises(ApiError) as ctx:
            self._assign(plan["id"], 2, source="GEN-S")
        self.assertEqual(409, ctx.exception.status)

    def test_plan_change_carries_assignments(self):
        outage, plan = self._active_plan("OUT-R7")
        self._assign(plan["id"], 1)
        self._assign(plan["id"], 2, start="2026-09-25T09:00:00Z", end="2026-09-25T10:00:00Z")
        self.s.field_report("field", "field", plan["id"], 1, "r7-1", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        changed = self.s.make_plan_change("dispatcher", "dispatcher", plan["id"], [
            {"seq": 1, "action": "检查", "asset": "SUB", "required_mw": 60, "critical": True},
            {"seq": 2, "action": "送电", "asset": "LINE", "required_mw": 70, "depends_on": [1], "critical": True},
            {"seq": 3, "action": "转供", "asset": "LINE-B", "required_mw": 50, "depends_on": [1]},
            {"seq": 4, "action": "巡视", "asset": "SUB", "required_mw": 20, "depends_on": [2]}], plan["revision"])
        seqs = {a["step_no"] for a in self.s.plan_detail(changed["id"])["assignments"]}
        self.assertEqual({1, 2}, seqs)

    def test_publish_blocked_with_reasons_then_restored(self):
        outage, plan = self._active_plan("OUT-R8")
        published = self.s.publish_status("dispatcher", "dispatcher", outage["id"], plan["id"])["status"]
        self.assertEqual("blocked", published["state"])
        self.assertFalse(published["resources_ready"])
        joined = " ".join(published["resource_blocking_reasons"])
        self.assertIn("尚未指定电源", joined)
        self._assign(plan["id"], 1)
        self._assign(plan["id"], 2, start="2026-09-25T09:00:00Z", end="2026-09-25T10:00:00Z")
        self._assign(plan["id"], 3, source="GEN-S")
        self.s.field_report("field", "field", plan["id"], 1, "r8-1", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        self.s.field_report("field", "field", plan["id"], 2, "r8-2", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 2, "confirmed")
        self.s.field_report("field", "field", plan["id"], 3, "r8-3", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 3, "confirmed")
        published = self.s.publish_status("dispatcher", "dispatcher", outage["id"], plan["id"])["status"]
        self.assertTrue(published["resources_ready"])
        self.assertEqual("restored", published["state"])

    def test_region_without_steps_is_blocking(self):
        outage = self.s.create_outage("dispatcher", "dispatcher", "OUT-R9", "局部停运", ["A", "B"])
        self.s.create_plan("dispatcher", "dispatcher", outage["id"], [
            {"seq": 1, "action": "检查", "asset": "SUB", "required_mw": 60}])
        board = self.s.resource_status(outage["id"])
        self.assertFalse(board["resources_ready"])
        region_b = next(r for r in board["regions"] if r["region"] == "B")
        self.assertIn("缺少恢复步骤", " ".join(region_b["blocking_reasons"]))

    def test_cross_plan_overlap(self):
        o1, p1 = self._active_plan("OUT-R10")
        self._assign(p1["id"], 1, start="2026-09-25T08:00:00Z", end="2026-09-25T12:00:00Z", priority=3)
        o2 = self.s.create_outage("dispatcher", "dispatcher", "OUT-R10B", "另一处停运", ["B"])
        p2 = self.s.create_plan("dispatcher", "dispatcher", o2["id"], [
            {"seq": 1, "action": "转供", "asset": "LINE-B", "required_mw": 50}])
        p2 = self.s.submit_plan("dispatcher", "dispatcher", p2["id"], p2["revision"])
        p2 = self.s.approve_plan("dispatcher", "dispatcher", p2["id"], p2["revision"])
        p2 = self.s.activate_plan("dispatcher", "dispatcher", p2["id"], p2["revision"])
        res = self._assign(p2["id"], 1, source="GEN", start="2026-09-25T09:00:00Z", end="2026-09-25T10:00:00Z", priority=1)
        self.assertEqual("displaced", res["assignment"]["status"])
        self.assertEqual(p1["id"], res["assignment"]["occupied_by"]["plan_id"])
        self.assertEqual(1, res["assignment"]["occupied_by"]["step_no"])


if __name__ == "__main__": unittest.main()
