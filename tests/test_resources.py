import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, GridService, Store

WIDE = ("2000-01-01T00:00:00Z", "2099-01-01T00:00:00Z")


class ResourceSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.s = GridService(Store(Path(self.tmp.name) / "g.db"))
        self.sub = self.s.register_asset("dispatcher", "dispatcher", "SUB", "中心站", "substation", 200, "A")
        self.line = self.s.register_asset("dispatcher", "dispatcher", "LINE", "线路", "line", 100, "A", self.sub["id"])
        self.s.register_facility("dispatcher", "dispatcher", "医院", "hospital", self.sub["id"], 1, 50)

    def tearDown(self): self.s.store.close(); self.tmp.cleanup()

    def _active_plan(self, code="OUT-R"):
        outage = self.s.create_outage("dispatcher", "dispatcher", code, "线路跳闸", ["A"])
        plan = self.s.create_plan("dispatcher", "dispatcher", outage["id"], [
            {"seq": 1, "action": "检查", "asset": "SUB", "required_mw": 80, "critical": True},
            {"seq": 2, "action": "送电", "asset": "LINE", "required_mw": 70, "depends_on": [1], "critical": True}])
        plan = self.s.submit_plan("dispatcher", "dispatcher", plan["id"], plan["revision"])
        plan = self.s.approve_plan("dispatcher", "dispatcher", plan["id"], plan["revision"], "安全校核通过")
        return outage, self.s.activate_plan("dispatcher", "dispatcher", plan["id"], plan["revision"])

    def _gen(self, code="GEN-1", capacity=200.0):
        return self.s.register_power_resource("dispatcher", "dispatcher", code, f"发电车{code}",
                                              "mobile_generator", capacity, "应急中心", "A", "王工", "13800000001")

    def _assign(self, plan, step, resource, prio, frm, to, rev=None):
        result = self.s.assign_resource("dispatcher", "dispatcher", plan["id"], step, resource["id"], "王工",
                                        frm, to, prio, plan["revision"] if rev is None else rev)
        plan["revision"] = result["plan_revision"]
        return result

    def test_overlap_keeps_higher_priority_and_shows_reassign_window(self):
        _outage, plan = self._active_plan()
        gen = self._gen()
        self._assign(plan, 1, gen, 3, "2026-09-25T10:00:00Z", "2026-09-25T12:00:00Z")
        self._assign(plan, 2, gen, 1, "2026-09-25T11:00:00Z", "2026-09-25T13:00:00Z")
        detail = self.s.plan_detail(plan["id"])
        by_step = {a["step_no"]: a for a in detail["assignments"]}
        self.assertEqual("allocated", by_step[2]["state"])
        displaced = by_step[1]
        self.assertEqual("displaced", displaced["state"])
        self.assertEqual({"plan_id": plan["id"], "step_no": 2},
                         {k: displaced["occupied_by"][k] for k in ("plan_id", "step_no")})
        self.assertEqual([{"from": "2026-09-25T10:00Z", "to": "2026-09-25T11:00Z"}],
                         displaced["reassignable_windows"])

    def test_confirmed_step_cannot_swap_power(self):
        _outage, plan = self._active_plan("OUT-R2")
        gen1, gen2 = self._gen("GEN-1"), self._gen("GEN-2")
        self._assign(plan, 1, gen1, 2, *WIDE)
        self.s.field_report("field", "field", plan["id"], 1, "r-1", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        with self.assertRaises(ApiError) as ctx:
            self._assign(plan, 1, gen2, 2, *WIDE)
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("已确认", ctx.exception.message)

    def test_stale_reversion_prompts_reload(self):
        _outage, plan = self._active_plan("OUT-R3")
        gen = self._gen("GEN-3")
        stale = plan["revision"]
        self._assign(plan, 1, gen, 2, *WIDE)
        with self.assertRaises(ApiError) as ctx:
            self._assign(plan, 2, gen, 2, *WIDE, rev=stale)
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("重新加载", ctx.exception.message)
        self.assertEqual(stale + 1, ctx.exception.details["current_revision"])

    def test_readiness_blocks_publish_then_allows_with_reasons(self):
        outage, plan = self._active_plan("OUT-R4")
        readiness = self.s.resource_readiness(outage["id"], plan["id"])
        self.assertFalse(readiness["ready"])
        self.assertIn("未指定备用电源", readiness["regions"][0]["block_reasons"][0])
        with self.assertRaises(ApiError) as ctx:
            self.s.publish_status("dispatcher", "dispatcher", outage["id"], plan["id"])
        self.assertEqual(409, ctx.exception.status)
        self.assertEqual("A", ctx.exception.details["blocked_regions"][0]["region"])
        gen1, gen2 = self._gen("GEN-A"), self._gen("GEN-B")
        self._assign(plan, 1, gen1, 2, *WIDE)
        self._assign(plan, 2, gen2, 2, *WIDE)
        self.assertTrue(self.s.resource_readiness(outage["id"], plan["id"])["ready"])
        self.s.field_report("field", "field", plan["id"], 1, "a", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        self.s.field_report("field", "field", plan["id"], 2, "b", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 2, "confirmed")
        published = self.s.publish_status("dispatcher", "dispatcher", outage["id"], plan["id"])
        self.assertEqual("restored", published["status"]["state"])
        self.assertTrue(published["status"]["resource_ready"])

    def test_capacity_shortage_and_window_outside_availability(self):
        outage, plan = self._active_plan("OUT-R5")
        small = self._gen("GEN-S", capacity=50)
        with self.assertRaises(ApiError):
            self._assign(plan, 1, small, 2, *WIDE)
        big = self._gen("GEN-B", capacity=200)
        self._assign(plan, 1, big, 2, "2000-01-01T00:00:00Z", "2001-01-01T00:00:00Z")
        readiness = self.s.resource_readiness(outage["id"], plan["id"])
        self.assertFalse(readiness["ready"])
        self.assertTrue(any("不在电源可用时段内" in r for r in readiness["regions"][0]["block_reasons"]))

    def test_plan_change_carries_allocations(self):
        outage, plan = self._active_plan("OUT-R6")
        gen1, gen2 = self._gen("GEN-C1"), self._gen("GEN-C2")
        self._assign(plan, 1, gen1, 2, *WIDE)
        self._assign(plan, 2, gen2, 2, *WIDE)
        self.s.field_report("field", "field", plan["id"], 1, "c-1", plan["version"], "completed")
        self.s.confirm_step("dispatcher", "dispatcher", plan["id"], 1, "confirmed")
        plan2 = self.s.make_plan_change("dispatcher", "dispatcher", plan["id"], [
            {"seq": 1, "action": "检查", "asset": "SUB", "required_mw": 80, "critical": True},
            {"seq": 2, "action": "送电", "asset": "LINE", "required_mw": 70, "depends_on": [1], "critical": True}],
            plan["revision"])
        detail = self.s.plan_detail(plan2["id"])
        self.assertEqual([1, 2], sorted(a["step_no"] for a in detail["assignments"]))
        self.assertTrue(all(a["state"] == "allocated" for a in detail["assignments"]))


if __name__ == "__main__": unittest.main()
