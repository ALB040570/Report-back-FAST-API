import copy
import unittest

from app.services.pushdown import (
    PushdownConfig,
    PushdownFilter,
    PushdownPaging,
    PushdownPathError,
    build_body_with_pushdown,
    set_by_dot_path,
)


class PushdownUnitTests(unittest.TestCase):
    def test_set_by_dot_path_dict(self) -> None:
        body = {"params": [{}]}
        set_by_dot_path(body, "params.0.limit", 10)
        self.assertEqual(body["params"][0]["limit"], 10)

    def test_set_by_dot_path_list_expand(self) -> None:
        body = {"params": []}
        set_by_dot_path(body, "params.0.objLocation", 1069)
        self.assertEqual(body["params"][0]["objLocation"], 1069)

    def test_set_by_dot_path_error(self) -> None:
        body = {"params": {}}
        with self.assertRaises(PushdownPathError):
            set_by_dot_path(body, "params.0.limit", 1)

    def test_build_body_with_pushdown_eq(self) -> None:
        original = {"params": [{}]}
        cfg = PushdownConfig(
            enabled=True,
            mode="jsonrpc_params",
            paging=None,
            filters=[PushdownFilter(filter_key="objLocation", op="eq", target_path="body.params.0.objLocation")],
        )
        filters = {"globalFilters": {"objLocation": {"values": [1069]}}}
        original_copy = copy.deepcopy(original)
        body, applied, paging = build_body_with_pushdown(original, filters, cfg, None)
        self.assertEqual(applied, 1)
        self.assertFalse(paging)
        self.assertEqual(body["params"][0]["objLocation"], 1069)
        self.assertEqual(original, original_copy)

    def test_build_body_with_pushdown_in_and_range(self) -> None:
        original = {"params": [{}]}
        cfg = PushdownConfig(
            enabled=True,
            mode="jsonrpc_params",
            paging=PushdownPaging(
                strategy="offset",
                limit_path="body.params.0.limit",
                offset_path="body.params.0.offset",
            ),
            filters=[
                PushdownFilter(filter_key="status", op="in", target_path="body.params.0.status"),
                PushdownFilter(filter_key="date", op="range", target_path="body.params.0.date"),
            ],
        )
        filters = {
            "globalFilters": {
                "status": {"values": ["A", "B"]},
                "date": {"range": {"from": "2025-01-01", "to": "2025-02-01"}},
            }
        }
        body, applied, paging = build_body_with_pushdown(
            original,
            filters,
            cfg,
            {"limit": 5, "offset": 10},
        )
        self.assertEqual(applied, 2)
        self.assertTrue(paging)
        self.assertEqual(body["params"][0]["status"], ["A", "B"])
        self.assertEqual(body["params"][0]["date"], {"from": "2025-01-01", "to": "2025-02-01"})
        self.assertEqual(body["params"][0]["limit"], 5)
        self.assertEqual(body["params"][0]["offset"], 10)

    def test_build_body_with_pushdown_safe_only_skips_join_fields(self) -> None:
        original = {"params": [{}]}
        cfg = PushdownConfig(
            enabled=True,
            mode="jsonrpc_params",
            paging=None,
            filters=[PushdownFilter(filter_key="OBJ.nameSection", op="eq", target_path="body.params.0.obj")],
        )
        filters = {"globalFilters": {"OBJ.nameSection": {"values": ["X"]}}}
        body, applied, _ = build_body_with_pushdown(original, filters, cfg, None, safe_only=True)
        self.assertEqual(applied, 0)
        self.assertNotIn("obj", body["params"][0])
