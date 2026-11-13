import os
import unittest

import yaml

ROOT = os.path.join(os.path.dirname(__file__), "..")


class TestBundle(unittest.TestCase):
    def test_bundle_parses_and_wiring_is_consistent(self):
        b = yaml.safe_load(open(os.path.join(ROOT, "databricks.yml")))
        job = b["resources"]["jobs"]["wf_daily_master"]
        keys = {t["task_key"] for t in job["tasks"]}
        for t in job["tasks"]:
            for d in t.get("depends_on", []):
                self.assertIn(d["task_key"], keys)
        self.assertEqual(job["email_notifications"]["on_failure"], ["${var.alert_email}"])
        for p in b["resources"]["pipelines"].values():
            self.assertFalse(p.get("continuous", False))
            for lib in p["libraries"]:
                self.assertTrue(os.path.exists(os.path.join(ROOT, lib["file"]["path"])), lib["file"]["path"])


if __name__ == "__main__":
    unittest.main()
