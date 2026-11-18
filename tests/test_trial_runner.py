from datetime import datetime, date, time, timedelta
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

from scripts.run_runner_trial import compute_future_slots


class TestTrialRunner(unittest.TestCase):
    def setUp(self):
        self.tz = ZoneInfo("Asia/Ho_Chi_Minh")

    def test_compute_future_slots_rounds_forward(self):
        """Slots must be spaced by exactly 5m and rounded forward, preserving safety delay."""
        # 14:02 + 12m delay = 14:14 -> rounded forward to 14:15
        now_vn = datetime(2026, 11, 18, 14, 2, 30, tzinfo=self.tz)
        target_date, slots_str, slots_dt = compute_future_slots(now_vn, delay_minutes=12)

        self.assertEqual(target_date, date(2026, 11, 18))
        self.assertEqual(slots_str, ["14:15", "14:20", "14:25"])
        self.assertTrue(slots_dt[0] < slots_dt[1] < slots_dt[2])

        # Verify delay is at least 12 minutes
        delay_actual = (slots_dt[0] - now_vn).total_seconds()
        self.assertGreaterEqual(delay_actual, 12 * 60)

    def test_midnight_crossing_shifts_to_tomorrow(self):
        """If slots would cross midnight, push to 00:15 tomorrow to keep all 3 in same date."""
        # 23:42 + 10m delay = 23:52 -> slot 3 would be 00:02 (crossing midnight)
        now_vn = datetime(2026, 11, 18, 23, 42, 0, tzinfo=self.tz)
        target_date, slots_str, slots_dt = compute_future_slots(now_vn, delay_minutes=10)

        # Shifted to tomorrow
        self.assertEqual(target_date, date(2026, 11, 19))
        self.assertEqual(slots_str, ["00:15", "00:20", "00:25"])
        self.assertEqual(slots_dt[0].date(), date(2026, 11, 19))
        self.assertEqual(slots_dt[1].date(), date(2026, 11, 19))
        self.assertEqual(slots_dt[2].date(), date(2026, 11, 19))


if __name__ == "__main__":
    unittest.main()
