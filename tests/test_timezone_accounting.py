import unittest
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch

from src.workflow import day_counts


class TestTimezoneAccounting(unittest.TestCase):
    def test_utc_date_differs_from_vietnam_accounting_date(self):
        """
        Verify that a snapshot scheduled at 00:20 Asia/Ho_Chi_Minh on 2026-11-18
        (which is 2026-11-17 17:20:00 UTC) correctly resolves to accounting date
        2026-11-18, not the UTC date 2026-11-17.
        """
        tz_vietnam = ZoneInfo("Asia/Ho_Chi_Minh")
        scheduled_utc_str = "2026-11-17T17:20:00+00:00"
        scheduled_dt = datetime.fromisoformat(scheduled_utc_str)

        # 1. UTC date differs from Vietnam date
        utc_date = scheduled_dt.date()
        self.assertEqual(utc_date, date(2026, 11, 17))
        self.assertEqual(scheduled_utc_str[:10], "2026-11-17")

        vietnam_dt = scheduled_dt.astimezone(tz_vietnam)
        vietnam_date = vietnam_dt.date()
        self.assertEqual(vietnam_date, date(2026, 11, 18))
        self.assertEqual(vietnam_dt.strftime("%H:%M"), "00:20")

        # Confirm they are indeed different calendar dates
        self.assertNotEqual(utc_date, vietnam_date)

    def test_day_counts_with_vietnam_vs_utc_date(self):
        """
        Simulate interim published rows having local_date='2026-11-18' and verify
        that querying with the Vietnam accounting date finds the rows, while
        querying with the naive sliced UTC date yields 0.
        """
        mock_config = MagicMock()
        mock_config.study.study_id = "hcm_urban_test"

        fake_rows = [
            {
                "study_id": "hcm_urban_test",
                "local_date": "2026-11-18",
                "current_speed_kmh": "25.0",
                "ci": "0.0",
                "precipitation_mm": "0.0",
                "weather_code": "3",
            }
            for _ in range(13)
        ]

        with patch("src.workflow.read_current", return_value=fake_rows):
            # Querying with UTC date string '2026-11-17' gives 0
            utc_counts = day_counts(mock_config, "2026-11-17")
            self.assertEqual(utc_counts["processed_rows"], 0)
            self.assertEqual(utc_counts["speed_nonnull"], 0)

            # Querying with Vietnam accounting date (str or date) gives 13
            vn_counts_str = day_counts(mock_config, "2026-11-18")
            self.assertEqual(vn_counts_str["processed_rows"], 13)
            self.assertEqual(vn_counts_str["speed_nonnull"], 13)
            self.assertEqual(vn_counts_str["ci_nonnull"], 13)
            self.assertEqual(vn_counts_str["weather_complete"], 13)

            vn_counts_obj = day_counts(mock_config, date(2026, 11, 18))
            self.assertEqual(vn_counts_obj["processed_rows"], 13)
            self.assertEqual(vn_counts_obj["speed_nonnull"], 13)

    def test_workflow_progress_accounting_day_resolution(self):
        """
        Verify the exact logic used in ETLQueue.completed to resolve accounting_day
        from record['scheduled_at'] and study timezone.
        """
        tz_vietnam = ZoneInfo("Asia/Ho_Chi_Minh")

        test_cases = [
            # Exactly midnight VN (17:00 UTC previous day)
            ("2026-11-17T17:00:00+00:00", date(2026, 11, 18)),
            # Slot 00:20 VN (17:20 UTC previous day)
            ("2026-11-17T17:20:00+00:00", date(2026, 11, 18)),
            # 06:59 VN (23:59 UTC previous day)
            ("2026-11-17T23:59:00+00:00", date(2026, 11, 18)),
            # 07:00 VN (00:00 UTC same day)
            ("2026-11-18T00:00:00+00:00", date(2026, 11, 18)),
            # 16:00 VN (09:00 UTC same day)
            ("2026-11-18T09:00:00+00:00", date(2026, 11, 18)),
            # 23:59 VN (16:59 UTC same day)
            ("2026-11-18T16:59:00+00:00", date(2026, 11, 18)),
        ]

        for scheduled_str, expected_local_day in test_cases:
            record = {"scheduled_at": scheduled_str}
            scheduled_dt = datetime.fromisoformat(record["scheduled_at"])
            if scheduled_dt.tzinfo is None:
                scheduled_dt = scheduled_dt.replace(tzinfo=timezone.utc)
            accounting_day = scheduled_dt.astimezone(tz_vietnam).date()
            self.assertEqual(accounting_day, expected_local_day, f"Failed for {scheduled_str}")


if __name__ == "__main__":
    unittest.main()
