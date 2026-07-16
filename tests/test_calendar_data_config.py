from __future__ import annotations

import unittest
from pathlib import Path

from india_swing.calendar_data.config import (
    CALENDAR_DATA_ROOT_ENV,
    CalendarDataConfig,
)


class CalendarDataConfigTests(unittest.TestCase):
    def test_default_and_explicit_roots(self) -> None:
        self.assertEqual(
            CalendarDataConfig.from_env({}).data_root,
            Path("var/calendar_data"),
        )
        self.assertEqual(
            CalendarDataConfig.from_env(
                {CALENDAR_DATA_ROOT_ENV: "C:/calendar-archive"}
            ).data_root,
            Path("C:/calendar-archive"),
        )

    def test_null_byte_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, CALENDAR_DATA_ROOT_ENV):
            CalendarDataConfig.from_env(
                {CALENDAR_DATA_ROOT_ENV: "bad\x00path"}
            )


if __name__ == "__main__":
    unittest.main()
