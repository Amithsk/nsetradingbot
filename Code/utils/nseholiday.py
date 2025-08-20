from datetime import date

HOLIDAYS_2025 = {
    date(2025, 1, 1),
    date(2025, 1, 26),
    date(2025, 3, 14),
    date(2025, 4, 10),
    date(2025, 4, 18),
    date(2025, 5, 1),
    date(2025, 8, 15),
    date(2025, 8, 27),
    date(2025, 10, 2),
    date(2025, 10, 21),
    date(2025, 10, 22),
    date(2025, 11, 5),
    date(2025, 12, 25),
}

def nseholiday(date_obj):
    return date_obj in HOLIDAYS_2025
