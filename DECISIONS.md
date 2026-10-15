# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (identity, 409 on duplicates, `$lookup` target); `department+emp_code` (filtered, sorted employee list; trend); `joined_on` and `department+joined_on` (headcount match in the summary). `attendance_logs`: unique `emp_code+date` (one record per day, race-safe punch-in, monthly/summary/trend lookups); `date desc+emp_code` (list sort and date ranges, leaderboard month match); `status+date desc+emp_code` (status filter); `emp_code+punch_in desc` (punch-out's "latest punch-in ≤ t"). I rejected an index on `late_minutes`: the month's date range already narrows the leaderboard scan well enough.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one` with no pre-check. The unique index lets exactly one insert succeed; the other raises `DuplicateKeyError`, which becomes 409.

3. **Ties.** `$setWindowFields` with `$rank` gives competition ranks (1, 2, 2, 4), and I filter `rank <= limit` after ranking. Everyone tied at the cutoff is returned, ordered by `emp_code`, so the result can have more rows than `limit`.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`), not from the logs. It `$lookup`s that month's logs and `$unwind`s with `preserveNullAndEmptyArrays`, so an employee with no logs still adds 1 to headcount and 0 to everything else.

5. **At 100x.** I would precompute per-employee daily/monthly rollups at write time (punch-out / PATCH), or use a nightly `$merge`, so analytics read thousands of rows rather than millions. I would also move from offset pagination to keyset pagination for `/attendance`.
