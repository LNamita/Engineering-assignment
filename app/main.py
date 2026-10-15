"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file:
  1. configuration and database
  2. time / rounding helpers and the business rules R1-R5
  3. request models and validation helpers
  4. serialisation (MongoDB document -> API JSON)
  5. CRUD endpoints (employees, punch-in, punch-out, list, regularize)
  6. analytics pipeline builders (shared by the endpoints and by /admin/explain)
  7. analytics endpoints
  8. explain endpoint
"""
import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration and database
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # real environment variables take priority over .env

log = logging.getLogger("attendance")

# tz_aware=True: every datetime read back is an aware UTC datetime, so epoch conversion is unambiguous.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MIN_EPOCH_MS = 100_000_000_000
MAX_EPOCH_MS = 4_102_444_800_000

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
ABSENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: create_index is a no-op when an identical index already exists."""
    global _indexes_ready
    # employees
    employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="department_emp_code")
    employees.create_index([("joined_on", ASCENDING)], name="joined_on")
    employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="department_joined_on")
    # attendance_logs
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_code_date")
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
    logs.create_index(
        [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_desc_emp_code"
    )
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_code_punch_in_desc")
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # do not crash the process; /health retries
        log.error("index creation failed at startup: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time, rounding and business-rule helpers
# --------------------------------------------------------------------------- #
def to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Aware (or naive-means-UTC) datetime -> integer epoch milliseconds."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def from_ms(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ms_precision() -> datetime:
    """Server clock truncated to milliseconds, the precision BSON dates keep."""
    now = now_utc()
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def truncate_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def parse_hhmm(value: str) -> time:
    h, m = map(int, value.split(":"))
    return time(h, m)


def is_overnight(emp: dict) -> bool:
    return parse_hhmm(emp["shift_end"]) <= parse_hhmm(emp["shift_start"])


def round_half_up(value, places: int) -> Optional[float]:
    """Half-up rounding through Decimal; floats go through repr so 2.675 is treated as 2.675."""
    if value is None:
        return None
    if isinstance(value, Decimal128):
        value = value.to_decimal()
    if not isinstance(value, Decimal):
        value = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
    return float(value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; early punches of an overnight shift belong to the previous day."""
    local = punch_in.astimezone(IST)
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        return local.date() - timedelta(days=1)
    return local.date()


def shift_bounds(day: date, emp: dict) -> tuple[datetime, datetime]:
    """Shift start and end instants for the shift that starts on `day` (IST)."""
    start = datetime.combine(day, parse_hhmm(emp["shift_start"]), tzinfo=IST)
    end = datetime.combine(day, parse_hhmm(emp["shift_end"]), tzinfo=IST)
    if end <= start:  # overnight shift ends on the next calendar day
        end += timedelta(days=1)
    return start, end


def compute_late_minutes(punch_in: datetime, emp: dict, day: date) -> int:
    """R2: late only when strictly more than 10:00 after shift start; then floor minutes since shift start."""
    start, _ = shift_bounds(day, emp)
    seconds = int((truncate_seconds(punch_in) - start).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up (exact Decimal arithmetic, no float rounding)."""
    seconds = int((truncate_seconds(punch_out) - truncate_seconds(punch_in)).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, emp: dict, day: date) -> int:
    """R3: floor minutes after shift end, only when >= 30."""
    _, end = shift_bounds(day, emp)
    minutes = int((truncate_seconds(punch_out) - end).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], emp: dict, day: date) -> dict:
    """Derived values for a record, per the table in DATA_MODEL.md section 3."""
    if status in ABSENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, emp, day),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        hours = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = hours
        out["overtime_minutes"] = compute_overtime(punch_out, emp, day)
        out["half_day"] = Decimal(repr(hours)) < HALF_DAY_HOURS  # R5 uses the rounded value
    return out


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = map(int, month.split("-"))
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


def count_weekdays(first: date, last: date) -> int:
    """Mon-Fri days in [first, last], arithmetic rather than a day-by-day loop."""
    if first > last:
        return 0
    days = (last - first).days + 1
    full_weeks, rest = divmod(days, 7)
    count = full_weeks * 5
    start_dow = first.weekday()
    count += sum(1 for i in range(rest) if (start_dow + i) % 7 < 5)
    return count


# --------------------------------------------------------------------------- #
# 3. Request models and validation helpers
# --------------------------------------------------------------------------- #
EpochMillis = Annotated[StrictInt, Field(ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
HHMM = Annotated[str, Field(pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]
DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def unprocessable(msg: str, loc: tuple = ("body",)) -> RequestValidationError:
    """A 422 with FastAPI's standard validation body ({"detail": [ ... ]})."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


def parse_date(value: Optional[str], loc: tuple) -> Optional[date]:
    if value is None:
        return None
    try:
        if not DATE_RE.match(value):
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        raise unprocessable("must be a valid date YYYY-MM-DD", loc)


class EmployeeCreate(BaseModel):
    emp_code: Annotated[str, Field(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        if not DATE_RE.match(v):
            raise ValueError("must be a date YYYY-MM-DD")
        date.fromisoformat(v)  # raises ValueError for e.g. 2026-02-30
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    """Derived fields are not declared, so a client that sends them is ignored (extra keys are dropped)."""

    status: Optional[Status] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        for field in ("status", "punch_in", "punch_out"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} may be omitted but not null")
        return self


Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
Month = Annotated[str, Query(pattern=MONTH_PATTERN)]
Limit = Annotated[int, Query(ge=1, le=50)]


# --------------------------------------------------------------------------- #
# 4. Serialisation
# --------------------------------------------------------------------------- #
INSTANT_FIELDS = ("punch_in", "punch_out")


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": to_ms(doc.get("created_at")),
    }


def history_out(entry: dict) -> dict:
    changes = {}
    for field, change in (entry.get("changes") or {}).items():
        if field in INSTANT_FIELDS:
            changes[field] = {"from": to_ms(change.get("from")), "to": to_ms(change.get("to"))}
        else:
            changes[field] = {"from": change.get("from"), "to": change.get("to")}
    return {"at": to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Never exposes _id; legacy documents get the documented defaults for missing fields."""
    work_hours = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": float(work_hours) if work_hours is not None else None,
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    emp.setdefault("shift_start", "09:30")
    emp.setdefault("shift_end", "18:30")
    return emp


# --------------------------------------------------------------------------- #
# 5. System, employees and attendance endpoints
# --------------------------------------------------------------------------- #
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    doc["created_at"] = now_ms_precision()
    try:
        employees.insert_one(doc)  # uniqueness is enforced by the unique index, not by a pre-check
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


def employees_query(department: Optional[str]) -> dict:
    return {"department": department} if department is not None else {}


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = employees_query(department)
    total = employees.count_documents(q)
    cursor = employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else truncate_seconds(now_utc())
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, emp, day),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # the unique (emp_code, date) index makes concurrent duplicates fail here
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else truncate_seconds(now_utc())
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    pin = rec["punch_in"]
    if ts <= truncate_seconds(pin):
        raise unprocessable("punched_at must be after punch_in", ("body", "punched_at"))
    if ts - truncate_seconds(pin) > MAX_SHIFT:
        raise unprocessable("punched_at must be within 24 hours of punch_in", ("body", "punched_at"))
    day = date.fromisoformat(rec["date"])
    derived = derived_fields(rec["status"], pin, ts, emp, day)
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},  # conditional update: only one concurrent punch-out wins
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def attendance_query(
    emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str], status: Optional[str]
) -> dict:
    d_from = parse_date(date_from, ("query", "date_from"))
    d_to = parse_date(date_to, ("query", "date_to"))
    if d_from and d_to and d_from > d_to:
        raise unprocessable("date_from must be on or before date_to", ("query", "date_from"))
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if d_from or d_to:
        q["date"] = {}
        if d_from:
            q["date"]["$gte"] = d_from.isoformat()
        if d_to:
            q["date"]["$lte"] = d_to.isoformat()
    if status is not None:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q = attendance_query(emp_code, date_from, date_to, status)
    total = logs.count_documents(q)
    cursor = logs.find(q).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeRequest):  # noqa: A002 - name fixed by the contract
    day = parse_date(date, ("path", "date"))
    emp = get_employee_or_404(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day") or False),
    }
    new_status = body.status or old["status"]
    if new_status in ABSENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"{new_status} clears punch times; do not send punch_in/punch_out")
        new_in = new_out = None
    else:
        new_in = from_ms(body.punch_in) if body.punch_in is not None else old["punch_in"]
        new_out = from_ms(body.punch_out) if body.punch_out is not None else old["punch_out"]
        if new_in is None:
            raise unprocessable("a presence status requires a punch_in", ("body", "punch_in"))
        if body.punch_in is not None and attendance_date(new_in, emp) != day:
            raise unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        if new_out is not None:
            if new_out <= new_in:
                raise unprocessable("punch_out must be after punch_in", ("body", "punch_out"))
            if new_out - new_in > MAX_SHIFT:
                raise unprocessable("punch_out must be within 24 hours of punch_in", ("body", "punch_out"))

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out}
    if all(new[k] == old[k] for k in new):
        raise unprocessable("the request changes nothing")
    new.update(derived_fields(new_status, new_in, new_out, emp, day))
    changes = {k: {"from": old[k], "to": new[k]} for k in new if new[k] != old[k]}

    entry = {"at": now_ms_precision(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    history_len = len(rec.get("history") or [])
    # Optimistic concurrency: only apply if nobody changed the record (or its history) since we read it.
    guard = {
        "_id": rec["_id"],
        "status": old["status"],
        "punch_in": old["punch_in"],
        "punch_out": old["punch_out"],
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, history_len]},
    }
    updated = logs.find_one_and_update(
        guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently, retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 6. Analytics pipeline builders (shared with /admin/explain)
# --------------------------------------------------------------------------- #
def present_value_expr(prefix: str = "$") -> dict:
    """R5/R6: 1 for a full present day, 0.5 for a half day, 0 for ABSENT/LEAVE."""
    return {
        "$cond": [
            {"$in": [f"{prefix}status", list(PRESENCE)]},
            {"$cond": [{"$eq": [{"$ifNull": [f"{prefix}half_day", False]}, True]}, 0.5, 1]},
            0,
        ]
    }


def is_weekday_expr(date_field: str = "$date") -> dict:
    # $dayOfWeek: 1 = Sunday ... 7 = Saturday
    dow = {"$dayOfWeek": {"$dateFromString": {"dateString": date_field, "format": "%Y-%m-%d"}}}
    return {"$and": [{"$gte": [dow, 2]}, {"$lte": [dow, 6]}]}


def late_flag_expr() -> dict:
    return {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}


def employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": {"$cond": [is_weekday_expr(), present_value_expr(), 0]}},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": late_flag_expr()},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Runs on `employees` so that people with zero logs still produce a row (R9)."""
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match = {"department": department, **match}
    return [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                    {
                        "$group": {
                            "_id": None,
                            "present_days": {"$sum": {"$cond": [is_weekday_expr(), present_value_expr(), 0]}},
                            # exact decimal sum so the half-up average is not disturbed by float error
                            "work_hours_sum": {
                                "$sum": {
                                    "$cond": [
                                        {"$and": [{"$in": ["$status", list(PRESENCE)]},
                                                  {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]},
                                        {"$toDecimal": "$work_hours"},
                                        {"$toDecimal": 0},
                                    ]
                                }
                            },
                            "work_hours_count": {
                                "$sum": {
                                    "$cond": [
                                        {"$and": [{"$in": ["$status", list(PRESENCE)]},
                                                  {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "late_count": {"$sum": late_flag_expr()},
                            "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                            "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                        }
                    },
                ],
                "as": "stats",
            }
        },
        {"$unwind": {"path": "$stats", "preserveNullAndEmptyArrays": True}},  # keep employees with no logs
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": {"$ifNull": ["$stats.present_days", 0]}},
                "work_hours_sum": {"$sum": {"$ifNull": ["$stats.work_hours_sum", {"$toDecimal": 0}]}},
                "work_hours_count": {"$sum": {"$ifNull": ["$stats.work_hours_count", 0]}},
                "late_count": {"$sum": {"$ifNull": ["$stats.late_count", 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$stats.total_late_minutes", 0]}},
                "leave_count": {"$sum": {"$ifNull": ["$stats.leave_count", 0]}},
                "on_duty_count": {"$sum": {"$ifNull": ["$stats.on_duty_count", 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": 1,
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$work_hours_count", 0]},
                        {"$divide": ["$work_hours_sum", {"$toDecimal": "$work_hours_count"}]},
                        None,
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        # standard competition ranking (1, 2, 2, 4) computed by the database, not from array positions
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff are kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def round_half_up_expr(expr: Any, places: int) -> dict:
    """Half-up rounding inside MongoDB ($round is half-to-even); operates on non-negative decimals."""
    scale = {"$toDecimal": 10 ** places}
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, {"$toDecimal": "0.5"}]}}, scale]}


def department_trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """Runs on `employees`: one join event per employee (for headcount) plus one event per log, then the
    database groups by day, fills the missing days with $densify and computes the window with $setWindowFields."""
    from_dt = datetime.combine(d_from, time(0), tzinfo=timezone.utc)
    after_to_dt = datetime.combine(d_to + timedelta(days=1), time(0), tzinfo=timezone.utc)
    as_date = lambda field: {"$dateFromString": {"dateString": field, "format": "%Y-%m-%d"}}  # noqa: E731
    return [
        {"$match": {"department": department, "joined_on": {"$lte": d_to.isoformat()}}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
                    {"$project": {"_id": 0, "date": 1, "present": present_value_expr(), "late": late_flag_expr()}},
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "_id": 0,
                "events": {
                    "$concatArrays": [
                        # an employee is in the headcount from max(joined_on, from) onwards
                        [{"date": {"$max": ["$joined_on", d_from.isoformat()]}, "joined": 1, "present": 0, "late": 0}],
                        {"$map": {"input": "$logs", "as": "l",
                                  "in": {"date": "$$l.date", "joined": 0, "present": "$$l.present", "late": "$$l.late"}}},
                    ]
                },
            }
        },
        {"$unwind": "$events"},
        {"$replaceWith": "$events"},
        # sentinel so $densify has something to extend even when nobody is in the headcount yet
        {"$unionWith": {"pipeline": [{"$documents": [{"date": d_from.isoformat(), "joined": 0, "present": 0, "late": 0}]}]}},
        {
            "$group": {
                "_id": as_date("$date"),
                "joined": {"$sum": "$joined"},
                "present_count": {"$sum": "$present"},
                "late_count": {"$sum": "$late"},
            }
        },
        {"$project": {"_id": 0, "day": "$_id", "joined": 1, "present_count": 1, "late_count": 1}},
        # one row per calendar day of the range, even when nothing happened that day
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [from_dt, after_to_dt]}}},
        {
            "$set": {
                "joined": {"$ifNull": ["$joined", 0]},
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {"$and": [{"$gte": [{"$dayOfWeek": "$day"}, 2]}, {"$lte": [{"$dayOfWeek": "$day"}, 6]}]},
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}}},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        round_half_up_expr({"$divide": [{"$toDecimal": "$present_count"},
                                                        {"$toDecimal": "$headcount"}]}, 4),
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                # $avg ignores nulls and returns null when the window has no numeric value
                "output": {"moving_avg_raw": {"$avg": "$attendance_rate", "window": {"documents": [-6, "current"]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": {"$cond": [{"$eq": ["$attendance_rate", None]}, None, {"$toDouble": "$attendance_rate"}]},
                "moving_avg_7d": {
                    "$cond": [
                        {"$eq": [{"$ifNull": ["$moving_avg_raw", None]}, None]},
                        None,
                        {"$toDouble": round_half_up_expr("$moving_avg_raw", 4)},
                    ]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]


def trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from = parse_date(from_, ("query", "from"))
    d_to = parse_date(to, ("query", "to"))
    if d_to < d_from:
        raise unprocessable("to must be on or after from", ("query", "to"))
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"range must not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    return d_from, d_to


# --------------------------------------------------------------------------- #
# 7. Analytics endpoints
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Month):
    emp = get_employee_or_404(emp_code)
    first, last = month_bounds(month)
    joined = date.fromisoformat(emp["joined_on"]) if emp.get("joined_on") else first
    working_days = count_weekdays(max(first, joined), last)  # R7
    rows = list(logs.aggregate(employee_monthly_pipeline(emp_code, month)))
    agg = rows[0] if rows else {}
    present_days = agg.get("present_days", 0)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present_days, 2),
        "leave_days": agg.get("leave_days", 0),
        "late_count": agg.get("late_count", 0),
        "total_late_minutes": agg.get("total_late_minutes", 0),
        "total_overtime_minutes": agg.get("total_overtime_minutes", 0),
        "attendance_pct": (
            round_half_up(Decimal(repr(float(present_days))) * 100 / Decimal(working_days), 2) if working_days else None
        ),
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Month, department: Optional[str] = None):
    items = [
        {
            "department": row["department"],
            "headcount": row["headcount"],
            "present_days": round_half_up(row["present_days"], 2),
            "avg_work_hours": round_half_up(row["avg_work_hours"], 2),
            "late_count": row["late_count"],
            "total_late_minutes": row["total_late_minutes"],
            "leave_count": row["leave_count"],
            "on_duty_count": row["on_duty_count"],
        }
        for row in employees.aggregate(department_summary_pipeline(month, department))
    ]
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: Month, limit: Limit = 10, department: Optional[str] = None):
    return {"month": month, "items": list(logs.aggregate(late_leaderboard_pipeline(month, limit, department)))}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, Query(alias="from")],
    to: str,
):
    d_from, d_to = trend_range(from_, to)
    if employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    items = list(employees.aggregate(department_trend_pipeline(department, d_from, d_to)))
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 8. Explain endpoint
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def require(value: Optional[str], name: str) -> str:
    if value is None:
        raise RequestValidationError([{"type": "missing", "loc": ("query", name), "msg": "Field required", "input": None}])
    return value


def check_month(value: str) -> str:
    if not re.match(MONTH_PATTERN, value):
        raise unprocessable("month must be YYYY-MM", ("query", "month"))
    return value


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: Limit = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint == "attendance_list":
        collection = logs
        command = {
            "find": logs.name,
            "filter": attendance_query(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
    else:
        if endpoint == "employee_monthly":
            collection = logs
            pipeline = employee_monthly_pipeline(require(emp_code, "emp_code"), check_month(require(month, "month")))
        elif endpoint == "department_summary":
            collection = employees
            pipeline = department_summary_pipeline(check_month(require(month, "month")), department)
        elif endpoint == "late_leaderboard":
            collection = logs
            pipeline = late_leaderboard_pipeline(check_month(require(month, "month")), limit, department)
        else:  # department_trend
            collection = employees
            d_from, d_to = trend_range(require(from_, "from"), require(to, "to"))
            pipeline = department_trend_pipeline(require(department, "department"), d_from, d_to)
        command = {"aggregate": collection.name, "pipeline": pipeline, "cursor": {}}
    result = db.command({"explain": command, "verbosity": "executionStats"})
    # raw explain document as plain JSON (relaxed Extended JSON for BSON-only types such as Timestamp)
    raw = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection.name, "explain": raw}
