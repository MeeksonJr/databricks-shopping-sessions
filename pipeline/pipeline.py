#!/usr/bin/env python3
"""
MeeksonLabs High-Performance Databricks Shopping Sessions Pipeline
Buildathon 2026
"""

from __future__ import annotations

import collections
import datetime
from glob import glob
import json
import os
from pathlib import Path
import re
import sys
import duckdb
import polars as pl

# ASCII digits [0-9] only
RE_F1 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
RE_F2 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
RE_F3 = re.compile(r"^[0-9]+$")


def parse_ts(val: str | None) -> int | None:
    if not val:
        return None
    if RE_F3.match(val):
        try:
            return int(val)
        except ValueError:
            return None
    m1 = RE_F1.match(val)
    if m1:
        y, mo, d, h, mi, s = map(int, m1.groups())
        try:
            dt = datetime.datetime(y, mo, d, h, mi, s, tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    m2 = RE_F2.match(val)
    if m2:
        y, mo, d, h, mi, s, sign, oh, om = m2.groups()
        y, mo, d, h, mi, s, oh, om = map(int, (y, mo, d, h, mi, s, oh, om))
        if oh > 23 or om > 59:
            return None
        offset_minutes = oh * 60 + om
        if sign == "-":
            offset_minutes = -offset_minutes
        tz = datetime.timezone(datetime.timedelta(minutes=offset_minutes))
        try:
            dt = datetime.datetime(y, mo, d, h, mi, s, tzinfo=tz)
            return int(dt.astimezone(datetime.timezone.utc).timestamp() * 1000)
        except ValueError:
            return None
    return None


def parse_csv_line(line: str, expected_cols: int) -> list[str | None] | None:
    if not line:
        return None
    parts = line.split(",")
    if len(parts) != expected_cols:
        return None
    res = []
    for p in parts:
        if p.startswith('"') and p.endswith('"') and len(p) >= 2:
            inner = p[1:-1]
            if '"' in inner:
                return None
            res.append(inner if inner != "" else None)
        elif '"' in p:
            return None
        else:
            res.append(p if p != "" else None)
    return res


class DisjointSet:
    def __init__(self):
        self.parent = {}
        self.min_id = {}

    def find(self, i: str) -> str:
        if i not in self.parent:
            self.parent[i] = i
            self.min_id[i] = i
            return i
        if self.parent[i] != i:
            self.parent[i] = self.find(self.parent[i])
        return self.parent[i]

    def union(self, i: str, j: str) -> None:
        root_i = self.find(i)
        root_j = self.find(j)
        if root_i != root_j:
            m = min(self.min_id[root_i], self.min_id[root_j])
            self.parent[root_j] = root_i
            self.min_id[root_i] = m

    def get_canonical(self, i: str) -> str:
        root = self.find(i)
        return self.min_id[root]


def run_pipeline(data_dir: str, output_dir: str) -> None:
    # 1. Manifest
    with open(os.path.join(data_dir, "manifest.json"), "r", encoding="utf-8", newline="") as f:
        manifest = json.load(f)
    window_start = parse_ts(manifest["window_start"])
    window_end = parse_ts(manifest["window_end"])
    click_window_start = window_start - 7 * 24 * 3600 * 1000

    # 2. Products
    products = {}
    prod_path = os.path.join(data_dir, "products.csv")
    if os.path.exists(prod_path):
        lines = open(prod_path, "r", encoding="utf-8", errors="ignore", newline="").read().split("\n")
        for line in lines[1:]:
            parts = parse_csv_line(line, 2)
            if parts and parts[0] is not None and parts[1] is not None:
                products[parts[0]] = parts[1]

    # 3. Campaigns
    campaigns = {}
    camp_path = os.path.join(data_dir, "campaigns.csv")
    if os.path.exists(camp_path):
        lines = open(camp_path, "r", encoding="utf-8", errors="ignore", newline="").read().split("\n")
        for line in lines[1:]:
            parts = parse_csv_line(line, 2)
            if parts and parts[0] is not None and parts[1] is not None:
                campaigns[parts[0]] = parts[1]

    # 4. Links & Disjoint Set
    dsu = DisjointSet()
    for link_file in sorted(glob(os.path.join(data_dir, "links", "*.csv"))):
        lines = open(link_file, "r", encoding="utf-8", errors="ignore", newline="").read().split("\n")
        for line in lines[1:]:
            parts = parse_csv_line(line, 2)
            if parts and parts[0] is not None and parts[1] is not None:
                dsu.union(parts[0], parts[1])

    # 5. Events: Parse -> Dedup on event_id (keep smallest ingest_ts)
    # event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
    events_by_id = {}
    for ev_file in sorted(glob(os.path.join(data_dir, "events", "*.csv"))):
        lines = open(ev_file, "r", encoding="utf-8", errors="ignore", newline="").read().split("\n")
        for line in lines[1:]:
            parts = parse_csv_line(line, 7)
            if parts and parts[0] is not None:
                eid = parts[0]
                ingest_ts = parse_ts(parts[3])
                if ingest_ts is None:
                    continue
                if eid not in events_by_id or ingest_ts < events_by_id[eid]["ingest_ts"]:
                    events_by_id[eid] = {
                        "parts": parts,
                        "ingest_ts": ingest_ts,
                    }

    # Validate surviving events
    surviving_events = []
    for ev in events_by_id.values():
        parts = ev["parts"]
        ingest_ts = ev["ingest_ts"]
        uid = parts[1]
        raw_ts = parts[2]
        etype = parts[4]
        pid = parts[5]
        price_raw = parts[6]

        if not uid or not pid:
            continue
        ts = parse_ts(raw_ts)
        if ts is None or not (window_start <= ts < window_end):
            continue
        if etype not in ("view", "cart", "purchase"):
            continue

        price_cents = 0
        if etype == "purchase":
            if not price_raw or not RE_F3.match(price_raw):
                continue
            try:
                price_cents = int(price_raw)
            except ValueError:
                continue
            if not (1 <= price_cents <= 10_000_000):
                continue

        # Drop late events
        if ingest_ts - ts > 24 * 3600 * 1000:
            continue

        person = dsu.get_canonical(uid)
        category = products.get(pid, "unknown")
        surviving_events.append({
            "person": person,
            "ts": ts,
            "etype": etype,
            "pid": pid,
            "category": category,
            "price_cents": price_cents,
        })

    # 6. Clicks: Parse -> Dedup on click_id -> Validate
    # click_id,user_id,campaign_id,ts
    clicks_by_id = {}
    for clk_file in sorted(glob(os.path.join(data_dir, "clicks", "*.csv"))):
        lines = open(clk_file, "r", encoding="utf-8", errors="ignore", newline="").read().split("\n")
        for line in lines[1:]:
            parts = parse_csv_line(line, 4)
            if parts and parts[0] is not None:
                cid = parts[0]
                if cid not in clicks_by_id:
                    clicks_by_id[cid] = parts

    valid_clicks_by_person = collections.defaultdict(list)
    for cid, parts in clicks_by_id.items():
        uid = parts[1]
        camp_id = parts[2]
        raw_ts = parts[3]
        if not uid or not camp_id or camp_id not in campaigns:
            continue
        ts = parse_ts(raw_ts)
        if ts is None or not (click_window_start <= ts < window_end):
            continue
        person = dsu.get_canonical(uid)
        valid_clicks_by_person[person].append({
            "click_id": cid,
            "campaign_id": camp_id,
            "ts": ts,
        })

    # Sort clicks per person
    for p in valid_clicks_by_person:
        valid_clicks_by_person[p].sort(key=lambda c: (c["ts"], c["click_id"]))

    # 7. Sessionize events per person
    person_events = collections.defaultdict(list)
    for ev in surviving_events:
        person_events[ev["person"]].append(ev)

    user_daily_map = collections.defaultdict(lambda: {
        "sessions": 0,
        "events": 0,
        "revenue_cents": 0,
        "cat_rev": collections.defaultdict(int),
    })

    campaign_daily_map = collections.defaultdict(lambda: {
        "channel": "",
        "sessions": 0,
        "purchasing_sessions": 0,
        "revenue_cents": 0,
        "assisted_sessions": 0,
    })

    for person, evs in person_events.items():
        evs.sort(key=lambda e: e["ts"])

        sessions = []
        cur_session = []
        last_distinct_ts = None

        for e in evs:
            if not cur_session:
                cur_session.append(e)
                last_distinct_ts = e["ts"]
            else:
                if e["ts"] == last_distinct_ts:
                    cur_session.append(e)
                elif e["ts"] - last_distinct_ts > 1_800_000:
                    sessions.append(cur_session)
                    cur_session = [e]
                    last_distinct_ts = e["ts"]
                else:
                    cur_session.append(e)
                    last_distinct_ts = e["ts"]
        if cur_session:
            sessions.append(cur_session)

        person_clicks = valid_clicks_by_person.get(person, [])

        for sess in sessions:
            sess_start = sess[0]["ts"]
            sess_day = datetime.datetime.fromtimestamp(sess_start / 1000, tz=datetime.timezone.utc).date()
            sess_events = len(sess)
            sess_rev = sum(e["price_cents"] for e in sess)
            is_purchasing = any(e["etype"] == "purchase" for e in sess)

            # Update user daily
            ud_entry = user_daily_map[(person, sess_day)]
            ud_entry["sessions"] += 1
            ud_entry["events"] += sess_events
            ud_entry["revenue_cents"] += sess_rev
            for e in sess:
                if e["etype"] == "purchase":
                    ud_entry["cat_rev"][e["category"]] += e["price_cents"]

            # Attribution & Assists
            qualifying = [
                c for c in person_clicks
                if c["ts"] <= sess_start and (sess_start - c["ts"]) <= 7 * 24 * 3600 * 1000
            ]

            if qualifying:
                credited = max(qualifying, key=lambda c: (c["ts"], [-ord(ch) for ch in c["click_id"]]))
                credited_camp = credited["campaign_id"]
                credited_chan = campaigns[credited_camp]
            else:
                credited_camp = "organic"
                credited_chan = "organic"

            cd_entry = campaign_daily_map[(credited_camp, sess_day)]
            cd_entry["channel"] = credited_chan
            cd_entry["sessions"] += 1
            if is_purchasing:
                cd_entry["purchasing_sessions"] += 1
            cd_entry["revenue_cents"] += sess_rev

            assisting_campaigns = {c["campaign_id"] for c in qualifying}
            for ac in assisting_campaigns:
                entry = campaign_daily_map[(ac, sess_day)]
                entry["channel"] = campaigns[ac]
                entry["assisted_sessions"] += 1

    # Format user_daily rows
    user_rows = []
    for (person, day), stats in user_daily_map.items():
        top_cat = None
        if stats["cat_rev"]:
            top_cat = min(
                stats["cat_rev"].keys(),
                key=lambda cat: (-stats["cat_rev"][cat], cat),
            )
        user_rows.append({
            "user_id": person,
            "day": day,
            "sessions": stats["sessions"],
            "events": stats["events"],
            "revenue_cents": stats["revenue_cents"],
            "top_category": top_cat,
        })

    # Format campaign_daily rows
    campaign_rows = []
    for (camp, day), stats in campaign_daily_map.items():
        campaign_rows.append({
            "campaign_id": camp,
            "channel": stats["channel"],
            "day": day,
            "sessions": stats["sessions"],
            "purchasing_sessions": stats["purchasing_sessions"],
            "revenue_cents": stats["revenue_cents"],
            "assisted_sessions": stats["assisted_sessions"],
        })

    # Write Parquet outputs
    out_user_dir = os.path.join(output_dir, "user_daily")
    out_camp_dir = os.path.join(output_dir, "campaign_daily")
    os.makedirs(out_user_dir, exist_ok=True)
    os.makedirs(out_camp_dir, exist_ok=True)

    if user_rows:
        df_user = pl.DataFrame(user_rows, schema={
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "top_category": pl.String,
        })
    else:
        df_user = pl.DataFrame(schema={
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "top_category": pl.String,
        })
    df_user.write_parquet(os.path.join(out_user_dir, "part-0.parquet"))

    if campaign_rows:
        df_camp = pl.DataFrame(campaign_rows, schema={
            "campaign_id": pl.String,
            "channel": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "purchasing_sessions": pl.Int64,
            "revenue_cents": pl.Int64,
            "assisted_sessions": pl.Int64,
        })
    else:
        df_camp = pl.DataFrame(schema={
            "campaign_id": pl.String,
            "channel": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "purchasing_sessions": pl.Int64,
            "revenue_cents": pl.Int64,
            "assisted_sessions": pl.Int64,
        })
    df_camp.write_parquet(os.path.join(out_camp_dir, "part-0.parquet"))


if __name__ == "__main__":
    data = os.environ.get("DATA_DIR", "/data")
    out = os.environ.get("OUTPUT_DIR", "/output")
    if len(sys.argv) > 2:
        data = sys.argv[-2]
        out = sys.argv[-1]
    run_pipeline(data, out)
