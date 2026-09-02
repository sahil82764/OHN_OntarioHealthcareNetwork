#!/usr/bin/env python3
"""
Validate generated synthetic data before loading it into Fabric.

Two things are checked:

1. Defect rates — are the deliberate data-quality problems actually present,
   at roughly the configured rate? If they are not, the DQ engine has nothing
   to catch and the data-quality report will be a flat 100%.

2. Planted correlations — do the relationships the reports are meant to reveal
   actually exist in the data? If ED wait time has no effect on satisfaction
   scores, report page 12 shows a shapeless cloud and the project looks broken
   even though every pipeline ran correctly.

Usage:
    python validate_data.py --landing ./landing
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime

GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


def read(landing: str, source: str, entity: str) -> list[dict]:
    base = os.path.join(landing, source, entity)
    if not os.path.isdir(base):
        return []
    rows = []
    for part in sorted(os.listdir(base)):
        pdir = os.path.join(base, part)
        if not os.path.isdir(pdir):
            continue
        for fn in sorted(os.listdir(pdir)):
            if fn.endswith(".csv"):
                with open(os.path.join(pdir, fn), encoding="utf-8") as fh:
                    rows.extend(csv.DictReader(fh))
    return rows


def pct(n, d):
    return 0.0 if not d else 100.0 * n / d


def check(label, value, lo, hi, unit="%"):
    ok = lo <= value <= hi
    colour = GREEN if ok else RED
    mark = "PASS" if ok else "FAIL"
    print(f"  {colour}{mark}{RESET}  {label:<52} {value:>8.2f}{unit}  "
          f"(expect {lo}–{hi}{unit})")
    return ok


def ts(v):
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def num(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default="./landing")
    args = ap.parse_args()
    L = args.landing
    failures = 0

    print("\n" + "=" * 76)
    print("SECTION 1 — Injected defects (these feed the DQ engine)")
    print("=" * 76)

    ehr_pat = read(L, "EHR", "patient")
    sched_pat = read(L, "SCHED", "patient")
    fin_pat = read(L, "FIN", "patient_account")
    all_pat = ehr_pat + sched_pat + fin_pat
    n = len(all_pat)

    missing_hcn = sum(1 for r in all_pat if not (
        r.get("health_card_number") or r.get("health_card") or r.get("hc_number")))
    failures += not check("Patient records with no health card number",
                          pct(missing_hcn, n), 5.0, 16.0)

    missing_dob = sum(1 for r in all_pat if not (
        r.get("date_of_birth") or r.get("dob") or r.get("birth_dt")))
    failures += not check("Patient records with no birth date",
                          pct(missing_dob, n), 0.5, 3.5)

    truth = read(L, "_truth", "patient_truth")
    total_src = sum(int(r["source_record_count"]) for r in truth)
    dup_ratio = total_src / max(1, len(truth))
    failures += not check("Source records per real person (MPI difficulty)",
                          dup_ratio, 2.2, 2.9, "x")

    multi = sum(1 for r in truth if int(r["source_record_count"]) >= 2)
    failures += not check("People appearing in 2+ source records",
                          pct(multi, len(truth)), 85.0, 100.0)

    adm = read(L, "EHR", "admission")
    bad_los = sum(1 for r in adm if r["discharge_ts"]
                  and ts(r["discharge_ts"]) and ts(r["admission_ts"])
                  and ts(r["discharge_ts"]) < ts(r["admission_ts"]))
    failures += not check("Admissions with discharge before admission (DQ-ADM-003)",
                          pct(bad_los, len(adm)), 0.1, 1.2)

    ed = read(L, "EHR", "emergency_visit")
    bad_ctas = sum(1 for r in ed if r["triage_score"] not in ("1", "2", "3", "4", "5"))
    failures += not check("ED visits with invalid CTAS (DQ-ED-001)",
                          pct(bad_ctas, len(ed)), 0.1, 1.5)

    lab = read(L, "LIS", "lab_result")
    bad_tat = sum(1 for r in lab if ts(r["result_ts"]) and ts(r["order_ts"])
                  and ts(r["result_ts"]) < ts(r["order_ts"]))
    failures += not check("Lab results with negative turnaround (DQ-LAB-003)",
                          pct(bad_tat, len(lab)), 0.05, 1.0)

    no_loinc = sum(1 for r in lab if not r["loinc_code"] or r["loinc_code"] in
                   ("LOCAL-99", "XX-000", "PENDING"))
    failures += not check("Lab results with unmappable LOINC (DQ-LAB-001)",
                          pct(no_loinc, len(lab)), 0.5, 3.5)

    clm = read(L, "CLAIMS", "claim_header")
    over = sum(1 for r in clm if num(r["approved_amount"]) and num(r["billed_amount"])
               and num(r["approved_amount"]) > num(r["billed_amount"]))
    failures += not check("Claims approved above billed (DQ-CLM-002)",
                          pct(over, len(clm)), 0.05, 1.2)

    print("\n" + "=" * 76)
    print("SECTION 2 — Operational metrics (these drive the report pages)")
    print("=" * 76)

    # --- Length of stay -------------------------------------------------
    los_vals = []
    for r in adm:
        a, d = ts(r["admission_ts"]), ts(r["discharge_ts"]) if r["discharge_ts"] else None
        if a and d and d >= a:
            los_vals.append(max(1, (d - a).days))
    if los_vals:
        failures += not check("Average length of stay",
                              statistics.mean(los_vals), 4.0, 12.0, " days")
        failures += not check("Median length of stay",
                              statistics.median(los_vals), 3.0, 9.0, " days")

    # --- ED wait times --------------------------------------------------
    pia_by_ctas = defaultdict(list)
    for r in ed:
        a, p = ts(r["arrival_ts"]), ts(r["physician_seen_ts"]) if r["physician_seen_ts"] else None
        if a and p and p > a and r["triage_score"] in ("1", "2", "3", "4", "5"):
            pia_by_ctas[int(r["triage_score"])].append((p - a).total_seconds() / 60)

    print("\n  Physician initial assessment wait by CTAS level:")
    medians = {}
    for lvl in sorted(pia_by_ctas):
        m = statistics.median(pia_by_ctas[lvl])
        medians[lvl] = m
        print(f"      CTAS {lvl}: median {m:>7.1f} min   (n={len(pia_by_ctas[lvl]):,})")

    monotonic = all(medians.get(i, 0) < medians.get(i + 1, 1e9)
                    for i in sorted(medians)[:-1])
    colour = GREEN if monotonic else RED
    print(f"  {colour}{'PASS' if monotonic else 'FAIL'}{RESET}  "
          f"Higher acuity is seen sooner (CTAS 1 < 2 < 3 < 4 < 5)")
    failures += not monotonic

    lwbs = sum(1 for r in ed if r["left_without_being_seen"] == "1")
    failures += not check("Left without being seen rate", pct(lwbs, len(ed)), 1.5, 7.0)

    admit_conv = sum(1 for r in ed if r["resulted_in_admission"] == "1")
    failures += not check("ED to inpatient conversion rate",
                          pct(admit_conv, len(ed)), 8.0, 28.0)

    # --- Appointments ---------------------------------------------------
    appt = read(L, "SCHED", "appointment")
    statuses = Counter(r["status_code"] for r in appt)
    completed = statuses["COMPLETED"]
    no_shows = statuses["NO_SHOW"]
    cancels = sum(v for k, v in statuses.items() if k.startswith("CANCELLED"))
    failures += not check("No-show rate (excl. cancellations)",
                          pct(no_shows, completed + no_shows), 6.0, 22.0)
    failures += not check("Cancellation rate", pct(cancels, len(appt)), 8.0, 26.0)

    # Lead time should predict no-shows — page 6 depends on this
    short_lead = [r for r in appt if r["booking_ts"] and r["scheduled_ts"]
                  and (ts(r["scheduled_ts"]) - ts(r["booking_ts"])).days <= 7]
    long_lead = [r for r in appt if r["booking_ts"] and r["scheduled_ts"]
                 and (ts(r["scheduled_ts"]) - ts(r["booking_ts"])).days >= 30]
    ns_short = pct(sum(1 for r in short_lead if r["status_code"] == "NO_SHOW"), len(short_lead))
    ns_long = pct(sum(1 for r in long_lead if r["status_code"] == "NO_SHOW"), len(long_lead))
    print(f"\n  No-show rate by booking lead time:")
    print(f"      ≤7 days out:  {ns_short:>6.2f}%   (n={len(short_lead):,})")
    print(f"      ≥30 days out: {ns_long:>6.2f}%   (n={len(long_lead):,})")
    ok = ns_long > ns_short * 1.15
    print(f"  {GREEN if ok else RED}{'PASS' if ok else 'FAIL'}{RESET}  "
          f"Longer lead time raises no-show risk")
    failures += not ok

    # --- Claims ---------------------------------------------------------
    adjudicated = [r for r in clm if r["adjudication_date"]]
    approved = sum(1 for r in adjudicated if r["status_code"] in ("A1", "A2"))
    denied = sum(1 for r in adjudicated if r["status_code"] == "D1")
    failures += not check("Claim approval rate (of adjudicated)",
                          pct(approved, len(adjudicated)), 78.0, 96.0)
    failures += not check("Claim denial rate (of adjudicated)",
                          pct(denied, len(adjudicated)), 4.0, 22.0)

    by_payer = defaultdict(lambda: [0, 0])
    for r in adjudicated:
        by_payer[r["payer_id"]][0] += 1
        if r["status_code"] == "D1":
            by_payer[r["payer_id"]][1] += 1
    print("\n  Denial rate by payer (page 11 needs this to vary):")
    rates = []
    for payer, (tot, den) in sorted(by_payer.items()):
        if tot >= 20:
            r_ = pct(den, tot)
            rates.append(r_)
            print(f"      {payer}: {r_:>6.2f}%   (n={tot:,})")
    spread = (max(rates) - min(rates)) if len(rates) > 1 else 0
    ok = spread >= 4.0
    print(f"  {GREEN if ok else RED}{'PASS' if ok else 'FAIL'}{RESET}  "
          f"Denial rate varies meaningfully across payers (spread {spread:.1f} pts)")
    failures += not ok

    # --- Satisfaction vs wait time --------------------------------------
    srv = read(L, "SURVEY", "survey_response")
    ed_by_enc = {r["encounter_id"]: r for r in ed}
    paired = []
    for s in srv:
        e = ed_by_enc.get(s["encounter_id"])
        if not e or not e["physician_seen_ts"]:
            continue
        a, p = ts(e["arrival_ts"]), ts(e["physician_seen_ts"])
        sc = num(s["overall_score"])
        if a and p and p > a and sc and 1 <= sc <= 10:
            paired.append(((p - a).total_seconds() / 60, sc))

    if len(paired) >= 30:
        paired.sort()
        third = len(paired) // 3
        fast = statistics.mean(s for _, s in paired[:third])
        slow = statistics.mean(s for _, s in paired[-third:])
        print(f"\n  Mean satisfaction score by ED wait tercile:")
        print(f"      Shortest waits: {fast:.2f} / 10")
        print(f"      Longest waits:  {slow:.2f} / 10")
        ok = fast - slow >= 0.5
        print(f"  {GREEN if ok else RED}{'PASS' if ok else 'FAIL'}{RESET}  "
              f"Longer ED waits produce lower satisfaction (gap {fast - slow:.2f})")
        failures += not ok
    else:
        print(f"\n  {YELLOW}SKIP{RESET}  Too few ED-linked surveys to test "
              f"the wait/satisfaction relationship (n={len(paired)})")

    # --- Readmissions ---------------------------------------------------
    by_patient = defaultdict(list)
    for r in adm:
        a, d = ts(r["admission_ts"]), ts(r["discharge_ts"]) if r["discharge_ts"] else None
        if a:
            by_patient[r["patient_id"]].append((a, d, r["admission_type"]))
    readmits = index = 0
    for _, events in by_patient.items():
        events.sort()
        for i in range(1, len(events)):
            prior_d = events[i - 1][1]
            if not prior_d:
                continue
            gap = (events[i][0] - prior_d).days
            index += 1
            if 0 <= gap <= 30 and events[i][2] != "EL":
                readmits += 1
    if index:
        failures += not check("30-day readmission rate (approximate)",
                              pct(readmits, index + len(adm) - index), 4.0, 25.0)

    # --- Bed occupancy sanity -------------------------------------------
    beds = read(L, "FACIL", "bed")
    bed_asgn = read(L, "EHR", "bed_assignment")
    occupied_days = 0.0
    for r in bed_asgn:
        s, e = ts(r["assignment_start_ts"]), ts(r["assignment_end_ts"])
        if s and e and e > s:
            occupied_days += (e - s).total_seconds() / 86400
    span_days = 365 * 2.5
    occ = pct(occupied_days, len(beds) * span_days)
    failures += not check("Overall bed occupancy rate", occ, 8.0, 92.0)

    print("\n" + "=" * 76)
    if failures == 0:
        print(f"{GREEN}All checks passed — data is ready to load into Fabric.{RESET}")
    else:
        print(f"{RED}{failures} check(s) failed.{RESET} Adjust the constants at the "
              f"top of ohn_generator.py and regenerate.")
        print("Common fixes: raise --patients (small samples fail rate checks by "
              "chance), or tune DEFECTS / BASE_* rates.")
    print("=" * 76 + "\n")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
