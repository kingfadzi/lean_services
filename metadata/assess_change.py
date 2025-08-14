#!/usr/bin/env python3
"""
Assess change risk for an Application ID using the ARB decision matrix.

Usage:
  python assess_change.py --app-id <APP_CORRELATION_ID>

DB connection:
  - Use DATABASE_URL (postgres://user:pass@host:port/dbname)
  OR these env vars:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
import argparse
import psycopg  # psycopg 3
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple


# ---------------------------
# Decision Matrix Logic
# ---------------------------

@dataclass
class Ratings:
    criticality: str        # A, B, C, D
    security: str           # A1, A2, B, C, D
    integrity: str          # A, B, C, D
    availability: str       # A, B, C, D
    resilience: str         # A, B, C, D


@dataclass
class Recommendation:
    required: str           # "Yes – Mandatory" | "Yes – Conditional" | "No"
    arbs: Optional[str]     # "All ARBs" | "Security" | "Data" | "EA" | "Service Transition" | list
    mode: str               # "Full review" | "Scoped review" | "Express to prod"


def _arb_mapping(security: str, integrity: str, availability: str, resilience: str) -> List[str]:
    arbs = []
    if security in ("A1", "A2"):
        arbs.append("Security")
    if integrity == "A":
        arbs.append("Data")
    if availability == "A":
        arbs.append("EA")
    if resilience == "A":
        arbs.append("Service Transition")
    return arbs


def decide(r: Ratings) -> Recommendation:
    """
    Implements the matrix you approved (no 'Any' logic; it evaluates a concrete combination).
    """
    # A: Crown Jewels => All ARBs mandatory for all changes
    if r.criticality == "A":
        return Recommendation("Yes – Mandatory", "All ARBs", "Full review")

    # B rules
    if r.criticality == "B":
        if r.security in ("A1", "A2") or r.integrity == "A" or r.availability == "A" or r.resilience == "A":
            arbs = _arb_mapping(r.security, r.integrity, r.availability, r.resilience)
            return Recommendation("Yes – Mandatory", ", ".join(arbs) if arbs else "EA", "Full review")
        # All four ratings ≤ B (i.e., no A1/A2 and none equals A)
        sec_ok = r.security not in ("A1", "A2")
        others_ok = r.integrity in ("B", "C", "D") and r.availability in ("B", "C", "D") and r.resilience in ("B", "C", "D")
        if sec_ok and others_ok:
            return Recommendation("Yes – Conditional", "EA", "Scoped review")

    # C rules
    if r.criticality == "C":
        if r.security == "A1":
            return Recommendation("Yes – Mandatory", "Security", "Full review")
        if r.integrity == "A" and r.resilience == "A":
            return Recommendation("Yes – Mandatory", "Data, Service Transition", "Full review")
        if r.availability == "A":
            return Recommendation("Yes – Conditional", "EA", "Scoped review")
        # All ≤ B
        sec_ok = r.security not in ("A1", "A2")
        others_ok = r.integrity in ("B", "C", "D") and r.availability in ("B", "C", "D") and r.resilience in ("B", "C", "D")
        if sec_ok and others_ok:
            return Recommendation("No", None, "Express to prod")

    # D rules
    if r.criticality == "D":
        if r.security == "D" and r.integrity == "D" and r.availability == "D" and r.resilience == "D":
            return Recommendation("No", None, "Express to prod")
        # Any rating ≥ B (i.e., not all D)
        if r.security in ("A1", "A2", "A", "B") or r.integrity in ("A", "B") or r.availability in ("A", "B") or r.resilience in ("A", "B"):
            arbs = _arb_mapping(r.security, r.integrity, r.availability, r.resilience)
            return Recommendation("Yes – Conditional", ", ".join(arbs) if arbs else "EA", "Scoped review")

    # Default fall‑through (very low risk)
    return Recommendation("No", None, "Express to prod")


# ---------------------------
# SQL & Data Access
# ---------------------------

# NOTE: Adjust table/column names for ratings if your schema differs.
SQL = """
SELECT
    lca.lean_control_service_id,
    lpbd.jira_backlog_id,
    si.it_service_instance,
    so.service_offering_join,
    -- Ratings assumed to live on service_offering
    so.app_criticality     AS app_criticality,
    so.security_rating     AS security_rating,
    so.integrity_rating    AS integrity_rating,
    so.availability_rating AS availability_rating,
    so.resilience_rating   AS resilience_rating
FROM public.vwsfitbusinessservice AS bs
JOIN public.lean_control_application AS lca
  ON lca.servicenow_app_id = bs.service_correlation_id
JOIN public.vwsfitserviceinstance AS si
  ON bs.it_business_service_sysid = si.it_business_service_sysid
JOIN public.lean_control_product_backlog_details AS lpbd
  ON lpbd.lct_product_id = lca.lean_control_service_id
 AND lpbd.is_parent = TRUE
JOIN public.vwsfbusinessapplication AS child_app
  ON si.business_application_sysid = child_app.business_application_sys_id
JOIN public.service_offering AS so
  ON so.service_offering_join = si.it_service_instance
WHERE child_app.correlation_id = %(app_id)s
ORDER BY si.it_service_instance;
"""

def get_connection():
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg.connect(dsn)
    # Build from discrete env vars
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db   = os.getenv("PGDATABASE", "")
    user = os.getenv("PGUSER", "")
    pwd  = os.getenv("PGPASSWORD", "")
    return psycopg.connect(host=host, port=port, dbname=db, user=user, password=pwd)


# ---------------------------
# Orchestration
# ---------------------------

def rank_security(sec: str) -> int:
    order = {"A1": 1, "A2": 2, "A": 3, "B": 4, "C": 5, "D": 6}
    return order.get(sec, 99)

def rank_simple(val: str) -> int:
    order = {"A": 1, "B": 2, "C": 3, "D": 4}
    return order.get(val, 99)

def worst_overall(recos: List[Recommendation]) -> Recommendation:
    """
    Collapse multiple instance-level recommendations into a single 'overall' app-level recommendation.
    Priority: Mandatory > Conditional > No
    For ARB domains, union and keep stable order EA, Security, Data, Service Transition.
    For mode, choose the strongest needed.
    """
    if not recos:
        return Recommendation("No", None, "Express to prod")

    # Determine strongest requirement
    if any(r.required == "Yes – Mandatory" for r in recos):
        required = "Yes – Mandatory"
        mode = "Full review"
    elif any(r.required == "Yes – Conditional" for r in recos):
        required = "Yes – Conditional"
        mode = "Scoped review"
    else:
        required = "No"
        mode = "Express to prod"

    # Merge ARBs
    domain_order = ["EA", "Security", "Data", "Service Transition"]
    domains = []
    for r in recos:
        if r.arbs:
            if r.arbs == "All ARBs":
                domains = domain_order[:]  # all in order
                break
            for part in [d.strip() for d in r.arbs.split(",")]:
                if part and part not in domains:
                    domains.append(part)

    if domains and set(domains) == set(domain_order):
        arbs = "All ARBs"
    else:
        # sort by canonical order
        arbs = ", ".join([d for d in domain_order if d in domains]) if domains else None

    return Recommendation(required, arbs, mode)


def main():
    parser = argparse.ArgumentParser(description="Assess change risk for an Application ID")
    parser.add_argument("--app-id", required=True, help="child_app.correlation_id")
    args = parser.parse_args()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SQL, {"app_id": args.app_id})
        rows = cur.fetchall()

        if not rows:
            print(f"No records found for app_id='{args.app_id}'.")
            return

        # Column order based on SELECT
        # 0:lca_id, 1:jira_backlog_id, 2:instance, 3:so_join, 4:crit, 5:sec, 6:int, 7:avail, 8:resil
        per_instance_results: List[Tuple[dict, Recommendation]] = []
        for row in rows:
            lca_id, jira_id, inst, so_join, crit, sec, integ, avail, resil = row

            # Normalize/validate strings
            crit = (crit or "").strip().upper()
            sec  = (sec or "").strip().upper()
            integ = (integ or "").strip().upper()
            avail = (avail or "").strip().upper()
            resil = (resil or "").strip().upper()

            # If any rating is missing, you can decide a default or skip row.
            # Here we skip to keep decisions explicit.
            missing = []
            if crit not in ("A", "B", "C", "D"): missing.append("app_criticality")
            if sec not in ("A1", "A2", "A", "B", "C", "D"): missing.append("security_rating")
            if integ not in ("A", "B", "C", "D"): missing.append("integrity_rating")
            if avail not in ("A", "B", "C", "D"): missing.append("availability_rating")
            if resil not in ("A", "B", "C", "D"): missing.append("resilience_rating")
            if missing:
                print(f"WARNING: Skipping instance '{inst}' due to missing/invalid ratings: {', '.join(missing)}")
                continue

            r = Ratings(criticality=crit, security=sec, integrity=integ, availability=avail, resilience=resil)
            rec = decide(r)

            per_instance_results.append((
                {
                    "lean_control_service_id": lca_id,
                    "jira_backlog_id": jira_id,
                    "it_service_instance": inst,
                    "service_offering_join": so_join,
                    "app_criticality": crit,
                    "security_rating": sec,
                    "integrity_rating": integ,
                    "availability_rating": avail,
                    "resilience_rating": resil,
                },
                rec
            ))

        if not per_instance_results:
            print(f"No instances with complete ratings for app_id='{args.app_id}'.")
            return

        # Print per-instance detail
        print("\n=== Per-Instance Recommendations ===")
        for rec_row, rec in per_instance_results:
            print(f"- Instance: {rec_row['it_service_instance']}  (service_offering_join={rec_row['service_offering_join']})")
            print(f"  LCS ID: {rec_row['lean_control_service_id']} | JIRA Backlog: {rec_row['jira_backlog_id']}")
            print(f"  Ratings: Criticality={rec_row['app_criticality']}, Security={rec_row['security_rating']}, "
                  f"Integrity={rec_row['integrity_rating']}, Availability={rec_row['availability_rating']}, "
                  f"Resilience={rec_row['resilience_rating']}")
            print(f"  Recommendation: {rec.required} | ARBs: {rec.arbs or '—'} | Mode: {rec.mode}\n")

        # Roll-up to overall recommendation (worst case across instances)
        overall = worst_overall([rec for _, rec in per_instance_results])

        # Derive an "overall" (worst) ratings snapshot for context (optional)
        # We pick the worst (highest-severity) across instances by ordering.
        worst_crit = sorted([r["app_criticality"] for r, _ in per_instance_results], key=rank_simple)[0]
        worst_sec  = sorted([r["security_rating"] for r, _ in per_instance_results], key=rank_security)[0]
        worst_int  = sorted([r["integrity_rating"] for r, _ in per_instance_results], key=rank_simple)[0]
        worst_av   = sorted([r["availability_rating"] for r, _ in per_instance_results], key=rank_simple)[0]
        worst_res  = sorted([r["resilience_rating"] for r, _ in per_instance_results], key=rank_simple)[0]

        print("=== Overall App Recommendation (Worst-Case Across Instances) ===")
        print(f"App ID: {args.app_id}")
        print(f"Worst Ratings: Criticality={worst_crit}, Security={worst_sec}, Integrity={worst_int}, "
              f"Availability={worst_av}, Resilience={worst_res}")
        print(f"Overall Recommendation: {overall.required} | ARBs: {overall.arbs or '—'} | Mode: {overall.mode}\n")


if __name__ == "__main__":
    main()
