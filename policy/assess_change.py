#!/usr/bin/env python3
import argparse, json, shutil, subprocess, sys
from typing import List, Optional, Dict
import yaml, psycopg, pandas as pd

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_connection(cfg: dict):
    db = cfg.get("database", {})
    return psycopg.connect(
        host=db.get("host", "localhost"),
        port=int(db.get("port", 5432)),
        dbname=db.get("name", ""),
        user=db.get("user", ""),
        password=db.get("password", "")
    )

def ensure_opa_available():
    if shutil.which("opa") is None:
        print("ERROR: 'opa' CLI not found on PATH.", file=sys.stderr)
        sys.exit(1)

def opa_eval(policy_path: str, input_obj: dict) -> dict:
    cmd = ["opa", "eval", "-I", "-f", "json", "--data", policy_path, "data.change_arb.result"]
    proc = subprocess.run(
        cmd, input=json.dumps(input_obj).encode("utf-8"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
    )
    out = json.loads(proc.stdout.decode("utf-8"))
    return out["result"][0]["expressions"][0]["value"]

def severity_rank(required: str) -> int:
    return {"No": 0, "Yes – Conditional": 1, "Yes – Mandatory": 2}.get(required, 0)

def canonical_arb_union(domains: List[Optional[str]]) -> Optional[str]:
    canon = ["EA", "Security", "Data", "Service Transition"]
    selected = set()
    for d in domains:
        if not d:
            continue
        for p in [x.strip() for x in d.split(",")]:
            if p == "All ARBs": return "All ARBs"
            if p in canon: selected.add(p)
    if not selected: return None
    return "All ARBs" if set(selected) == set(canon) else ", ".join([x for x in canon if x in selected])

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--app-id", required=True)
    p.add_argument("--policy", required=True)
    args = p.parse_args()

    ensure_opa_available()
    cfg = load_config(args.config)
    sql = cfg.get("sql")
    if not sql:
        print("ERROR: no 'sql' key found in config.yaml", file=sys.stderr); sys.exit(1)

    with get_connection(cfg) as conn:
        df = pd.read_sql(sql, conn, params={"app_id": args.app_id})

    if df.empty:
        print(f"No records found for app_id='{args.app_id}'."); sys.exit(0)

    for col in ["app_criticality","security_rating","integrity_rating","availability_rating","resilience_rating"]:
        df[col] = df[col].astype(str).str.strip().str.upper()

    print("\n=== Per-Instance Recommendations (OPA/Rego) ===")
    arb_strings, required_levels = [], []

    for _, row in df.iterrows():
        inp = {
            "criticality": row["app_criticality"],
            "security": row["security_rating"],
            "integrity": row["integrity_rating"],
            "availability": row["availability_rating"],
            "resilience": row["resilience_rating"],
        }
        try:
            decision = opa_eval(args.policy, inp)
        except subprocess.CalledProcessError as e:
            print(f"- Instance: {row['it_service_instance']}  (OPA ERROR)\n{e.stderr.decode()}")
            continue
        except Exception as e:
            print(f"- Instance: {row['it_service_instance']}  (OPA ERROR) {e}")
            continue

        arbs_list = decision.get("arb_domains")
        arbs = ", ".join(arbs_list) if isinstance(arbs_list, list) else (arbs_list or None)
        arb_strings.append(arbs)
        required_levels.append(decision.get("risk_assessment_required", "No"))

        print(f"- Instance: {row['it_service_instance']}  (service_offering_join={row['service_offering_join']})")
        print(f"  Ratings: Criticality={row['app_criticality']}, Security={row['security_rating']}, "
              f"Integrity={row['integrity_rating']}, Availability={row['availability_rating']}, "
              f"Resilience={row['resilience_rating']}")
        print(f"  Recommendation: {decision.get('risk_assessment_required','No')} | "
              f"ARBs: {arbs or '—'} | Mode: {decision.get('review_mode','Express to prod')}\n")

    if not required_levels:
        print("No instances evaluated."); sys.exit(0)

    strongest_required = max(required_levels, key=severity_rank)
    overall_mode = {"Yes – Mandatory":"Full review","Yes – Conditional":"Scoped review","No":"Express to prod"}[strongest_required]
    overall_arbs = canonical_arb_union(arb_strings)

    print("=== Overall App Recommendation ===")
    print(f"App ID: {args.app_id}")
    print(f"Overall Recommendation: {strongest_required} | ARBs: {overall_arbs or '—'} | Mode: {overall_mode}\n")

if __name__ == "__main__":
    main()
