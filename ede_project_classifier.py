import os
import re
import sys
import json
import argparse
import textwrap
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from google.cloud import bigquery
from openai import OpenAI

load_dotenv()


# =========================================================================
# 1. TEAM DEFINITIONS
# =========================================================================

TEAM_DEFINITIONS = {
    "C360": (
        "Team C360 is the primary team responsible for the entire lifecycle of "
        "consumer data within our systems. This includes managing data ingestion "
        "from various sources, standardization and normalization to ensure "
        "consistency, and enforcing data privacy and compliance rules. Their scope "
        "covers everything from basic customer contact information to complex "
        "relationship mapping, transactional history, and preference centers. They "
        "handle defect resolutions related to data anomalies, process bulk requests "
        "for data modifications, and provide guidance on integrating consumer data "
        "with external platforms. Key table references: Customer_Master, "
        "Contact_Info, Preference_Registry."
    ),
    "B360": (
        "Team B360 is dedicated to managing all intakes related to business data, "
        "including B2B relationships, corporate accounts, and partner information. "
        "They manage the entire data lifecycle from ingestion through processing, "
        "ensuring consistency and adherence to business logic and regulatory "
        "standards. Their scope encompasses everything from basic company profiles "
        "to complex hierarchy management, contractual agreements, and associated "
        "contacts. They handle defect resolutions related to business data "
        "anomalies, process bulk modifications, and advise on integrating business "
        "data with external platforms. Key table references: Business_Master, "
        "Account_Details, Partner_Registry."
    ),
    "Financial Insights": (
        "The Financial Insights team manages all intakes related to fiscal data, "
        "including budgeting, forecasting, transaction processing, and financial "
        "reporting. They handle data ingestion from various financial systems, "
        "perform reconciliation checks to ensure accuracy, and ensure compliance "
        "with accounting standards and regulatory requirements. Their scope covers "
        "everything from general ledger entries to complex financial models, audit "
        "requests, and key performance metrics. They handle defect resolutions "
        "related to financial data discrepancies, process adjustments, and provide "
        "analysis for strategic planning. Key table references: General_Ledger, "
        "Budget_Details, Transaction_Logs."
    ),
    "Reliability": (
        "The Reliability team is responsible for ensuring the overall stability, "
        "performance, and availability of our systems. They manage common modules "
        "and shared technology components, proactively monitoring system health, "
        "conducting capacity planning, and responding to critical incidents. Their "
        "scope covers everything from platform infrastructure to cross-cutting "
        "services like logging and monitoring, and disaster recovery. They handle "
        "defect resolutions related to performance degradation, manage upgrades "
        "and patching, and establish best practices for system architecture and "
        "reliability. Key table references: System_Health, Common_Modules, "
        "Incident_Logs."
    ),
    "Metadata Management": (
        "The Metadata Management team is responsible for maintaining the "
        "centralized catalog and governance of all data assets across the "
        "organization. They manage the complete metadata lifecycle from "
        "definition and ingestion, lineage tracking, and ongoing maintenance, "
        "ensuring consistency and discoverability of data across all systems. "
        "Their scope encompasses everything from basic data dictionaries and "
        "schema definitions to complex data lineage mappings, asset "
        "classifications. They handle defect resolutions related to metadata "
        "inconsistencies, process bulk updates to data definitions and "
        "classifications, and provide guidance on implementing metadata standards "
        "and discovery practices across teams."
    ),
}

TEAM_NAMES = list(TEAM_DEFINITIONS.keys()) + ["Other"]

# =========================================================================
# 2. CONFIG (env vars)
# =========================================================================

FUELIX_API_KEY = os.environ.get("FUELIX_API_KEY", "")
FUELIX_MODEL = os.environ.get("FUELIX_MODEL", "claude-sonnet-4-5")
FUELIX_BASE_URL = os.environ.get("FUELIX_BASE_URL", "https://proxy.fuelix.ai")

_fuelix_client = None


def get_fuelix_client() -> OpenAI:
    global _fuelix_client
    if _fuelix_client is None:
        _fuelix_client = OpenAI(api_key=FUELIX_API_KEY, base_url=FUELIX_BASE_URL)
    return _fuelix_client


INTAKE_TABLE = "`cio-datahub-work-pr-0be526.datahub_temp.bq_jira_project`"
DEFAULT_GCP_PROJECT = "cio-datahub-work-dv-c03a6c"
DEFAULT_RESULTS_TABLE = "cio-datahub-work-dv-c03a6c.datahub_operations.ede_ticket_classifications"


def _require_config():
    missing = [name for name, val in [("FUELIX_API_KEY", FUELIX_API_KEY)] if not val]
    if missing:
        sys.exit(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Set them in a .env file in this folder, or export them in your shell."
        )


# =========================================================================
# 3. BIGQUERY: FETCH RAW TICKET DATA
# =========================================================================

INTAKE_QUERY = """
WITH src AS (
  SELECT
    key AS jira_key,
    jql AS project_key,
    summary,
    issue_type,
    status,
    assignee,
    JSON_EXTRACT(issue_str, '$.fields.description') AS description_json,
    SAFE_CAST(JSON_VALUE(issue_str, '$.fields.created') AS TIMESTAMP) AS ticket_created,
    SAFE_CAST(JSON_VALUE(issue_str, '$.fields.updated') AS TIMESTAMP) AS ticket_updated
  FROM {intake_table}
  WHERE key IS NOT NULL
    AND JSON_EXTRACT(issue_str, '$.fields.description') LIKE '%Requestor Name:%'
    AND JSON_EXTRACT(issue_str, '$.fields.description') LIKE '%What do you need help with%'
)
SELECT * FROM src
WHERE TRUE
  {where_clause}
ORDER BY ticket_created DESC
{limit_clause}
"""


def adf_to_text(node) -> str:
    """Recursively flatten Jira JSON into plain text."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    parts = []
    if isinstance(node, dict):
        if node.get("type") == "text":
            parts.append(node.get("text", ""))
        for child in node.get("content", []) or []:
            parts.append(adf_to_text(child))
        if node.get("type") in ("paragraph", "heading", "listItem"):
            parts.append("\n")
    elif isinstance(node, list):
        for child in node:
            parts.append(adf_to_text(child))
    return "".join(parts)


def flatten_description(description_json: Optional[str]) -> str:
    """description_json is the raw JSON string from JSON_EXTRACT (or None/"null")."""
    if not description_json or description_json == "null":
        return ""
    try:
        parsed = json.loads(description_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    return adf_to_text(parsed)


FIELD_PATTERN = re.compile(
    r"\*\s*([^*:]+?)\s*:\s*\*?\s*\n?\s*(.+?)(?=\n\s*[•\-]?\s*\*[^*:]+?:\s*|\Z)",
    re.DOTALL,
)


def extract_fields(text: str) -> dict:
    fields = {}
    for label, value in FIELD_PATTERN.findall(text):
        fields[label.strip().lower()] = value.strip()
    return fields


TABLE_FIELD_KEYS = ("table path/name or api name", "table name")
PLATFORM_FIELD_KEYS = ("platform",)
TITLE_FIELD_KEYS = ("support request title", "defect title")
DESC_FIELD_KEYS = ("problem description", "please provide a detailed description")


def pick_field(fields: dict, keys) -> Optional[str]:
    for k in keys:
        if k in fields and fields[k].strip():
            return fields[k].strip()
    return None


def ensure_results_table(client: bigquery.Client, results_table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{results_table}` (
        ticket_key           STRING    NOT NULL,
        team                 STRING,
        confidence_level     INT64,
        reasoning            STRING,
        signals_used         ARRAY<STRING>,
        ticket_created_date  TIMESTAMP,
        ticket_updated_date  TIMESTAMP,
        classified_at        TIMESTAMP NOT NULL
    )
    """
    client.query(ddl).result()


def get_already_classified_keys(client: bigquery.Client, results_table: str) -> list:
    query = f"SELECT ticket_key FROM `{results_table}`"
    try:
        return [row["ticket_key"] for row in client.query(query).result()]
    except Exception:
        # Table may not exist yet on a brand-new setup; ensure_results_table
        # is always called before this, but fall back just in case.
        return []


def fetch_tickets_from_bq(
    client: bigquery.Client,
    project_key: str,
    ticket_keys: Optional[list],
    already_classified: list,
    limit: Optional[int],
    since_date: Optional[str] = None,
) -> list:
    since_clause = " AND ticket_created >= TIMESTAMP(@since_date)" if since_date else ""

    if ticket_keys:

        where_clause = f"AND project_key = @project_key AND jira_key IN UNNEST(@ticket_keys){since_clause}"
        limit_clause = ""
        params = [
            bigquery.ScalarQueryParameter("project_key", "STRING", project_key),
            bigquery.ArrayQueryParameter("ticket_keys", "STRING", ticket_keys),
        ]
    else:
        where_clause = f"AND project_key = @project_key AND jira_key NOT IN UNNEST(@already_classified){since_clause}"
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        params = [
            bigquery.ScalarQueryParameter("project_key", "STRING", project_key),
            bigquery.ArrayQueryParameter("already_classified", "STRING", already_classified or [""]),
        ]

    if since_date:
        params.append(bigquery.ScalarQueryParameter("since_date", "DATE", since_date))

    query = INTAKE_QUERY.format(intake_table=INTAKE_TABLE, where_clause=where_clause, limit_clause=limit_clause)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(query, job_config=job_config)
    rows = list(job.result())
    return [dict(row.items()) for row in rows]


def write_results_to_bq(client: bigquery.Client, results_table: str, results: list) -> None:
    if not results:
        return
    now = datetime.now(timezone.utc).isoformat()
    bq_rows = []
    for r in results:
        if r.get("error"):
            continue  # don't write failed rows; they'll be retried next run
        bq_rows.append({
            "ticket_key": r["key"],
            "team": r.get("team"),
            "confidence_level": r.get("confidence_level"),
            "reasoning": r.get("reasoning"),
            "signals_used": r.get("signals_used"),
            "ticket_created_date": r.get("ticket_created_date"),
            "ticket_updated_date": r.get("ticket_updated_date"),
            "classified_at": now,
        })
    if not bq_rows:
        return

    # Remove any existing row(s) for these ticket keys first, so re-running
    # a ticket (e.g. via --ticket-key, or a future re-run) replaces its old
    # result instead of duplicating it.
    keys = [row["ticket_key"] for row in bq_rows]
    delete_query = f"DELETE FROM `{results_table}` WHERE ticket_key IN UNNEST(@keys)"
    delete_job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("keys", "STRING", keys)]
    )
    client.query(delete_query, job_config=delete_job_config).result()


    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_json(bq_rows, results_table, job_config=load_job_config)
    load_job.result()

    errors = load_job.errors
    if errors:
        print(f"  ! BigQuery load errors: {errors}")


# =========================================================================
# 4. LLM CLASSIFICATION (FuelIX)
# =========================================================================

def build_prompt(title, table_ref, system_ref, description) -> list:
    defs_block = "\n\n".join(f"- {name}: {desc}" for name, desc in TEAM_DEFINITIONS.items())

    system_prompt = textwrap.dedent(f"""
        You are a triage assistant for an Enterprise Data Hub (EDE) intake process.
        Every ticket must be routed to exactly one of these five teams, or to "Other"
        if none of the five genuinely fit:

        {defs_block}

        Decide the team using this priority order:
        1. Table name / dataset the ticket references (if it clearly belongs to one team's data).
        2. System / platform the ticket references (if it clearly maps to one team).
        3. The description of the problem, matched against the team definitions above.
        4. If none of the above give a confident, single-team match, answer "Other".

        Also assess your own confidence that this team assignment is correct, as an
        integer from 0 to 100:
        - 80-100: a table name, dataset, or system reference directly and
          unambiguously identifies one team's data.
        - 50-79: the description strongly aligns with one team's stated scope, but
          there's no direct table/system reference confirming it.
        - 0-49: the match is a reasonable guess but the ticket is vague, could
          plausibly fit more than one team, or barely fits any of the five
          (this includes most "Other" classifications).

        List EVERY signal that actually supports your team choice (not just the
        one you weighted most heavily) - e.g. if both a table name AND the
        description point to the same team, include both. Order the list by
        priority, highest first (table_name, then system_name, then
        description). If nothing meaningfully supports the choice, use ["none"].

        Respond with ONLY a JSON object, no markdown fences, in this exact shape:
        {{"team": "<one of: {', '.join(TEAM_NAMES)}>",
          "confidence": <integer 0-100>,
          "signals_used": ["<table_name|system_name|description|none>", ...],
          "reasoning": "<1-3 sentences explaining why you picked this team>"}}
    """).strip()

    user_prompt = textwrap.dedent(f"""
        Ticket title: {title or "(none provided)"}
        Table / API reference: {table_ref or "(none provided)"}
        System / platform: {system_ref or "(none provided)"}

        Description:
        {description or "(none provided)"}
    """).strip()

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


# Valid signal labels, in priority order - used to validate and sort
# whatever the model returns in "signals_used".
SIGNAL_PRIORITY = ["table_name", "system_name", "description", "none"]


def normalize_signals(raw_signals) -> list:
    """Validate the model's signals_used value and sort it by SIGNAL_PRIORITY.

    Accepts a list (expected) or a bare string (in case the model ignores
    the array format), drops anything not in SIGNAL_PRIORITY, dedupes, and
    falls back to ["none"] if nothing valid remains.
    """
    if isinstance(raw_signals, str):
        raw_signals = [raw_signals]
    if not isinstance(raw_signals, list):
        raw_signals = []

    valid = {s for s in raw_signals if s in SIGNAL_PRIORITY}
    if not valid:
        return ["none"]
    return [s for s in SIGNAL_PRIORITY if s in valid]


def classify_with_ai(title, table_ref, system_ref, description) -> dict:
    messages = build_prompt(title, table_ref, system_ref, description)
    client = get_fuelix_client()

    response = client.chat.completions.create(
        model=FUELIX_MODEL,
        messages=messages,
        temperature=0,
        max_tokens=1024,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {
            "team": "Other",
            "confidence": 20,
            "signals_used": ["none"],
            "reasoning": f"Model response was not valid JSON, defaulted to Other. Raw response: {raw[:300]}",
        }

    if parsed.get("team") not in TEAM_NAMES:
        parsed["team"] = "Other"

    try:
        confidence = int(parsed.get("confidence"))
        confidence = max(0, min(100, confidence))
    except (TypeError, ValueError):
        confidence = 20  # conservative fallback if the model didn't return a usable number
    parsed["confidence"] = confidence

    parsed["signals_used"] = normalize_signals(parsed.get("signals_used"))

    return parsed


# =========================================================================
# 5. MAIN
# =========================================================================

def process_row(row: dict) -> dict:
    key = row.get("jira_key")

    description_text = flatten_description(row.get("description_json"))
    fields = extract_fields(description_text)

    title = pick_field(fields, TITLE_FIELD_KEYS) or row.get("summary")
    table_ref = pick_field(fields, TABLE_FIELD_KEYS)
    system_ref = pick_field(fields, PLATFORM_FIELD_KEYS)
    problem_desc = pick_field(fields, DESC_FIELD_KEYS) or description_text

    result = classify_with_ai(title, table_ref, system_ref, problem_desc)

    def _iso(dt):
        return dt.isoformat() if dt else None

    return {
        "key": key,
        "title": title,
        "table_ref": table_ref,
        "system_ref": system_ref,
        "team": result.get("team", "Other"),
        "confidence_level": result.get("confidence", 20),
        "signals_used": result.get("signals_used", ["none"]),
        "reasoning": result.get("reasoning", ""),
        "ticket_created_date": _iso(row.get("ticket_created")),
        "ticket_updated_date": _iso(row.get("ticket_updated")),
    }


def main():
    parser = argparse.ArgumentParser(description="Classify unclassified EDE front-door intake tickets by owning team, sourced from and written to BigQuery.")
    parser.add_argument("--gcp-project", default=DEFAULT_GCP_PROJECT, help=f"GCP project ID to bill/run BigQuery jobs under. Default: {DEFAULT_GCP_PROJECT}")
    parser.add_argument("--results-table", default=DEFAULT_RESULTS_TABLE, help=f"Full 'project.dataset.table' where classifications are written (created automatically if it doesn't exist). Default: {DEFAULT_RESULTS_TABLE}")
    parser.add_argument("--project-key", default="EDE", help="JIRA project key to filter on (the 'jql' column in the intake table). Default: EDE")
    parser.add_argument("--since", default="2026-01-01", help="Only consider tickets created on/after this date (YYYY-MM-DD). Default: 2026-01-01. Pass empty string to disable.")
    parser.add_argument("--ticket-key", help="One or more ticket keys, comma-separated (e.g. EDE-1234,EDE-1235). Forces (re)classification even if already in the results table.")
    parser.add_argument("--limit", type=int, default=20, help="Max new/unclassified tickets to process per run (ignored if --ticket-key is set)")
    parser.add_argument("--out", default="classification_results.json", help="Local JSON copy of this run's results, for quick review")
    args = parser.parse_args()

    _require_config()

    client = bigquery.Client(project=args.gcp_project)
    ensure_results_table(client, args.results_table)

    ticket_keys = [k.strip() for k in args.ticket_key.split(",")] if args.ticket_key else None
    already_classified = [] if ticket_keys else get_already_classified_keys(client, args.results_table)

    rows = fetch_tickets_from_bq(client, args.project_key, ticket_keys, already_classified, args.limit, since_date=args.since or None)
    print(f"Fetched {len(rows)} ticket(s) to classify.")

    results = []
    for row in rows:
        try:
            r = process_row(row)
        except Exception as e:
            r = {"key": row.get("jira_key"), "error": str(e)}
            print(f"  ! {row.get('jira_key')}: ERROR {e}")
            results.append(r)
            continue

        print(f"  {r['key']}  ->  {r['team']}  [{r['confidence_level']}% confidence]  (via {', '.join(r['signals_used'])})")
        print(f"      {r['reasoning']}")
        results.append(r)

    write_results_to_bq(client, args.results_table, results)

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)

    written = sum(1 for r in results if not r.get("error"))
    print(f"\nWrote {written} result(s) to `{args.results_table}` and a local copy to {args.out}")


if __name__ == "__main__":
    main()