import json
import os
import sys
from datetime import datetime, timezone

import duckdb
import streamlit as st

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

import bedrock_helper
import importlib
importlib.reload(bedrock_helper)
from bedrock_helper import call_nova_lite, call_nova_pro


APP_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(APP_DIR, "..", ".."))
DB_PATH = os.path.join(APP_DIR, "..", "shared", "sigma_platform.duckdb")
PIPELINE_CANDIDATES = [
    os.path.join(REPO_ROOT, "day7", "lab", "pipeline_brain", "hardened_pipeline.py"),
    os.path.join(REPO_ROOT, "day7", "lab", "pipeline_brain", "my_pipeline.py"),
    os.path.join(REPO_ROOT, "day7", "lab", "pipeline_brain", "generated_pipeline.py"),
]
VERDICT_PATH = os.path.join(APP_DIR, "verdict.json")


st.set_page_config(page_title="Runbook Guardian", layout="wide")

st.markdown("""
<style>
    /* Gradient Headings */
    h1 {
        background: -webkit-linear-gradient(45deg, #FF4B2B, #FF416C);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 900 !important;
    }
    h2 {
        color: #FF416C !important;
        border-bottom: 2px solid #FF416C;
        display: inline-block;
        padding-bottom: 4px;
        margin-bottom: 10px;
    }
    h3 {
        color: #1e3c72 !important;
        font-weight: 700 !important;
        background: -webkit-linear-gradient(45deg, #1e3c72, #2a5298);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    /* Button Click Effects */
    div.stButton > button:first-child {
        transition: all 0.15s ease-in-out;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    div.stButton > button:first-child:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }
    div.stButton > button:first-child:active {
        transform: translateY(2px) scale(0.95);
        box-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

if "token_tracker" not in st.session_state:
    st.session_state.token_tracker = {
        "nova-lite": {"input": 0, "output": 0, "cost": 0.0},
        "nova-pro": {"input": 0, "output": 0, "cost": 0.0},
    }


def read_pipeline() -> tuple[str, str]:
    for path in PIPELINE_CANDIDATES:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                return f.read(), path
    return "", "No pipeline file found"


def read_pipeline_from_input() -> tuple[str, str]:
    st.sidebar.header("Pipeline Input")
    source = st.sidebar.radio(
        "Source",
        ["Default Day 7 pipeline", "Upload .py file", "Paste code", "Local path"],
    )

    if source == "Upload .py file":
        uploaded = st.sidebar.file_uploader("Pipeline file", type=["py", "txt"])
        if uploaded is not None:
            return uploaded.read().decode("utf-8", errors="replace"), uploaded.name
        return read_pipeline()

    if source == "Paste code":
        pasted = st.sidebar.text_area("Paste pipeline code", height=260)
        if pasted.strip():
            return pasted, "Pasted pipeline code"
        return read_pipeline()

    if source == "Local path":
        default_path = PIPELINE_CANDIDATES[0] if os.path.exists(PIPELINE_CANDIDATES[0]) else ""
        local_path = st.sidebar.text_input("Absolute or repo-relative path", value=default_path)
        candidate = local_path
        if candidate and not os.path.isabs(candidate):
            candidate = os.path.join(REPO_ROOT, candidate)
        if candidate and os.path.exists(candidate):
            with open(candidate, "r", encoding="utf-8", errors="replace") as f:
                return f.read(), candidate
        st.sidebar.warning("Path not found. Falling back to default pipeline.")
        return read_pipeline()

    return read_pipeline()


def fallback_runbook_sections(pipeline_path: str) -> dict:
    return {
        "setup": f"""### Environment
- Pipeline source: `{pipeline_path}`
- Use the project Python environment with PySpark installed.
- Use Java 17 for Spark jobs.
- Confirm input transaction data and merchant dimension data are available.

### Required Access
- Read access to raw transaction input.
- Read access to merchant dimension data.
- Write access to Bronze, Silver, and metadata output locations.""",
        "normal_run": """1. Confirm `run_date` and `run_id`.
2. Start the Spark session.
3. Run Bronze ingestion for the selected input file.
4. Run Silver transformation for the same `run_date`.
5. Confirm Silver output partition exists.
6. Record row counts, warnings, and run status in the incident or batch log.""",
        "failure_scenarios": """| Scenario | Symptom | First Action |
|---|---|---|
| Missing source file | Bronze read fails or row count is zero | Stop and request upstream replay |
| Java/Spark startup failure | SparkSession cannot start | Check Java 17, PySpark, and local port binding |
| Merchant dimension missing | Join fails or `UNMATCHED` spikes | Stop publication and page merchant data owner |
| Unexpected row-count drop | Silver count much lower than Bronze | Compare filtered records before publishing |
| Duplicate transactions | More duplicate IDs than expected | Verify dedup logic and upstream resend behavior |""",
        "validation_steps": """- Bronze row count for `run_date` is greater than zero.
- Silver row count is within expected range after filters.
- `transaction_id` has no nulls.
- `amount` has no negative values.
- Duplicate `transaction_id` values are removed.
- `quality_flag = UNMATCHED` remains below the escalation threshold.
- Output partition was replaced idempotently, not appended repeatedly.""",
        "escalation_path": """| Owner | When To Escalate |
|---|---|
| Data Engineering on-call | Reruns, partition cleanup, code failure |
| Platform/SRE | Spark, Java, filesystem, permissions, scheduler failures |
| Merchant data owner | Missing or stale merchant dimension |
| Analytics/product owner | Dashboard delay or partial-data decision |""",
        "known_gaps": """The pipeline marks unmatched merchants as `UNMATCHED`, but it does not enforce a threshold that halts publishing when unmatched merchant enrichment spikes. Add a fail-fast Silver quality gate before writing output.""",
    }


def sections_to_markdown(sections: dict) -> str:
    titles = {
        "setup": "Setup",
        "normal_run": "Normal Run",
        "failure_scenarios": "Failure Scenarios",
        "validation_steps": "Validation Steps",
        "escalation_path": "Escalation Path",
        "known_gaps": "Known Gaps",
    }
    parts = ["# Operational Runbook"]
    for key, title in titles.items():
        parts.append(f"## {title}\n{section_to_markdown(sections.get(key, ''))}")
    return "\n\n".join(parts)


def section_to_markdown(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return "\n".join(f"- {section_to_markdown(item)}" for item in value).strip()
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            label = str(key).replace("_", " ").title()
            body = section_to_markdown(item)
            lines.append(f"### {label}\n{body}" if body else f"### {label}")
        return "\n\n".join(lines).strip()
    if value is None:
        return ""
    return str(value).strip()


def parse_runbook_sections(text: str, pipeline_path: str) -> dict:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
        if cleaned.rstrip().endswith("```"):
            cleaned = cleaned.rstrip()[:-3].strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return fallback_runbook_sections(pipeline_path)

    fallback = fallback_runbook_sections(pipeline_path)
    return {
        "setup": section_to_markdown(parsed.get("setup") or fallback["setup"]),
        "normal_run": section_to_markdown(parsed.get("normal_run") or fallback["normal_run"]),
        "failure_scenarios": section_to_markdown(parsed.get("failure_scenarios") or fallback["failure_scenarios"]),
        "validation_steps": section_to_markdown(parsed.get("validation_steps") or fallback["validation_steps"]),
        "escalation_path": section_to_markdown(parsed.get("escalation_path") or fallback["escalation_path"]),
        "known_gaps": section_to_markdown(parsed.get("known_gaps") or fallback["known_gaps"]),
    }


def fallback_runbook(pipeline_path: str) -> str:
    return sections_to_markdown(fallback_runbook_sections(pipeline_path))


def fallback_questions() -> str:
    return """1. Which exact command should I run for the Silver stage, and from which directory?
2. If the merchant dimension is missing, should I publish Silver with `UNMATCHED` rows or stop the pipeline?
3. What row-count drop is big enough to page someone instead of treating it as normal filtering?
4. Where do I find the latest successful run id and run metadata?
5. If `quality_flag = UNMATCHED` suddenly jumps, is that a documentation issue or a pipeline failure?"""


def fallback_senior_answers() -> str:
    return """1. Run the pipeline from the repository root or the lab folder documented in Setup. Use the same Python and Java 17 environment that was validated for Spark. The runbook should include the exact command for your deployment target.
2. Stop the pipeline if merchant enrichment is missing or if `UNMATCHED` spikes above threshold. Publishing rows that cannot be attributed to merchants can corrupt Gold revenue and merchant-performance reporting.
3. Page Data Engineering if Silver drops more than 20% from Bronze after expected null, negative amount, and dedup filters. Use historical baselines if they exist.
4. Check the run metadata output for `run_id`, `run_date`, row counts, status, and error message. If metadata is missing, treat that as an operational gap and document the manual incident notes.
5. It is both a good question and a pipeline issue. The code flags `UNMATCHED`, but without a fail-fast quality gate the job can still succeed while producing unsafe data."""


def fallback_gap_analysis() -> str:
    return """| Question | Classification | Decision |
|---|---|---|
| Exact command and directory | RUNBOOK GAP | Add a copy-paste run command and environment prerequisites. |
| Missing merchant dimension | GOOD QUESTION | This reveals a real operational decision: publishing unmatched rows can corrupt analytics. |
| Row-count drop threshold | RUNBOOK GAP | Add thresholds for Bronze to Silver drops and escalation. |
| Latest run id metadata | RUNBOOK GAP | Add where run metadata lives and what fields to check. |
| `UNMATCHED` spike | GOOD QUESTION | This reveals a pipeline gap: the code flags unmatched rows but does not halt on a threshold. |

Most critical pipeline issue: the Silver pipeline creates `quality_flag`, but no quality gate prevents publishing when merchant enrichment fails at scale.
"""


def fallback_updated_runbook() -> str:
    return """# Updated Critical Runbook Snippet

## Silver Quality Gate
Before publishing Silver, calculate:

- `bronze_count`
- `silver_count`
- `unmatched_count`
- `unmatched_pct = unmatched_count / silver_count * 100`

Stop the pipeline and page Data Engineering if:

- `silver_count = 0`
- `silver_count` drops by more than 20% vs Bronze after expected filters
- `unmatched_pct > 5%`
- merchant dimension file/table is missing

Do not publish downstream Gold tables until the failed quality gate is resolved or explicitly waived by the data owner.

## Proposed Code Fix
Add a Silver-stage check before writing output:

```python
unmatched_count = silver_df.filter(col("quality_flag") == "UNMATCHED").count()
output_count = silver_df.count()
if output_count == 0 or unmatched_count / output_count > 0.05:
    raise ValueError("Silver quality gate failed: unmatched merchant threshold exceeded")
```
"""


@st.cache_data(show_spinner=False)
def load_evidence() -> dict:
    if not os.path.exists(DB_PATH):
        return {"error": f"Database not found: {DB_PATH}"}

    con = duckdb.connect(DB_PATH, read_only=True)
    tables = [row[0] for row in con.execute("show tables").fetchall()]
    evidence = {"tables": tables}

    if "silver_transactions" in tables:
        evidence["silver_profile"] = con.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS missing_transaction_ids,
                SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts
            FROM silver_transactions
            """
        ).fetchdf()

    if "merchants" in tables and "silver_transactions" in tables:
        evidence["merchant_join_risk"] = con.execute(
            """
            SELECT
                COUNT(*) AS silver_rows,
                SUM(CASE WHEN m.merchant_id IS NULL THEN 1 ELSE 0 END) AS unmatched_merchants
            FROM silver_transactions s
            LEFT JOIN merchants m USING (merchant_id)
            """
        ).fetchdf()

    if "pipeline_versions" in tables:
        evidence["pipeline_versions"] = con.execute(
            "SELECT * FROM pipeline_versions LIMIT 10"
        ).fetchdf()

    con.close()
    return evidence


def safe_ai_call(label: str, fn, system: str, user: str, max_tokens: int, fallback: str) -> str:
    try:
        with st.spinner(label):
            text, usage = fn(system, user, max_tokens=max_tokens, return_usage=True)
            
            model_name = "nova-lite" if "lite" in fn.__name__ else "nova-pro"
            in_tokens = usage.get("inputTokens", 0)
            out_tokens = usage.get("outputTokens", 0)
            
            if model_name == "nova-lite":
                cost = (in_tokens / 1000000) * 0.06 + (out_tokens / 1000000) * 0.24
            else:
                cost = (in_tokens / 1000000) * 0.80 + (out_tokens / 1000000) * 3.20
                
            st.session_state.token_tracker[model_name]["input"] += in_tokens
            st.session_state.token_tracker[model_name]["output"] += out_tokens
            st.session_state.token_tracker[model_name]["cost"] += cost
            
            return text
    except Exception as exc:
        st.warning(f"{label} failed, using demo fallback. Details: {exc}")
        return fallback


def build_verdict(
    runbook: str,
    runbook_sections: dict,
    questions: str,
    senior_answers: str,
    analysis: str,
    updated_runbook: str,
    pipeline_path: str,
) -> dict:
    verdict = {
        "module": "Team 9 - Runbook Guardian",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_reviewed": pipeline_path,
        "runbook_sections": runbook_sections,
        "genuine_pipeline_issue": (
            "Silver marks unmatched merchants as UNMATCHED but does not stop publishing "
            "when unmatched merchant enrichment spikes."
        ),
        "what_ai_got_wrong": (
            "A naive runbook describes rerun steps but misses the missing quality gate: "
            "there is no recovery procedure for bad Silver output if the pipeline never fails."
        ),
        "runbook_markdown": runbook,
        "junior_questions": questions,
        "senior_answers": senior_answers,
        "gap_analysis": analysis,
        "updated_runbook": updated_runbook,
    }
    with open(VERDICT_PATH, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2)
    return verdict


pipeline_code, pipeline_path = read_pipeline_from_input()

st.sidebar.header("AI Usage")
usage_placeholder = st.sidebar.empty()

def update_usage_sidebar():
    lite = st.session_state.token_tracker["nova-lite"]
    pro = st.session_state.token_tracker["nova-pro"]
    total_cost = lite['cost'] + pro['cost']
    
    health_color = "gray"
    health_text = "Unknown"
    
    if "pipeline_health" in st.session_state:
        if st.session_state.pipeline_health == "red":
            health_color = "#FF4B2B"
            health_text = "Needs Attention"
        elif st.session_state.pipeline_health == "yellow":
            health_color = "#FFD700"
            health_text = "Minor Issues"
        elif st.session_state.pipeline_health == "green":
            health_color = "#00C851"
            health_text = "Looks Good"
            
    usage_placeholder.markdown(f"""
**Amazon Nova Lite**
- Input Tokens: {lite['input']:,}
- Output Tokens: {lite['output']:,}
- Cost: ${lite['cost']:.4f}

**Amazon Nova Pro**
- Input Tokens: {pro['input']:,}
- Output Tokens: {pro['output']:,}
- Cost: ${pro['cost']:.4f}

**Total Cost: ${total_cost:.4f}**

---
<div style="display: flex; align-items: center; margin-top: 10px;">
    <span style="height: 14px; width: 14px; background-color: {health_color}; border-radius: 50%; display: inline-block; margin-right: 8px; box-shadow: 0 0 5px {health_color};"></span>
    <strong>Health:</strong>&nbsp; {health_text}
</div>
""", unsafe_allow_html=True)

update_usage_sidebar()

evidence = load_evidence()

st.title("Runbook Guardian")
st.caption("Sigma DataTech AI Ops Platform - Day 9")

st.subheader("Mission")
st.write(
    "Generate an operational runbook for the Silver pipeline, let a junior engineer "
    "challenge it, then close the gaps before the next 3 AM incident."
)

with st.expander("Pipeline Source", expanded=False):
    st.write(pipeline_path)
    st.code(pipeline_code[:12000] or "No pipeline code found.", language="python")

with st.expander("DuckDB Evidence", expanded=False):
    if "error" in evidence:
        st.error(evidence["error"])
    else:
        st.write("Tables:", ", ".join(evidence["tables"]))
        for key, value in evidence.items():
            if key != "tables":
                st.write(key.replace("_", " ").title())
                st.dataframe(value, width="stretch")

if "pipeline_path" not in st.session_state or st.session_state.pipeline_path != pipeline_path:
    st.session_state.pipeline_path = pipeline_path
    st.session_state.runbook_sections = fallback_runbook_sections(pipeline_path)
    st.session_state.runbook = sections_to_markdown(st.session_state.runbook_sections)

if "questions" not in st.session_state:
    st.session_state.questions = fallback_questions()
if "senior_answers" not in st.session_state:
    st.session_state.senior_answers = fallback_senior_answers()
if "analysis" not in st.session_state:
    st.session_state.analysis = fallback_gap_analysis()
if "updated_runbook" not in st.session_state:
    st.session_state.updated_runbook = fallback_updated_runbook()

run_all = st.button("Run 3-Round AI Review", type="primary")

if run_all:
    system_runbook = "You are a principal data reliability engineer writing precise operational runbooks."
    user_runbook = f"""Create a complete operational runbook for this pipeline.
Return ONLY valid JSON with exactly these string keys:
setup, normal_run, failure_scenarios, validation_steps, escalation_path, known_gaps.

Requirements:
- setup: environment, dependencies, permissions, input/output paths
- normal_run: exact sequence an on-call engineer should follow
- failure_scenarios: table or bullets covering symptoms and first actions
- validation_steps: concrete checks after the run
- escalation_path: who to call and when
- known_gaps: operational or pipeline risks the code does not fully handle

PIPELINE PATH:
{pipeline_path}

PIPELINE CODE:
{pipeline_code[:14000]}

DUCKDB EVIDENCE:
{evidence}
"""
    runbook_json = safe_ai_call(
        "Round 1: Nova Pro writing runbook",
        call_nova_pro,
        system_runbook,
        user_runbook,
        2200,
        json.dumps(fallback_runbook_sections(pipeline_path)),
    )
    st.session_state.runbook_sections = parse_runbook_sections(runbook_json, pipeline_path)
    st.session_state.runbook = sections_to_markdown(st.session_state.runbook_sections)

    system_junior = "You are a careful junior data engineer reading an incident runbook at 3 AM."
    user_junior = f"""Read this runbook and ask exactly 5 numbered questions.
Some questions can be basic, but at least one must expose a real operational or pipeline gap.

RUNBOOK:
{st.session_state.runbook}
"""
    st.session_state.questions = safe_ai_call(
        "Round 2: Nova Lite simulating junior engineer",
        call_nova_lite,
        system_junior,
        user_junior,
        900,
        fallback_questions(),
    )

    system_senior = "You are the senior on-call data engineer answering a junior engineer during an incident."
    user_senior = f"""Answer each junior question directly and practically.
Use the runbook and pipeline code. If the right answer exposes a code or runbook gap, say so clearly.
Return exactly 5 numbered answers.

PIPELINE CODE:
{pipeline_code[:14000]}

RUNBOOK:
{st.session_state.runbook}

JUNIOR QUESTIONS:
{st.session_state.questions}
"""
    st.session_state.senior_answers = safe_ai_call(
        "Round 2b: Senior answering junior questions",
        call_nova_pro,
        system_senior,
        user_senior,
        1200,
        fallback_senior_answers(),
    )

    system_analysis = "You are the team lead closing runbook and pipeline-readiness gaps."
    user_analysis = f"""Classify each question as RUNBOOK GAP, GOOD QUESTION, or UNNECESSARY.
Then identify the one question that reveals a real pipeline issue, not merely a documentation gap.
Finally produce an updated runbook snippet for the most critical gap.

PIPELINE CODE:
{pipeline_code[:14000]}

RUNBOOK:
{st.session_state.runbook}

JUNIOR QUESTIONS:
{st.session_state.questions}

SENIOR ANSWERS:
{st.session_state.senior_answers}
"""
    st.session_state.analysis = safe_ai_call(
        "Round 3: Nova Pro gap analysis",
        call_nova_pro,
        system_analysis,
        user_analysis,
        1800,
        fallback_gap_analysis() + "\n\n" + fallback_updated_runbook(),
    )
    st.session_state.updated_runbook = fallback_updated_runbook()
    
    gap_count = st.session_state.analysis.upper().count("RUNBOOK GAP")
    if gap_count >= 2:
        st.session_state.pipeline_health = "red"
    elif gap_count == 1:
        st.session_state.pipeline_health = "yellow"
    else:
        st.session_state.pipeline_health = "green"
        
    update_usage_sidebar()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    [
        "Operational Runbook",
        "Junior Q&A",
        "Senior Answers",
        "Gap Analysis",
        "The Hidden Trap",
        "Interactive Q&A"
    ]
)

with tab1:
    st.markdown(st.session_state.runbook)

with tab2:
    st.subheader("Junior Questions")
    st.markdown(st.session_state.questions)

with tab3:
    st.subheader("Senior Answers")
    st.markdown(st.session_state.senior_answers)

with tab4:
    st.markdown(st.session_state.analysis)
    st.divider()
    st.markdown(st.session_state.updated_runbook)

with tab5:
    st.subheader("Genuine Pipeline Issue Revealed")
    st.error(
        "**The Trap:** The pipeline can publish Silver output even when merchant enrichment fails at scale. "
        "It flags `UNMATCHED` merchants, but there is no threshold-based fail-fast gate to stop it."
    )
    st.subheader("What the Initial Runbook Misses")
    st.write(
        "The first AI runbook draft typically documents reruns and ownership, but it misses the "
        "critical case where the job technically succeeds (runs to completion), but the data itself is unsafe."
    )
    st.subheader("The Code Fix Required")
    st.write(
        "Clear validation thresholds must be added to the pipeline code itself. The pipeline must throw an error "
        "and stop execution before corrupt or highly unmatched data reaches downstream analytics."
    )

with tab6:
    st.subheader("Interactive Q&A")
    st.write("Ask any questions about the pipeline code or the runbook.")
    
    user_q = st.text_area("Your question:", key="qa_input", height=100)
    if st.button("Ask AI", key="qa_btn"):
        if user_q.strip():
            qa_system = "You are an expert Data Engineer. Answer the user's question about the pipeline code and runbook clearly and concisely."
            qa_user = f"PIPELINE CODE:\n{pipeline_code[:14000]}\n\nRUNBOOK:\n{st.session_state.runbook}\n\nQUESTION:\n{user_q}"
            
            qa_response = safe_ai_call(
                "Answering Question",
                call_nova_pro,
                qa_system,
                qa_user,
                max_tokens=1000,
                fallback="Demo fallback: Cannot answer right now."
            )
            st.markdown("### Answer")
            st.markdown(qa_response)
            update_usage_sidebar()
        else:
            st.warning("Please enter a question.")

verdict = build_verdict(
    st.session_state.runbook,
    st.session_state.runbook_sections,
    st.session_state.questions,
    st.session_state.senior_answers,
    st.session_state.analysis,
    st.session_state.updated_runbook,
    pipeline_path,
)

st.success(f"Verdict saved to {VERDICT_PATH}")
st.download_button(
    "Download verdict.json",
    data=json.dumps(verdict, indent=2),
    file_name="verdict.json",
    mime="application/json",
)
