markdown
# NL2SQL vs Cortex Analyst — Sigma DataTech Evaluation
Team: [Your team name]
Date: [Today's date]

## 5-Question Head-to-Head Results

| # | Question | Module 2 SQL Correct? | Cortex SQL Correct? | Module 2 Time | Cortex Time |
|---|----------|--------------------|---------------------|------------|-------------|
| 1 | Total transaction count | YES/NO | YES/NO | ~Xs | ~Xs |
| 2 | Failed transaction count | YES/NO | YES/NO | ~Xs | ~Xs |
| 3 | Highest revenue merchant | YES/NO | YES/NO | ~Xs | ~Xs |
| 4 | Failure rate by payment method | YES/NO | YES/NO | ~Xs | ~Xs |
| 5 | Total revenue (with COMPLETED filter) | YES/NO | YES/NO | ~Xs | ~Xs |

## Observations

### Where Module 2 NL2SQL was better:
(Fill in based on your results)

### Where Cortex Analyst was better:
(Fill in based on your results)

### Business Rule Accuracy
Question 5 is the critical test — revenue must only count COMPLETED
transactions. Did both systems apply this rule correctly?
- Module 2: [Did it use CASE WHEN STATUS='COMPLETED'?]
- Cortex: [Did it use the metric definition from the YAML?]

## Your Recommendation

Which approach would you deploy at Sigma DataTech for production self-serve
analytics, and why?

Consider:
- Setup effort (Module 2: 200 lines of Python + prompt. Cortex: YAML + API call)
- Maintenance (Module 2: update prompt for new tables. Cortex: update YAML)
- Accuracy (your observed results above)
- Cost (Nova Pro API calls vs Snowflake credit consumption)
- Data residency (Module 2: data leaves Snowflake to Bedrock. Cortex: stays inside Snowflake)
- Scalability (Module 2: you maintain schema context. Cortex: semantic model scales)

Your recommendation: [Module 2 NL2SQL / Cortex Analyst / Hybrid approach]
Reason: (2-3 sentences)