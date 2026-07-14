# EDE Intake Ticket Classifier

Classifies unassigned EDE front-door JIRA intake tickets into one of five
teams (C360, B360, Financial Insights, Reliability, Metadata Management) or
"Other", using an LLM via FuelIX. Reads from BigQuery, writes results back
to BigQuery.

## Dependencies

- Python 3.10+
- Google Cloud SDK

```powershell
pip install google-cloud-bigquery python-dotenv openai
```

## Setup

### 1. Get a FuelIX API key

1. Go to **https://dev.fuelix.ai/en/apps**
2. Select **New Personal Project** and create a new project to have an API Key generated
3. Copy the generated API key

### 2. Create a `.env` file in the project folder

```
FUELIX_API_KEY=your_fuelix_key_here
FUELIX_MODEL=claude-sonnet-4-5 (optional. It defaults to claude-sonnet-4-5 if not declared)
```

### 3. Authenticate to BigQuery (one-time per machine)

```powershell
gcloud auth application-default login
```

If you get a `403` error mentioning `serviceusage.services.use` or "quota
project," also run:

```powershell
gcloud auth application-default set-quota-project YOUR_GCP_PROJECT_ID
```

(any GCP project you have basic access to works here - it doesn't have to
match where the data lives)

## Running it

Classify a specific ticket:
```powershell
python ede_project_classifier.py --ticket-key EDE-1473
```

Multiple tickets:
```powershell
python ede_project_classifier.py --ticket-key EDE-1473,EDE-1500
```

Batch mode - process up to N new/unclassified tickets (uses the created_at date in descending order):
```powershell
python ede_project_classifier.py --limit 20
```
