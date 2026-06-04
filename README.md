# A Hackathon project - Confluent cloud hacakthon in May 2026
# OpsPilot: AI-Powered Streaming Incident Investigator (my project)

OpsPilot is a practical observability demo that uses Confluent Cloud, Kafka, Flink SQL, Azure OpenAI, Schema Registry, and Splunk to turn raw production-style logs into incident summaries that an engineering team can act on.

The goal is simple: when production logs show repeated failures for a service, the system should detect the pattern in real time, ask an AI agent to explain what is probably happening, and send the result to Splunk so engineers can see it in the same place where they already investigate incidents.

This project was originally inspired by a hackathon flow, but the setup below is independent. It uses your own Confluent Cloud, Azure OpenAI, and Splunk credentials.

## Problem Statement (Designed and implemented during hackathon)

Production incidents usually start with noisy logs. A service may emit many lines across application logs, platform logs, Kubernetes operator logs, Kafka logs, connector logs, and downstream dependency logs.

During an incident, engineers often spend time answering basic questions before the real fix can start:

- Which service is failing?
- Is this one bad log line or a repeated issue?
- Is it application, infrastructure, Kafka, connector, network, authentication, or downstream dependency related?
- What is the likely customer impact?
- What should the first remediation step be?
- What Splunk query should the team run next?

OpsPilot tries to reduce that first investigation step. It does not replace engineers. It gives them a faster first read of the incident.

## Solution

OpsPilot streams production-style logs into Kafka, processes them with Flink SQL, detects high-error windows, calls an AI incident agent, and writes the AI result to Splunk.

```text
Production-style logs
        |
        v
Kafka topic: opspilot_logs_raw_v2
        |
        v
Confluent Flink SQL
        |
        +--> critical_alerts_live
                  |
                  v
        Confluent AI Agent using Azure OpenAI
                  |
                  v
Kafka topic / Flink table: ai_incident_analysis
                  |
                  v
Splunk Sink Connector
                  |
                  v
Splunk Search / Dashboard
```

## Impact

The main value is faster incident triage.

Instead of asking every engineer to manually inspect logs and infer the problem from scratch, OpsPilot creates a first incident explanation:

- Incident summary
- Probable root cause
- Customer or business impact
- Severity reasoning
- Recommended remediation
- Splunk search query for deeper investigation
- Leadership-friendly summary

This can help reduce the time spent in the early part of a production call, especially when multiple services or platform components are producing logs at the same time.

## Technologies Used

- Confluent Cloud
- Apache Kafka
- Confluent Flink SQL
- Confluent Schema Registry / Stream Governance
- Confluent AI Agent
- Azure OpenAI
- Splunk Cloud
- Splunk HTTP Event Collector
- Splunk Sink Connector
- Optional Python producer for local test data

## Demo Flow

1. Produce log events into Kafka topic `opspilot_logs_raw_v2`.
2. Flink reads the raw log stream.
3. Flink groups errors by service in a 1-minute window.
4. If a service has 3 or more errors in that window, Flink creates a critical alert.
5. The AI agent analyzes the alert.
6. The AI result is written to `ai_incident_analysis`.
7. Splunk Sink Connector reads `ai_incident_analysis`.
8. Splunk displays the incident analysis.

## Prerequisites

You need accounts and credentials for:

- Confluent Cloud
- Azure OpenAI
- Splunk Cloud or Splunk Enterprise with HEC enabled

You also need these tools if you want to run the optional local producer:

- Git
- Python
- uv
- Confluent CLI

For the simplest demo, you can skip the local producer and publish messages directly from the Confluent Cloud topic UI.

## Cost Notes

This is not a fully free production system because it uses managed cloud services. For a small demo, the cost can be kept low by stopping resources when not in use.

Typical cost drivers:

- Confluent Cloud Kafka cluster
- Confluent Flink compute while streaming statements are running
- Fully managed Splunk Sink Connector tasks
- Azure OpenAI token usage
- Splunk ingest volume and retention

For a short internal demo, costs are usually low if:

- Flink streaming statements are stopped after the demo
- Splunk Sink Connector is paused after the demo
- Only a small number of test messages are produced
- Azure OpenAI is used only for a few incident summaries

For a company running this continuously, cost depends on log volume, alert frequency, retention, connector runtime, and how often the AI model is called. A sensible production design should avoid calling AI for every raw log line. AI should only run after Flink has reduced the stream to meaningful alert events.

## Kafka Topic

Raw logs are written to:

```text
opspilot_logs_raw_v2
```

Final AI incident output is written to:

```text
ai_incident_analysis
```

## Data Contract

The raw log topic uses JSON Schema through Schema Registry.

Expected log fields:

```json
{
  "app": "eventlayer-platform",
  "level": "ERROR",
  "service": "kafka-connect-operator",
  "message": "Connector apply failed because Kafka Connect worker has zero ready pods",
  "event_time": "2026-06-04T19:12:27.000Z",
  "trace_id": "trace-demo-20260604-1512-connect-001",
  "env": "prod",
  "region": "useast4"
}
```

Recommended JSON Schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OpsPilotLog",
  "type": "object",
  "properties": {
    "app": { "type": "string" },
    "level": { "type": "string", "enum": ["INFO", "WARN", "ERROR", "DEBUG"] },
    "service": { "type": "string" },
    "message": { "type": "string" },
    "event_time": { "type": "string", "format": "date-time" },
    "trace_id": { "type": "string" },
    "env": { "type": "string", "enum": ["dev", "qa", "stage", "prod"] },
    "region": { "type": "string" }
  },
  "required": ["app", "level", "service", "message", "event_time", "trace_id", "env", "region"],
  "additionalProperties": false
}
```

## Azure OpenAI Setup

Create an Azure OpenAI or Azure AI Foundry resource and deploy a chat model.

For this demo:

- Deployment name: `gpt-5-mini`
- Authentication: API key
- Endpoint format should look like:

```text
https://<azure-resource-name>.services.ai.azure.com/openai/deployments/<deployment-name>/chat/completions?api-version=<api-version>
```

In Confluent Flink, create a connection and model alias:

```sql
CREATE CONNECTION `azure_openai_connection`
WITH (
  'type' = 'azureopenai',
  'endpoint' = 'https://<azure-resource-name>.services.ai.azure.com/openai/deployments/gpt-5-mini/chat/completions?api-version=2024-08-01-preview',
  'api-key' = '<your-azure-openai-api-key>'
);

CREATE MODEL `llm_textgen_model`
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'azureopenai',
  'task' = 'text_generation',
  'azureopenai.connection' = 'azure_openai_connection',
  'azureopenai.model_version' = '2025-08-07',
  'azureopenai.PARAMS.max_completion_tokens' = '4096'
);

SHOW MODELS;
```

You should see:

```text
llm_textgen_model
```

## Flink Tables

Create the raw log table:

```sql
CREATE TABLE IF NOT EXISTS opspilot_logs_raw_v2 (
  app STRING,
  `level` STRING,
  service STRING,
  message STRING,
  event_time STRING,
  trace_id STRING,
  env STRING,
  region STRING,
  event_ts AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'value.format' = 'json-registry'
);
```

Create the AI output table:

```sql
CREATE TABLE IF NOT EXISTS ai_incident_analysis (
  service STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  error_count BIGINT,
  severity STRING,
  latest_error STRING,
  incident_summary STRING,
  probable_root_cause STRING,
  impact STRING,
  severity_assessment STRING,
  recommended_action STRING,
  splunk_search STRING,
  leadership_summary STRING,
  incident_analysis_json STRING,
  recommended_splunk_query STRING,
  raw_agent_response STRING
) WITH (
  'value.format' = 'json-registry'
);
```

## Flink Critical Alert Query

Start this in Streaming mode before publishing demo messages.

```sql
CREATE TABLE critical_alerts_live
WITH ('changelog.mode' = 'append')
AS
SELECT
  service,
  window_start,
  window_end,
  COUNT(*) AS error_count,
  'HIGH' AS severity,
  MAX(message) AS latest_error,
  MAX(trace_id) AS sample_trace_id,
  MAX(app) AS app,
  MAX(env) AS env,
  MAX(region) AS region
FROM TABLE(
  TUMBLE(TABLE opspilot_logs_raw_v2, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
WHERE `level` = 'ERROR'
  AND service IS NOT NULL
GROUP BY window_start, window_end, service
HAVING COUNT(*) >= 3;
```

This query is not tied to a specific trace ID. Any service with 3 or more errors in a 1-minute window becomes a critical alert.

## AI Agent

Create the incident investigation agent:

```sql
CREATE AGENT `log_incident_agent`
USING MODEL `llm_textgen_model`
USING PROMPT 'You are an intelligent AI production incident investigator for a real-time streaming observability platform.

Your workflow:
1. Analyze the incoming production alert including service name, error count, severity, latest error message, environment, region, and time window.
2. Identify the most likely root cause in simple engineering language.
3. Evaluate customer and business impact.
4. Classify the issue as infrastructure, application, downstream dependency, database, configuration, network, authentication, Kafka streaming, or unknown.
5. Recommend immediate remediation and troubleshooting actions.
6. Generate a useful Splunk search query for deeper investigation.
7. Create a JSON incident analysis response with this exact structure:
   {
     "incident_summary": "<short summary>",
     "probable_root_cause": "<root cause>",
     "impact": "<customer or business impact>",
     "severity_assessment": "<severity reasoning>",
     "recommended_action": "<next remediation step>",
     "splunk_search": "index=main service=<service> severity=HIGH",
     "leadership_summary": "<executive summary>"
   }

Format your final response with these THREE sections:

Incident Summary:
<short summary of incident>

Incident Analysis JSON:
<generated JSON>

Recommended Splunk Query:
<splunk query>

CRITICAL INSTRUCTIONS:
- Always explain incidents in simple operational language.
- Always provide concise actionable remediation guidance.
- Always include a valid Splunk query.
- Never ask for clarification.
- Assume all incidents are from live production systems.
- Prioritize customer impact and operational urgency.
- Keep responses concise and suitable for real-time dashboards.'
WITH (
  'max_iterations' = '10'
);
```

## Write AI Output to Final Topic

Start this in Streaming mode after the agent is created.

```sql
INSERT INTO ai_incident_analysis
SELECT
  service,
  window_start,
  window_end,
  error_count,
  severity,
  latest_error,
  incident_summary,
  JSON_VALUE(incident_analysis_json, '$.probable_root_cause') AS probable_root_cause,
  JSON_VALUE(incident_analysis_json, '$.impact') AS impact,
  JSON_VALUE(incident_analysis_json, '$.severity_assessment') AS severity_assessment,
  JSON_VALUE(incident_analysis_json, '$.recommended_action') AS recommended_action,
  JSON_VALUE(incident_analysis_json, '$.splunk_search') AS splunk_search,
  JSON_VALUE(incident_analysis_json, '$.leadership_summary') AS leadership_summary,
  incident_analysis_json,
  recommended_splunk_query,
  raw_agent_response
FROM (
  SELECT
    alert.service,
    alert.window_start,
    alert.window_end,
    alert.error_count,
    alert.severity,
    alert.latest_error,
    TRIM(REGEXP_EXTRACT(
      CAST(agent_response.response AS STRING),
      'Incident Summary:\\s*\\n([\\s\\S]+?)(?=\\n\\nIncident Analysis JSON:)',
      1
    )) AS incident_summary,
    TRIM(REGEXP_EXTRACT(
      CAST(agent_response.response AS STRING),
      'Incident Analysis JSON:\\s*\\n(?:```json\\s*)?([\\s\\S]+?)(?:```)?(?=\\n\\nRecommended Splunk Query:)',
      1
    )) AS incident_analysis_json,
    TRIM(REGEXP_EXTRACT(
      CAST(agent_response.response AS STRING),
      'Recommended Splunk Query:\\s*\\n([\\s\\S]+)$',
      1
    )) AS recommended_splunk_query,
    CAST(agent_response.response AS STRING) AS raw_agent_response
  FROM critical_alerts_live AS alert,
  LATERAL TABLE(AI_RUN_AGENT(
    `log_incident_agent`,
    CONCAT(
      'Production alert detected. ',
      'service=', alert.service,
      ', severity=', alert.severity,
      ', error_count=', CAST(alert.error_count AS STRING),
      ', latest_error=', alert.latest_error,
      ', app=', alert.app,
      ', env=', alert.env,
      ', region=', alert.region,
      ', window_start=', CAST(alert.window_start AS STRING),
      ', window_end=', CAST(alert.window_end AS STRING),
      '. Generate the incident analysis now.'
    )
  )) AS agent_response
) AS extracted;
```

## Why AI_RUN_AGENT Is Used

The AI agent is not a destination table by itself. It is called inside the Flink pipeline.

The flow is:

```text
critical_alerts_live
      |
      v
AI_RUN_AGENT(log_incident_agent, alert text)
      |
      v
agent response
      |
      v
ai_incident_analysis
```

So `AI_RUN_AGENT` is the function call that sends each alert to the agent. The `INSERT INTO ai_incident_analysis` writes the returned response to the final Kafka-backed table.

## Splunk HEC Setup

In Splunk:

1. Go to Settings.
2. Open Data Inputs.
3. Open HTTP Event Collector.
4. Create a new token.
5. Use index `main`.
6. Use source `confluent_ai_agent`.
7. Use sourcetype `_json`.
8. Copy the HEC token.

The HEC URL usually looks like:

```text
https://<splunk-cloud-host>:8088
```

## Splunk Sink Connector

In Confluent Cloud:

1. Open the Kafka cluster.
2. Go to Connectors.
3. Search for `Splunk Sink`.
4. Choose the fully managed `Splunk Sink` connector.
5. Select topic `ai_incident_analysis`.
6. Create or select Kafka API credentials.
7. Enter Splunk HEC URL.
8. Enter Splunk HEC token.
9. Set SSL certificate validation based on your Splunk Cloud setup.
10. Set value format to `JSON_SR`.
11. Set Splunk index to `main`.
12. Set Splunk source to `confluent_ai_agent`.
13. Set Splunk sourcetype to `_json`.
14. Use 1 task for demo.
15. Launch the connector.

For the demo environment, SSL validation may need to be set to:

```text
false
```

If it is set to `true` and no trust store is provided, the connector may fail with:

```text
Invalid HTTPS configuration. Certificate validation requires a valid trust store file.
```

## Splunk Search

Use this Splunk search to view AI-generated incidents:

```spl
index=main source=confluent_ai_agent
| spath
| table _time service severity error_count incident_summary probable_root_cause recommended_action splunk_search leadership_summary raw_agent_response
| sort - _time
```

For a demo, set the time range to:

```text
Last 15 minutes
```

If testing older events, use:

```text
All time
```
Splunk Query Result 

## Example Demo Messages

Publish at least 3 `ERROR` messages for the same service within the same 1-minute `event_time` window.

Example Kafka service mesh issue:

```json
{"app":"eventlayer-platform","level":"ERROR","service":"kafka-service-mesh","message":"Kafka broker TLS handshake failed for broker1-ssl-layer: inbound listener 30027 rejected upstream connection from service mesh sidecar","event_time":"2026-06-04T19:19:20.000Z","trace_id":"trace-demo-20260604-1519-kafka-tls-001","env":"prod","region":"useast4"}
```

```json
{"app":"eventlayer-platform","level":"ERROR","service":"kafka-service-mesh","message":"Kafka client connection reset while routing through service mesh to broker1-layer on port 3037","event_time":"2026-06-04T19:19:25.000Z","trace_id":"trace-demo-20260604-1519-kafka-tls-002","env":"prod","region":"useast4"}
```

```json
{"app":"eventlayer-platform","level":"ERROR","service":"kafka-service-mesh","message":"Kafka service mesh route degradation detected: repeated connection failures on broker SSL listener 3037 affecting event ingestion path","event_time":"2026-06-04T19:19:30.000Z","trace_id":"trace-demo-20260604-1519-kafka-tls-003","env":"prod","region":"useast4"}
```

## Verification Queries

Check raw logs:

```sql
SELECT *
FROM opspilot_logs_raw_v2
WHERE trace_id LIKE 'trace-demo-%';
```

Check critical alerts:

```sql
SELECT *
FROM critical_alerts_live
ORDER BY window_start DESC;
```

Check AI output:

```sql
SELECT *
FROM ai_incident_analysis
ORDER BY window_start DESC;
```

## Future Enhancements

- Add transaction-level correlation using `trace_id` or `transaction_id`.
- Detect missing end events for long-running transactions.
- Add service-specific alert thresholds.
- Add deduplication to avoid repeated AI calls for the same incident.
- Add severity scoring based on business criticality.
- Add Slack, Microsoft Teams, or PagerDuty notifications.
- Add runbook recommendations based on service name and incident type.
- Add dashboard filters by service, environment, region, and severity.
- Store incident history for trend analysis.
- Add feedback so engineers can mark AI recommendations as useful or not useful.
- Add anomaly detection based on historical baselines instead of static thresholds.
- Add deployment metadata so recent releases can be correlated with incidents.

## Final Summary

OpsPilot is a practical streaming incident investigation flow. It uses Kafka for log transport, Flink for real-time detection, Schema Registry for structured events, Azure OpenAI for incident explanation, and Splunk for operational visibility.

The useful part is not that it uses AI. The useful part is that AI is called only after the stream has already been reduced to a meaningful incident candidate. That keeps the system easier to reason about, easier to demo, and closer to something a real engineering team could build on.
