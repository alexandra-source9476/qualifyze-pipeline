# Part 2: NCR Monitoring and Alert System

## Overview

This document proposes a system to continuously monitor EudraGMDP for new Non-Compliance Reports (NCRs), notify all relevant stakeholders in a timely manner, and maintain a full history of compliance events that can be used for risk analysis and supplier recommendations.

The system builds directly on the pipeline from Part 1. Most of the data infrastructure is already in place — this part adds the monitoring loop, the notification layer, and the event history that makes the system useful beyond simple alerting.

---

## Assumptions

1. **EudraGMDP has no public API or RSS feed.** After investigating the site, no programmatic access is available. The only way to retrieve data is by downloading the Excel export. The monitoring system automates this download using a headless browser on a schedule.

2. **Polling frequency is set to every 6 hours as a starting point.** Based on historical observation of EudraGMDP, NCRs appear rarely — a few per month. Six hours is a reasonable balance between timeliness and infrastructure cost. This frequency should be agreed with the business team, as it directly affects how quickly clients are notified.

3. **Stakeholders are notified in two steps.** First, the Qualifyze internal team receives an alert. Then, each client who uses the affected site is notified individually. Clients are never notified before the internal team has visibility.

4. **The decision to exclude or reinstate a supplier belongs to the client.** Qualifyze's role is to detect, notify, and provide recommendations. Acting on the information — stopping orders, finding alternatives — is the client's responsibility.

5. **NCR withdrawal is detectable by absence.** When a site's GMPNC document disappears from the EudraGMDP export, or a new GMPC is issued for the same site, the system treats this as a resolution. A separate notification is sent for resolutions, following the same stakeholder flow as detections.

6. **NCR reissuance is a calculated property, not a separate event type.** An NCR is considered reissued if a new `ncr_detected` event follows a previous `ncr_resolved` event for the same site. This avoids additional detection logic.

7. **The grace period before a resolved supplier can be recommended again** is a business decision to be defined with the business team. A reasonable starting point is 12 months from the resolution date, but this may vary by client or product category.

8. **Supply chain propagation is limited to direct relationships known to Qualifyze.** If Qualifyze does not have visibility into a supplier's own supply chain, indirect NCR impacts cannot be detected automatically.

---

## What Gets Reused from Part 1

The following already exist and are reused without changes:

- The ingestion pipeline — the same Excel file downloaded by the monitoring job goes through the same raw → bronze → silver → gold flow
- `silver_compliance_documents` — already contains GMPNC records with full metadata
- `gold_site_compliance_view` — automatically reflects resolved NCRs when silver rebuilds

What is added on top:

- An automated fetch job that replaces manual Excel download
- An event log (`ncr_events`) that tracks every status change per site
- A current status table (`ncr_current_status`) that summarises the compliance picture per site
- A notification system that routes alerts to the right people at the right time
- A resolution detection flow that mirrors the detection flow in reverse

---

## Data Model Additions

**`ncr_events`** — append-only log of every compliance status change detected. Never updated, only appended. This is the source of truth for all historical analysis.

| Column | Notes |
|--------|-------|
| event_id | UUID per event |
| oms_location_id | the site |
| source_document_id | eudragmdp_doc_ref |
| event_type | ncr_detected or ncr_resolved |
| event_timestamp | when our system detected the change |
| detected_in_batch_id | which pipeline run produced this event |
| issue_date | date from EudraGMDP |
| qualifyze_site_id | if the site is in our internal registry |
| is_reissued | calculated — true if a resolved event exists before this detected event for the same site |

**`ncr_current_status`** — rebuilt on every run, shows the current compliance picture per site.

| Column | Notes |
|--------|-------|
| oms_location_id | |
| qualifyze_site_id | |
| current_status | compliant or non_compliant |
| active_ncr_count | how many active NCRs right now |
| total_ncr_count | total across all history |
| first_ncr_date | |
| last_ncr_date | |
| last_resolved_date | |
| avg_resolution_days | average time to resolve, calculated from ncr_events |

**`ncr_notifications`** — one row per notification sent, tracking who was notified and when.

| Column | Notes |
|--------|-------|
| notification_id | UUID |
| event_id | FK to ncr_events |
| recipient_type | internal / client |
| recipient_id | Qualifyze team or client ID |
| channel | email / push / dashboard |
| sent_at | |
| status | pending / sent / failed |

---

## System Components and Responsibilities

### 1. Automated Fetch Job

A scheduled job runs every 6 hours. It opens the EudraGMDP search page using a headless browser (Playwright), submits the search form without filters to retrieve all records, downloads the resulting Excel file, and places it in the storage layer. The job then triggers the Part 1 pipeline with this file as input.

This replaces the manual download assumed in Part 1. No changes to the pipeline itself are needed.

Tools: Playwright, Dagster scheduled job, AWS S3.

### 2. Change Detection

After each pipeline run completes and silver is rebuilt, a comparison job checks what changed compared to the previous run:

- New GMPNC documents that were not in `ncr_events` → insert `ncr_detected` event
- GMPNC documents that were in the previous silver but are absent from the current one → insert `ncr_resolved` event

The current status table is rebuilt after each event insertion.

Tools: dbt model or Python op in Dagster, PostgreSQL.

### 3. Supply Chain Propagation

If Qualifyze holds information about supplier relationships — for example, that Supplier A provides raw material to Supplier B, who is a client's direct supplier — an NCR on Supplier A generates an indirect alert to the client. The alert is clearly marked as indirect, giving the client context without creating false urgency.

This is only possible when Qualifyze has visibility into these relationships. It is a future capability that depends on how richly the internal site registry captures supplier dependencies.

Tools: an additional relationship table in the internal data model, a propagation query at notification time.

### 4. Internal Notification — Qualifyze Team

When an `ncr_detected` event is inserted, an alert goes immediately to the Qualifyze internal team via two channels:

- Email to the team distribution list, including site name, country, NCR reference, and a list of affected clients
- An alert appears in the monitoring dashboard (Datadog or Grafana, already used by Qualifyze) under a dedicated NCR panel, visible in real time

A person on rotation within Qualifyze receives the alert and takes ownership of following up with affected clients and preparing supplier recommendations.

Tools: AWS SES for email, Datadog or Grafana for dashboard, existing on-call rotation tooling.

### 5. Client Notification

For each client in the Qualifyze system who uses the affected site, a personalised notification is sent:

- Email with the site name, NCR date, and a link to the Qualifyze platform for more details
- Push notification if the Qualifyze mobile app is available
- An in-app alert visible the next time the client logs in

The same flow runs in reverse when an NCR is resolved — clients are notified that the supplier has returned to compliance, with the caveat that reinstatement is their own decision.

Tools: AWS SES, Firebase Cloud Messaging for push, in-app notification system.

### 6. Supplier Recommendation Engine

When an NCR is detected, an automated process searches the Qualifyze database for alternative suppliers. Candidates must:

- Have the same or overlapping manufacturing scope as the affected site
- Hold an active GMPC with no current NCRs
- Have no NCR history in the past 12 months — or whatever grace period is agreed with the business team
- Be in a geographically accessible region for the client

Candidates are ranked and sent to the person on rotation, who reviews and forwards to the affected clients. The ranking can start as a rules-based SQL scoring system and evolve into an ML model as more data accumulates.

Tools: dbt scoring model or Python service, surfaced through the Qualifyze platform.

### 7. Qualifyze Platform — Client-Facing Dashboard

The Qualifyze platform surfaces all of the above to clients in one place: active alerts for their suppliers, a compliance history timeline per supplier, and the list of recommended alternatives with side-by-side comparison of scope, location, cost indicators, and compliance score.

The compliance score is derived from `ncr_current_status` — total NCR count, average resolution time, time since last NCR. A supplier with one resolved NCR three years ago scores very differently from one with two active NCRs.

Tools: existing Qualifyze platform, new views backed by `ncr_current_status` and `ncr_events`.

### 8. ML Model — Supplier Stability Prediction

A model trained on `ncr_events` history predicts the probability that a supplier will receive an NCR in the next 6 months. Features include NCR frequency, resolution time, country, time since last inspection, and whether the supplier is in a geo-risk zone. Output is a risk score shown alongside supplier profiles.

This is a future capability that requires sufficient historical data to train reliably.

Tools: Python, scikit-learn or XGBoost, MLflow for experiment tracking, served via the Qualifyze platform.

### 9. Geo-Risk Detection

If three or more NCRs are detected from the same country within a 30-day window, the system generates an internal alert to the Qualifyze team. This may indicate a change in inspection standards by a national authority rather than isolated site issues. This alert stays internal — it is a signal for the team to investigate, not something to surface to clients directly without further analysis.

Tools: a simple dbt model or scheduled query on `ncr_events`, internal email alert.

---

## NCR Lifecycle

```
EudraGMDP publishes NCR
        ↓
Polling job detects it (within 6 hours)
        ↓
Pipeline runs → ncr_detected event inserted
        ↓
Internal team notified (email + dashboard)
        ↓
Affected clients notified (email + push)
        ↓
Recommendation engine runs → alternatives sent to rotation person
        ↓
Rotation person reviews and forwards to clients
        ↓
        ... time passes ...
        ↓
EudraGMDP removes NCR / issues new GMPC
        ↓
Polling job detects resolution
        ↓
ncr_resolved event inserted
        ↓
Internal team notified of resolution
        ↓
Affected clients notified — supplier returned to compliance
        ↓
Supplier eligible for reinstatement after grace period
```

---

## Improvements

**Severity scoring.** EudraGMDP does not publish a severity rating for NCRs. A proxy can be derived from `ncr_current_status` — a site with multiple NCRs and long resolution times is treated as higher risk. True severity scoring would require richer data from EMA, which is not currently available publicly.

**Webhook or API from EMA.** If EMA ever introduces a programmatic interface for EudraGMDP, the polling job can be replaced entirely. The rest of the system would remain unchanged.

**Client-configurable grace periods.** Different clients may have different risk tolerances. A configuration layer per client would allow each to set their own grace period and notification preferences.
