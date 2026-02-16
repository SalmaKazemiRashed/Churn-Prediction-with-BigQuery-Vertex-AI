# Churn Prediction

Predict which customers will churn based on behavioral inactivity, using:

- Synthetic customer + event data

- BigQuery for analytics & features

- Vertex AI AutoML for training & prediction

### Churn definition

A customer is churned if they have no activity for N days


The file structure is 

```plaintext
churn-prediction-gcp/
├── README.md
├── sql/
│   ├── 00_create_schemas.sql
│   ├── 01_generate_customers.sql
│   ├── 02_generate_events.sql
│   ├── 03_create_churn_labels.sql
│   ├── 04_create_features.sql
│   ├── 05_create_training_data.sql
│   ├── 06_create_prediction_input.sql 
│   ├── 07_create_churn_scored_users.sql
│   └── 08_find_churn_targets.sql

├── training/
│   └── train_vertex_automl.py
├── prediction/
│   └── batch_predict.py
└── requirements.txt
```

### Data Model

#### Customers
```plaintext
user_id, signup_data, country, plan, acquisition_channel
```

#### Events
```plaintext
user_id, event_name, event_time, platform
```

#### Churn Labels
```plaintext
user_id, churned
```

#### Feature Engineering
```plaintext
user_id, events_30d, logins_30d, purchases_30d, active_days_30d
```

#### Training Dataset
```plaintext
user_id, events_30d, logins_30d, purchases_30d, active_days_30d,churned
```

#### Vertex AI model
Activate billing account and link the account to the project and run python [code](training/train_vertex_automl.py)  


#### Batch prediction
Vertex AI works with project_ID and Model_ID. Run batch_predict.py [script](prediction/batch_predict.py)


### Stack and Environment Set up

###### - BigQuery  
data generation, labeling, feature engineering

###### - Vertex AI
model training + batch prediction

##### VS Code + BigQuery CLI (bq)

Install google cloud sdk through:
```bash
https://cloud.google.com/sdk/docs/install

```

Verify installation:

```bash
glcoud --version
bq version
```

login with gmail:

```bash
gcloud auth login
gcloud auth application-default login
```

For creating a project

- through [Link](https://console.cloud.google.com/)

- Create a project

It is verifiable through VS code terminal
```bash
gcloud projects list   ## Verify

gcloud config get-value project  ##verify

gcloud auth application-default set-quota-project project-ID  ##Fix quota project

gcloud services enable bigquery.googleapis.com  ##Enable BigQuery
```

Creating the project also doable through CLI 
```bash
gcloud projects create project_ID --name="project_name"
```

To validate BigQuery SQL:
```bash
bq query --use_legacy_sql=false "SELECT @@project_id AS project_id"
```


#### Synthetic Data

SQL -> generate synthetic data

```plaintext
├── sql/
│   ├── 00_create_schemas.sql
│   ├── 01_generate_customers.sql
│   ├── 02_generate_events.sql
│   ├── 03_create_churn_labels.sql
│   ├── 04_create_features.sql
│   ├── 05_create_training_data.sql
│   ├── 06_create_prediction_input.sql 
│   ├── 07_create_churn_scored_users.sql
│   └── 08_find_churn_targets.sql

```
For runing sql scripts:

Google Cloud Console → BigQuery

Also through python script

```bash
pip install google-cloud-bigquery

```

```Python
from google.cloud import bigquery

PROJECT_ID = "project-id"

client = bigquery.Client(project=PROJECT_ID)

sql = """
CREATE OR REPLACE TABLE `project-id.analytics.customers` AS
WITH base AS (
  SELECT
    GENERATE_UUID() AS user_id,
    DATE_SUB(CURRENT_DATE(), INTERVAL CAST(RAND() * 180 AS INT64) DAY) AS signup_date,
    country,
    plan,
    channel AS acquisition_channel
  FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS _
  CROSS JOIN UNNEST(["US","UK","DE","SE","CA"]) AS country
  CROSS JOIN UNNEST(["free","pro","enterprise"]) AS plan
  CROSS JOIN UNNEST(["organic","ads","referral"]) AS channel
)
SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER () <= 10000;
"""

job = client.query(sql)
job.result()  # Waits for completion

print("Customers table created.")

```


## Summary


1️⃣ Project & environment setup Summary

 Create a Google Cloud project (e.g. churn-prediction-v1)

Enable:

    BigQuery

    Vertex AI

Install locally:

    Google Cloud SDK (gcloud, bq)

    Python 3.9+

Authenticate:

    gcloud auth login

    gcloud config set project project_ID


2️⃣ Create BigQuery datasets (schemas)

Create logical datasets:


analytics → raw & analytical data

ml → features, training data, predictions


3️⃣ Generate synthetic customer data

Create realistic customer behavior using SQL:

```plaintext
user_id,  signup date, engagement signals

```

Store in BigQuery (analytics.customers, analytics.events)

Purpose:

- Avoid real Personally Identifiable Info (PII)

- Enable repeatable experiments

- Keep cost near zero


4️⃣ Build churn labels

Define churn logic (example):

No activity in last N days → churned = 1

Materialize labels table:

```plaintext
ml.churn_labels
(user_id, churned)
```


5️⃣ Feature engineering in BigQuery

Aggregate user behavior:

```plaintext
events_30d, logins_30d, purchases_30d, active_days_30d
```

Save as:

‍‍```plaintext
ml.user_features
‍‍‍```

6️⃣ Create ML training table

Join features + labels

Final supervised dataset:

```plaintext
ml.training_data
‍‍‍```


7️⃣ Train churn model with Vertex AI AutoML

Run Python locally (VS Code / terminal):

```Python
python training/train_vertex_automl.py
```


Vertex AI reads data directly from BigQuery

AutoML trains & evaluates multiple models

Best model is registered automatically with project_number

Output:

Model resource

8️⃣ Prepare batch prediction input

Select feature columns only (no label):

```plaintext
ml.prediction_input
```



9️⃣ Run batch prediction 

Submit batch prediction job via Python:

```plaintext
Input: BigQuery table

Output: BigQuery table (auto-named)
```


Result:
```plaintext
ml.predictions_2026_02_13...
```

Cost-efficient:

No endpoints

No always-on infra

🔟 Parse model predictions

Vertex AI outputs:

predicted_churned.classes

predicted_churned.scores

```plaintext
| active_days_30d | events_30d | logins_30d |                              predicted_churned                               | purchases_30d |               user_id                |
+-----------------+------------+------------+------------------------------------------------------------------------------+---------------+--------------------------------------+
| 1               | 1          | 0          |   {"classes":["0","1"],"scores":["0.25188514590263367","0.748114824295044"]} | 1             | 12a12005-6715-40d3-a3e3-ac16fff6d55b |
| 1               | 1          | 0          | {"classes":["0","1"],"scores":["0.25727182626724243","0.74272811412811279"]} | 0             | 2a322c7e-329e-4290-afb7-d79194af990e |
| 1               | 1          | 0          | {"classes":["0","1"],"scores":["0.25727182626724243","0.74272811412811279"]} | 0             | 94d02ea8-040a-43e7-a8b2-ccb7935bcc4f |
| 1               | 1          | 0          | {"classes":["0","1"],"scores":["0.25727182626724243","0.74272811412811279"]} | 0             | e1814003-e579-435e-aed9-17704a8eea46 |
| 1               | 2          | 0          | {"classes":["0","1"],"scores":["0.30248343944549561","0.69751656055450439"]} | 0             | 0d62fb0f-fecb-4551-9b21-ef5f3a578f8c |

```


Extract churn probability:

```SQL
predicted_churned.scores[OFFSET(1)]
```


(Probability of class "1" = churn)


1️⃣1️⃣ Build final churn insights

Rank users by churn risk

Apply thresholds (e.g. > 0.7)

Optionally materialize:

```plaintext

e0008296-d62c-45ff-bf37-2991a9316a26	0.74272811412811279	EMAIL_NUDGE
6533000d-f279-4c12-a445-ef27281ba6ed	0.70975381135940552	MONITOR
7e2f44c4-5216-4122-9d29-4d4cd937690c	0.74272811412811279	EMAIL_NUDGE
2726cb56-fc71-4376-a44b-d55f2b2e07d1	0.74272811412811279	EMAIL_NUDGE
78e568fd-5e4d-4256-b18c-448cc23bc7ac	0.74272811412811279	EMAIL_NUDGE
a37141b8-e881-463f-a562-6e7fccb6c513	0.748114824295044	EMAIL_NUDGE
317dd408-ad31-42d0-84b4-42934ca78f25	0.74272811412811279	EMAIL_NUDGE
ed78cd71-a6ed-4239-a5c9-b5dd42dc0754	0.70975381135940552	MONITOR
3eeeb5f2-9ccf-4e78-8099-9dcfb66a6ed6	0.70975381135940552	MONITOR
```

![](Visualization/churn_probability%20by%20user_id.png)