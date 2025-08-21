**Production analytics app for healthcare payer claims, powered by Databricks Lakebase and Streamlit**

***

## Overview

This project provides a Databricks-native analytics app for healthcare payer claims, built on **Databricks Lakebase** (managed Postgres with lakehouse-native features). It ingests the enriched claims data from the [payer_dlt](https://github.com/bigdatavik/payer_dlt) medallion pipelines (bronze, silver, gold, Lakeflow sync), exposing secure, interactive dashboards—all within Databricks.

- **Data Source:** Consumes the final claims enriched (`claims_enriched`) table prepared by the [payer_dlt](https://github.com/bigdatavik/payer_dlt) project.
- **Architecture:** Gold/Silver/Bronze Medallion Tables → Lakeflow Sync (Reverse ETL) → Lakebase Postgres → Production App.
- **Features:**  
  - Real-time KPIs and trend analytics  
  - Secure Postgres connection using Lakebase short-lived credentials  
  - Lakehouse-native governance with Unity Catalog and managed access  
  - Extendable with Python or Streamlit  
 
***

## Architecture

![](images/architecture.png)

- Access to an Azure Databricks workspace with **Lakebase** enabled and Unity Catalog configured
- The [payer_dlt](https://github.com/bigdatavik/payer_dlt) pipelines completed, with the `claims_enriched` gold table populated in Unity Catalog and eligible for Lakeflow sync
- Your workspace/service principal has privilege to provision apps, sync tables, and manage Lakebase

***

## Setup and Deployment

### 1. Sync Claims Table to Lakebase

- Use Lakeflow or the Databricks UI to **sync the `claims_enriched` table** from the gold schema into your Lakebase instance.
- For steps to provision the lakebase PostgreSQL database in Databricks see my [Medium blog](https://medium.com/@vikram.malhotra/how-to-build-a-databricks-analytics-app-on-lakebase-from-setup-to-insights-89275e37e6eb) 
- Register the synced Lakebase database/catalog in Unity Catalog for access and governance.

![](images/sync1.png)

![](images/sync2.png)


### 2. Clone and Deploy the App from Databricks UI

- In Databricks, go to the **Repos** tab.
- Click **Add Repo** or **Clone Repo** and enter:
  ```
  https://github.com/bigdatavik/payer-app-lakebaseapp.git
  ```
- Once cloned, go to **Compute > Apps**.
- Click **Create new app**, choose **Create a custom app**, then point to your cloned repo folder.

![](images/app1.png)
![](images/app2.png)
![](images/app3.png)
![](images/app4.png)
![](images/app5.png)
![](images/app6.png)

- Set any required environment variables for your Lakebase Postgres connection (can be from generated creds via the Databricks SDK, secrets, or Databricks env UI).
- Complete the app wizard and launch.

![](images/app7.png)
![](images/app8.png)
![](images/app9.png)

### 3. Using the App

- Once deployed, access the app from your Databricks Apps section.
- Explore interactive dashboards and filter claims data in real time.

![](images/app10.png)

***

## Notes

- **Credential Rotation:** Lakebase credentials typically expire every 15 minutes. The app supports refreshing; see code for patterns.
- **Security/Compliance:** Access is governed via Unity Catalog and Lakebase privileges. All changes or views are audit-logged within Databricks.
- **Customization:** Add new charts, filters, or business logic by editing/adding Streamlit components within the repo.

***

## Troubleshooting

- **Permission Denied**: Confirm the app/service principal has SELECT privilege on the synced claims table in Lakebase.
- **Lakebase Connectivity**: Ensure your instance is running and you are using valid (fresh) credentials.
- **psql not found**: Install the PostgreSQL client if local troubleshooting/debug is needed.

***

## Related Projects

- [payer_dlt](https://github.com/bigdatavik/payer_dlt): Gold/silver/bronze ETL pipelines, source of transformation and enrichment  
- [Databricks Lakebase Documentation] (https://learn.microsoft.com/en-us/azure/databricks/oltp/)  


*Move from data pipeline to production BI in hours, not weeks—with Databricks Lakebase and this app template!*
