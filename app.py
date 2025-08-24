import streamlit as st
import psycopg
import os
import time
import re
from databricks import sdk
from psycopg import sql
from psycopg_pool import ConnectionPool

# Database connection setup
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None

CLAIMS_TABLE = "claims_enriched"
SCHEMA_NAME = "reporting"

# def refresh_oauth_token():
#     """Refresh OAuth token if expired."""
#     global postgres_password, last_password_refresh
#     if postgres_password is None or time.time() - last_password_refresh > 900:
#         print("Refreshing PostgreSQL OAuth token")
#         try:
#             #postgres_password = workspace_client.config.oauth_token().access_token
#             postgres_password = os.getenv('PGPASSWORD')
#             last_password_refresh = time.time()
#         except Exception as e:
#             st.error(f"❌ Failed to refresh OAuth token: {str(e)}")
#             st.stop()

def refresh_oauth_token():
    """Refresh OAuth token if expired."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        try:
            # Try to get token from Databricks workspace client (cloud)
            postgres_password = workspace_client.config.oauth_token().access_token
            # If that didn't work, fallback to environment variable
            if not postgres_password:
                postgres_password = os.getenv('PGPASSWORD')
            if not postgres_password:
                raise ValueError("Could not get OAuth token from either Databricks SDK or PGPASSWORD environment variable.")
            last_password_refresh = time.time()
        except Exception as e:
            # On any error, fallback to environment variable
            postgres_password = os.getenv('PGPASSWORD')
            if not postgres_password:
                st.error(f"❌ Failed to refresh OAuth token: {str(e)} (also, PGPASSWORD env var not set!)")
                st.stop()
            last_password_refresh = time.time()



def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')} "
            f"application_name={os.getenv('PGAPPNAME')}"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
    return connection_pool

def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    
    # Recreate pool if token expired
    if postgres_password is None or time.time() - last_password_refresh > 900:
        if connection_pool:
            connection_pool.close()
            connection_pool = None
    
    return get_connection_pool().connection()

# def get_schema_name():
#     """Get the schema name in the format {PGAPPNAME}_schema_{PGUSER}."""
#     pgappname = os.getenv("PGAPPNAME", "my_app")
#     pguser = os.getenv("PGUSER", "").replace('-', '')
#     return f"{pgappname}_schema_{pguser}"

def init_database():
    """Initialize database schema and table."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # schema_name = get_schema_name()
            schema_name = SCHEMA_NAME
            
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.todos (
                    id SERIAL PRIMARY KEY,
                    task TEXT NOT NULL,
                    completed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(sql.Identifier(schema_name)))
            conn.commit()
            return True


def show_claims_table():
    schema = get_schema_name()
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get column names (schema)
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, CLAIMS_TABLE))
            cols_types = cur.fetchall()
            st.subheader("Claims Table Schema")
            st.table(cols_types)  # simple tabular view of columns & types

            # Get first 100 rows from claims table
            colnames = [col for col, _ in cols_types]
            colstr = ", ".join([f'"{col}"' for col in colnames])
            cur.execute(f'SELECT {colstr} FROM "{schema}"."{CLAIMS_TABLE}" LIMIT 100')
            rows = cur.fetchall()
            st.subheader("Claims Table Data (first 100 rows)")
            st.write(colnames)  # header row
            for row in rows:
                st.write(row)


def show_claims_analytics():
    # schema = get_schema_name()
    schema = SCHEMA_NAME
    table = CLAIMS_TABLE

    st.subheader("📊 Claims Analytics & KPIs")

    # --- KPI Metrics ---
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT 
                    COUNT(*) AS total_claims,
                    SUM(COALESCE(total_charge, 0)) AS total_charges,
                    COUNT(DISTINCT member_id) AS distinct_members,
                    COUNT(DISTINCT provider_id) AS distinct_providers,
                    SUM(CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END)::float/NULLIF(COUNT(*),0) AS denial_rate
                FROM "{schema}"."{table}"
            ''')
            total_claims, total_charges, distinct_members, distinct_providers, denial_rate = cur.fetchone()
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total Claims", int(total_claims))
            k2.metric("Total Charges", f"${total_charges:,.2f}")
            k3.metric("Unique Members", int(distinct_members))
            k4.metric("Unique Providers", int(distinct_providers))
            k5.metric(
                "Denial Rate",
                f"{(float(denial_rate) if denial_rate is not None else 0.0)*100:.1f}%"
            )

        # --- Claims by Status ---
    c2a, c2b = st.columns(2)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT claim_status, COUNT(*) AS n_claims
                FROM "{schema}"."{table}" GROUP BY claim_status
            ''')
            status_rows = cur.fetchall()
            status_chart = {row[0]: row[1] for row in status_rows}
    with c2a:
        st.markdown("#### Claims by Status")
        if status_chart:
            st.bar_chart(status_chart)
        else:
            st.info("No claims status data.")
    

     # --- Monthly Trend ---
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT substr(claim_date::text,1,7) AS month, SUM(total_charge) AS charges,
                    SUM(CASE WHEN claim_status='denied' THEN total_charge ELSE 0 END) AS denied_amt
                FROM "{schema}"."{table}"
                GROUP BY month ORDER BY month
            ''')
            monthly_rows = cur.fetchall()
            months = [row[0] for row in monthly_rows]
            charges = [row[1] for row in monthly_rows]
            denied_amt = [row[2] for row in monthly_rows]
    import pandas as pd
    with c2b:
        st.markdown("#### Monthly Charges & Denials")
        if months:
            df = pd.DataFrame({'charges': charges, 'denied_amt': denied_amt}, index=months)
            st.line_chart(df)
        else:
            st.info("No trend data.")
    
 # --- Denial Reasons ---
    c3a, c3b = st.columns(2)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT diagnosis_desc, COUNT(*) AS denied_claims
                FROM "{schema}"."{table}"
                WHERE claim_status='denied'
                GROUP BY diagnosis_desc
                ORDER BY denied_claims DESC LIMIT 10
            ''')
            denial_rows = cur.fetchall()
            denial_chart = {row[0]: row[1] for row in denial_rows}
    with c3a:
        st.markdown("#### Top Denial Reasons (Diagnosis)")
        if denial_chart:
            st.bar_chart(denial_chart)
        else:
            st.info("No denials by reason.")

    # --- Denial Rate by Provider ---
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT provider_name,
                    SUM(CASE WHEN claim_status='denied' THEN 1 ELSE 0 END)::float/NULLIF(COUNT(*),0) AS denial_rate,
                    COUNT(*) AS total
                FROM "{schema}"."{table}"
                GROUP BY provider_name HAVING COUNT(*) >= 3
                ORDER BY denial_rate DESC LIMIT 10
            ''')
            provider_rows = cur.fetchall()
            providers = [row[0] for row in provider_rows]
            denial_rates = [row[1] for row in provider_rows]
    with c3b:
        st.markdown("#### Providers with Highest Denial Rate")
        if providers:
            df = pd.DataFrame({'denial_rate': denial_rates}, index=providers)
            st.bar_chart(df)
        else:
            st.info("No denial/provider data.")


    # --- Diagnoses by Cost ---
    c4a, c4b = st.columns(2)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT diagnosis_desc, COUNT(*) AS n_claims, SUM(total_charge) AS charges
                FROM "{schema}"."{table}"
                GROUP BY diagnosis_desc ORDER BY charges DESC LIMIT 10
            ''')
            diag_rows = cur.fetchall()
            diag_chart = {row[0]: row[2] for row in diag_rows}
    with c4a:
        st.markdown("#### Top Diagnoses by Cost")
        if diag_chart:
            st.bar_chart(diag_chart)
        else:
            st.info("No diagnoses data.")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT provider_name, SUM(total_charge) AS charges, COUNT(*) AS n_claims
                FROM "{schema}"."{table}"
                GROUP BY provider_name ORDER BY charges DESC LIMIT 10
            ''')
            prov_rows = cur.fetchall()
    with c4b:
        st.markdown("#### Top Providers by Total Charge")
        if prov_rows:
            prov_df = pd.DataFrame(prov_rows, columns=['provider_name', 'charges', 'n_claims'])
            st.dataframe(prov_df)
        else:
            st.info("No provider data.")

    # --- Outlier High-Charge Claims ---
    st.markdown("#### Outlier High-Charge Claims")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT *
                FROM "{schema}"."{table}"
                WHERE total_charge > (
                    SELECT AVG(total_charge) + 3*STDDEV(total_charge) FROM "{schema}"."{table}"
                )
                ORDER BY total_charge DESC LIMIT 10
            ''')
            outlier_rows = cur.fetchall()
            if outlier_rows:
                outlier_colnames = [desc[0] for desc in cur.description]
                df_out = pd.DataFrame(outlier_rows, columns=outlier_colnames)
                st.dataframe(df_out)
            else:
                st.info("No outlier claims found.")


# Streamlit UI
def main():

    # show_claims_table()
    show_claims_analytics()


if __name__ == "__main__":
    main() 