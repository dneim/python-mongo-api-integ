import pandas as pd
from Automation_Scripts import db_creds
import psycopg2.pool

from email_send import *
from email_template import *

# --- Database Connection ---
pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    database=db_creds.DB_MAIN,
    host=db_creds.DB_HOST,
    user=db_creds.DB_USER,
    password=db_creds.DB_PASS,
    port=db_creds.DB_PORT
)

def get_connection():
    return pool.getconn()


def get_data(resource, conn):
    """
    Retrieve event logs for a 3-day span for resources like 'listing', 'openhouse','agent', etc.
    """
    cursor = conn.cursor()

    qry = f"""
            WITH last_events AS (
              SELECT resource_chain_id, MAX(id) AS last_event_id
              FROM table_resource_event
              WHERE resource_chain_id IN (
                SELECT id
                FROM table_resource_event_chain
                WHERE resource_type = '{resource}' 
                AND incoming_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '3 day'
                AND incoming_time < DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '1 day'
              )
              GROUP BY resource_chain_id
            )

            SELECT src.source_name, src.protocol, src.provider, chain.download_id, chain.resource_id, chain.incoming_time, chain.status, info.event_description
            FROM table_resource_event event
                    JOIN table_resource_event_chain chain ON chain.id = event.resource_chain_id
                    JOIN table_resource_event_info info ON info.id = event.event_info_id
                    JOIN table_source_info src ON src.id = chain.source_id
                    JOIN last_events ON last_events.last_event_id = event.id
            WHERE src.is_active = true
                AND src.source_name NOT LIKE '%_CUSTOM'
            ORDER BY chain.incoming_time desc;"""

    cursor.execute(qry)
    logs = [item for item in cursor.fetchall()]
    return logs


def consecutive_failures_report(data):
    """
    Aggregate consecutive failure counts per source and return summary and detailed DataFrames.
    """
    df = pd.DataFrame(data, columns=["source_name", "protocol", "provider", "download_id", "resource_id", "incoming_time",
                                     "status", "event_description"])

    df["incoming_time"] = pd.to_datetime(df["incoming_time"])
    df = df.sort_values(by=["source_name", "incoming_time"])

    consecutive_failures = {}
    consecutive_failure_records = []

    for source in df["source_name"].unique():
        source_df = df[df["source_name"] == source]
        last_status = None
        count = 0
        source_failure_records = []
        failure_dates = set()

        for _, row in source_df[::-1].iterrows():
            current_status = row["status"]
            current_date = row["incoming_time"].date()

            if last_status is None and current_status == 'Success':
                count = 0
                source_failure_records = []
                break

            if current_status in ['Failed', 'Aborted']:
                if current_date not in failure_dates:
                    count += 1
                    failure_dates.add(current_date)
                source_failure_records.append(row)
            elif current_status == 'Success':
                break

            last_status = current_status

        consecutive_failures[source] = count

        if source_failure_records:
            consecutive_failure_records.extend(source_failure_records)

    consecutive_failures_df = pd.DataFrame(list(consecutive_failures.items()),
                                           columns=["source_name", "consecutive_failures"])

    consecutive_failures_df = consecutive_failures_df[consecutive_failures_df["consecutive_failures"] > 0]

    consecutive_failure_records_df = pd.DataFrame(consecutive_failure_records)
    consecutive_failure_records_df = consecutive_failure_records_df[
        consecutive_failure_records_df["source_name"].isin(consecutive_failures_df["source_name"])]

    consecutive_failure_records_df = consecutive_failure_records_df.loc[
        consecutive_failure_records_df.groupby("source_name")["incoming_time"].idxmax()]

    return consecutive_failures_df, consecutive_failure_records_df


def main():
    conn = get_connection()
    cursor = conn.cursor()

    resources_daily = ['listing', 'incrlisting', 'incrphoto', 'openhouse','agent', 'office', 'pendinglisting','soldlisting']

    for resource in resources_daily:
        data_logs = get_data(resource, conn)
        consec_fails_df, consec_fail_records_df = consecutive_failures_report(data_logs)

        consec_fails_table_rows = ""
        for _, row in consec_fails_df.iterrows():
            consec_fails_table_rows += f"<tr><td>{row['source_name']}</td><td>{row['consecutive_failures']}</td></tr>"

        consec_fail_records_table_rows = ""
        for _, row in consec_fail_records_df.iterrows():
            consec_fail_records_table_rows += f"<tr><td>{row['source_name']}</td><td>{row['protocol']}</td><td>{row['provider']}</td><td>{row['download_id']}</td><td>{row['resource_id']}</td><td>{row['incoming_time']}</td><td>{row['status']}</td><td>{row['event_description']}</td></tr>"

        email_body = failed_scripts_job_template.format(
            env="PROD",
            resource_type=f"{resource}",
            consec_fails_table_rows=consec_fails_table_rows,
            consec_fail_records_table_rows=consec_fail_records_table_rows
        )

        subject = f'Failure Report for {resource} resource'
        to_recip = ['alerts@example.com']
        msg = Message(subject=subject, body=email_body, to_recip=to_recip)
        msg.send()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
