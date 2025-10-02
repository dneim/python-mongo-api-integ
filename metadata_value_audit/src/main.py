import requests
import json
import pymongo
import certifi
from Automation_Scripts import db_creds
import psycopg2.pool

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


def get_search_params_rets(cur, source):
    qry = f"""
            select info.source_name, info.protocol, cls.dataset_name, ctx.value
            from table_dataset_config cls
                    join table_source_info info on info.id = cls.dataset_id
                    join table_context_variable ctx on ctx.dataset_id = cls.dataset_id
            where cls.download_type = 'listing'
                    and info.source_name = '{source}'
                    and ctx.key = 'STATUS_FIELD'
            limit 1;
            """

    cur.execute(qry)
    result = cur.fetchone()
    if result:
        return {
            'source': result[0],
            'protocol': result[1],
            'dataset_name': result[2],
            'source_field': result[3]
        }
    else:
        return None


def get_metadata_es_rets(auth_url, data_dict):
    src = data_dict['source']
    dataset_name = data_dict['dataset_name']
    field_name = data_dict['source_field']

    qry = {
        "_source": ["lookups.documentId", "lookups.lookupLongValue"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"documentId": {"value": src.lower()}}},
                    {"term": {"resource": {"value": "property"}}},
                    {"term": {"className": {"value": dataset_name.lower()}}},
                    {"term": {"longName.keyword": {"value": field_name}}}
                ]
            }
        }
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(auth_url, headers=headers, data=json.dumps(qry))
        response.raise_for_status()
        metadata = response.json()

        hits = metadata.get("hits", {}).get("hits", [])
        if hits:
            print(f"\n✅ Field '{field_name}' found in metadata.")
            statuses = []
            for hit in hits:
                src_data = hit.get("_source", {})
                lookups = src_data.get("lookups", [])
                for lookup in lookups:
                    value = lookup.get("lookupLongValue")
                    if value:
                        statuses.append(value)

            data_dict["metadata_statuses"] = statuses
            return data_dict
        else:
            print(f"\n❌ Field '{field_name}' NOT found in metadata.")
            return data_dict

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except ValueError as ve:
        print(f"Value error: {ve}")


def get_metadata_es_webapi(auth_url, data_dict):
    src = data_dict['source']
    field_name = data_dict['source_field']

    qry = {
        "_source": ["lookups.documentId", "lookups.lookupLongValue"],
        "query": {
            "bool": {
                "must": [
                    { "term": { "documentId": { "value": src.lower() } } },
                    { "term": { "resource": { "value": "entitytype"} } },
                    { "term": {"className": { "value": "property"} } },
                    { "term": { "longName.keyword": { "value": field_name } } }
                ]
            }
        }
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(auth_url, headers=headers, data=json.dumps(qry))
        response.raise_for_status()
        metadata = response.json()

        hits = metadata.get("hits", {}).get("hits", [])
        if hits:
            print(f"\n✅ Field '{field_name}' found in metadata.")
            statuses = []
            for hit in hits:
                src_data = hit.get("_source", {})
                lookups = src_data.get("lookups", [])
                for lookup in lookups:
                    value = lookup.get("lookupLongValue")
                    if value:
                        statuses.append(value)

            data_dict["metadata_statuses"] = statuses
            return data_dict
        else:
            print(f"\n❌ Field '{field_name}' NOT found in metadata.")
            return data_dict

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except ValueError as ve:
        print(f"Value error: {ve}")


def get_metadata_mongo(collection, data_dict):
    src = data_dict['source']
    statuses = data_dict.get("metadata_statuses", [])
    found_statuses = []

    for status in statuses:
        pipeline = [
            {
                "$match": {
                    "_source": src,
                    "property.listing.status": status
                }
            },
            {
                "$limit": 1
            }
        ]

        result = list(collection.aggregate(pipeline))
        if result:
            found_statuses.append(status)

    data_dict["mongo_statuses"] = found_statuses
    return data_dict


def metadata_mongo_audit(data_dict):
    metadata_statuses = data_dict.get("metadata_statuses", [])
    mongo_statuses = data_dict.get("mongo_statuses", [])
    missing_statuses = [status for status in metadata_statuses if status not in mongo_statuses]
    data_dict["missing_statuses"] = missing_statuses
    return data_dict


def main():
    source = "SRC_PLACEHOLDER"
    es_auth_url = "https://placeholder-opensearch-url.com/api/search"

    connection_string = db_creds.CONNECTION_STR_RO
    client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())
    database = client['placeholder_db']
    collection = database['placeholder_collection']

    conn = get_connection()
    cursor = conn.cursor()

    search_params = get_search_params_rets(cursor, source)

    if not search_params:
        print("❌ No search parameters found. Exiting.")
        return

    if search_params.get('protocol') == 'RETS':
        metadata_dict = get_metadata_es_rets(es_auth_url, search_params)
    else:
        metadata_dict = get_metadata_es_webapi(es_auth_url, search_params)

    metadata_mongo_dict = get_metadata_mongo(collection, metadata_dict)
    finalized_dict = metadata_mongo_audit(metadata_mongo_dict)

    print("\n📋 Final Audit Summary:")
    print(json.dumps(finalized_dict, indent=2))

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()