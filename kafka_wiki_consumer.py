from kafka import KafkaConsumer
import json

import psycopg
from psycopg.rows import dict_row


def meta_table_init_vars(meta):
    query = """
    INSERT INTO meta
    (uri, request_id, id, domain, stream, dt, topic, partition, meta_offset)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """

    values = (
        meta.get("uri"),
        meta.get("request_id"),
        meta["id"],          
        meta.get("domain"),
        meta.get("stream"),
        meta.get("dt"),
        meta.get("topic"),
        meta.get("partition"),
        meta.get("offset"),
    )

    return query, values

def length_table_init_vars(length):
    query = """
    INSERT INTO length
    (old, new)
    VALUES (%s, %s)
    RETURNING length_id
    """

    values = (
        length.get("old"),
        length.get("new"),
    )

    return query, values

def revision_table_init_vars(revision):
    query = """
    INSERT INTO revision
    (old, new)
    VALUES (%s, %s)
    RETURNING revision_id
    """

    values = (
        revision.get("old"),
        revision.get("new"),
    )

    return query, values


def wiki_changes_table_init_vars(wiki_changes: dict, meta: dict, length_id: int, revision_id: int):
    query = """
    INSERT INTO wiki_changes
    (
        event_schema, meta_id, id, type, namespace, title, title_url, comment, parsedcomment,
        timestamp, user_name, bot, minor, patrolled, notify_url, server_url, server_name,
        server_script_path, wiki, length_id, revision_id, log_id, log_type, log_action,
        log_params, log_action_comment
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """

    values = (
        wiki_changes.get("schema"),
        meta.get("id"),                 
        wiki_changes.get("id"),                 
        wiki_changes.get("type"),
        wiki_changes.get("namespace"),
        wiki_changes.get("title"),
        wiki_changes.get("title_url"),
        wiki_changes.get("comment"),
        wiki_changes.get("parsedcomment"),
        wiki_changes.get("timestamp"),
        wiki_changes.get("user"),
        wiki_changes.get("bot"),
        wiki_changes.get("minor"),
        wiki_changes.get("patrolled"),
        wiki_changes.get("notify_url"),
        wiki_changes.get("server_url"),
        wiki_changes.get("server_name"),
        wiki_changes.get("server_script_path"),
        wiki_changes.get("wiki"),
        length_id,                      
        revision_id,                    
        wiki_changes.get("log_id"),
        wiki_changes.get("log_type"),
        wiki_changes.get("log_action"),
        json.dumps(wiki_changes.get("log_params")),
        wiki_changes.get("log_action_comment"),
    )

    return query, values
if __name__ == "__main__":
    try:
        conn = psycopg.connect(
            host="localhost",
            port=5432,
            dbname="wiki",
            user="postgres",
            password="postgres",
            row_factory=dict_row,   
        )

        cur = conn.cursor()

    except Exception as e:
        print(f"Error with connection to DB {e}")
        raise

    consumer = KafkaConsumer(
        "wiki-stream-events",
        bootstrap_servers="localhost:9092",
        group_id="group_1",
        auto_offset_reset="latest"
    )

    for msg in consumer:
        event = json.loads(msg.value.decode("utf-8"))
        change_id = event.get("id")
        if change_id is None:
            print("Skipping event without id:", event.get("type"))
            continue
        meta = event.get("meta", {})
        meta_query, meta_values = meta_table_init_vars(meta)
        
        length = event.get("length", {})
        length_query, length_values = length_table_init_vars(length)

        revision = event.get("revision", {})
        revision_query, revision_values = revision_table_init_vars(revision)
        try:

            cur.execute(meta_query, meta_values)

            cur.execute(length_query, length_values)
            row = cur.fetchone()
            length_id = row["length_id"] if row else None
            
            cur.execute(revision_query, revision_values)
            row = cur.fetchone()
            revision_id = row["revision_id"] if row else None
            
            main_query, main_values = wiki_changes_table_init_vars(event, meta, length_id, revision_id)
            cur.execute(main_query, main_values)

            conn.commit()
            
        except Exception as e:
            conn.rollback()
            print("DB error:", e)
            continue

    conn.close()



    