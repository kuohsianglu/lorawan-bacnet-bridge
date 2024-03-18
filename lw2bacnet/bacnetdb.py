import os
import sys
import time
import logging
import psycopg2

def conn_bacnetdb():
    return psycopg2.connect(database="bacnet", user="bacnet", password="bacnet")

def bacnetdb_init_table():
    sql_cmd = (
        """
        CREATE TABLE IF NOT EXISTS device (
            eui BYTEA PRIMARY KEY,
            decoder VARCHAR(100) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS datapoint (
                dp_id TEXT PRIMARY KEY,
                dev_eui BYTEA NOT NULL,
                name VARCHAR(100),
                type VARCHAR(32) NOT NULL,
                units VARCHAR(32) NOT NULL,
                value FLOAT8 NOT NULL,
                fport INT,
                cov BOOLEAN,
                FOREIGN KEY (dev_eui)
                    REFERENCES device (eui)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )

    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        for command in sql_cmd:
            cur.execute(command)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

def bacnetdb_insert_device(deveui, dcoder):
    sql = """INSERT INTO device(eui, decoder)
                VALUES(decode(%s,'hex'),%s)
                ON CONFLICT (eui) DO NOTHING;"""
    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (deveui, dcoder))

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

def bacnetdb_insert_datapoint(obj):
    sql = """INSERT INTO datapoint(dp_id, dev_eui, name, type, units, value, fport, cov)
                VALUES(%s, decode(%s,'hex'),%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dp_id) DO NOTHING;"""
    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (obj[0], obj[1], obj[2], obj[3], obj[4],obj[5],obj[6],obj[7]))

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

def bacnetdb_update_datapoint(oid, new_value):
    sql = """UPDATE datapoint
                SET value = %s
                WHERE dp_id = %s;"""
    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (new_value, oid))

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

def get_object_value(oid):
    sql = """SELECT value
                FROM object
                WHERE id = md5(%s);"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (oid,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val

def get_device_obj(deveui):
    sql = """SELECT *
                FROM object
                WHERE dev_eui = decode(%s,'hex');"""
    conn = None
    obj = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (deveui,))
        obj = cur.fetchall()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return obj

