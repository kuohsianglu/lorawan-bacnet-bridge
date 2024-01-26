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
        CREATE TABLE IF NOT EXISTS object (
                id TEXT PRIMARY KEY,
                dev_eui BYTEA NOT NULL,
                name VARCHAR(100) NOT NULL,
                type VARCHAR(32) NOT NULL,
                units VARCHAR(32) NOT NULL,
                value FLOAT8 NOT NULL,
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

def bacnetdb_insert_object(obj):
    sql = """INSERT INTO object(id, dev_eui, name, type, units, value)
                VALUES(md5(%s), decode(%s,'hex'),%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;"""
    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (obj[0], obj[1], obj[2], obj[3], obj[4],obj[5],))

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

def bacnetdb_update_object(oid, new_value):
    sql = """UPDATE object
                SET value = %s
                WHERE id = md5(%s);"""
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

