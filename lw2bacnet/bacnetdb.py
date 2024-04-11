import os
import sys
import time
import logging
import psycopg2

def conn_bacnetdb():
    return psycopg2.connect(database="bacnet", user="postgres", host="localhost")

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

def get_profile_id(dev_eui):
    sql = """SELECT profile_id
                FROM device
                WHERE eui = decode(%s,'hex');"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (dev_eui,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]

def get_decoder(dev_eui):
    sql = """SELECT decoder
                FROM device
                WHERE eui = decode(%s,'hex');"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (dev_eui,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]

def get_dp_type(dpid):
    sql = """SELECT type
                FROM datapoint
                WHERE dp_id = %s;"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (dpid,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]

def get_dp_name(dpid):
    sql = """SELECT name
                FROM datapoint
                WHERE dp_id = %s;"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (dpid,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]

def get_dp_units(dpid):
    sql = """SELECT units
                FROM datapoint
                WHERE dp_id = %s;"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (dpid,))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]


def get_fport(prof_id, ch):
    sql = """SELECT fport
                FROM profile_datatypes
                WHERE port_id = %s AND profile_id = %s;"""
    conn = None
    val = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql, (ch, prof_id))
        val = cur.fetchone()

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()

    return val[0]


def dump_dp_to_csv():
    sql_cmd = (
        """
        COPY (SELECT * FROM datapoint) to '/tmp/dp.csv' with csv;
        """
    )
    conn = None
    try:
        conn = conn_bacnetdb()
        cur = conn.cursor()

        cur.execute(sql_cmd)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[psycopg]: {error}")
    finally:
        if conn is not None:
            conn.close()
