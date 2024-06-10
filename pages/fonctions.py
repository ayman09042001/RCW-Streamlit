import streamlit as st
import snowflake.connector as sc
import pandas as pd
def connexion(us, pw, acc):
    con = sc.connect(
        account=acc,
        user=us,
        password=pw
    )
    return con
def dataWarehouses(cursor):
    sql = "SHOW DATABASES"
    df = cursor.execute(sql).fetchall()
    wh = [row[1] for row in df]
    return wh
def createDataWarehouses(cursor, newwarehouse):
    sql = f"CREATE DATABASE {newwarehouse}"
    df = cursor.execute(sql)
    return df
def createSchemaInWarehouse(cursor, warehouse, schema):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"CREATE SCHEMA {schema}")
def displayAllSchemas(cursor, warehouse):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute("SHOW SCHEMAS")
    schemas = cursor.fetchall()
    sch = [row[1] for row in schemas]
    return sch
def createTable(cursor, warehouse, schema, table, numberOfColumns, nameOfColumns, dataType):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    columns = ','.join([f"{nameOfColumns[i]} {dataType[i]}" for i in range(numberOfColumns)])
    cursor.execute(f"CREATE TABLE {table} ({columns})")
def displayAllTables(cursor, warehouse, schema):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute(f"SHOW TABLES")
    table = cursor.fetchall()
    tables = [row[1] for row in table]
    return tables
def displayTable(cursor, warehouse, schema, table):
    sql = f"select * from {warehouse}.{schema}.{table}"
    df = cursor.execute(sql).fetchall()
    columns_name = []
    for colonne in cursor.description:
        columns_name.append(colonne[0])
    return pd.DataFrame(df, columns=columns_name)
def DeleteTable(cursor, warehouse, schema, table):
    sql = f"DROP TABLE {warehouse}.{schema}.{table}"
    df = cursor.execute(sql)
    return df
def insertIntoTable(cursor, warehouse, schema, table, values):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    placeholders = ', '.join(['%s'] * len(values))
    query = f"INSERT INTO {table} VALUES ({placeholders})"
    cursor.execute(query, values)
def getColumnName(cursor, warehouse, schema, table):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
    first_column_name = cursor.description[0][0]
    return first_column_name
def get_first_column_values(cursor, warehouse, schema, table):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
    first_column_name = cursor.description[0][0]
    cursor.execute(f"SELECT {first_column_name} FROM {table}")
    values = cursor.fetchall()
    return [row[0] for row in values]
def deleteFromTable(cursor, warehouse, schema, table, columnName, value):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    query = f"DELETE FROM {table} WHERE {columnName} = '{value}'"
    cursor.execute(query)
def modifyLineInTable(cursor, warehouse, schema, table, columnName, value, values):
    cursor.execute(f"USE DATABASE {warehouse}")
    cursor.execute(f"USE SCHEMA {schema}")
    set_values = ', '.join([f"{col} = '{values[col]}'" for col in values if col != columnName])
    query = f"UPDATE {table} SET {set_values} WHERE {columnName} = '{value}'"
    cursor.execute(query)
    # st.write(query)
