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

def main():
    st.sidebar.title("Login To Snowflake")
    st.sidebar.divider()
    us = st.sidebar.text_input("Username: ")
    pw = st.sidebar.text_input("Password: ", type="password")
    st.sidebar.markdown(
        """
        <style>
        .stButton > button {
            display: block;
            margin-left: auto;
            margin-right: auto;
            color: green;
        }
        </style>
        """, unsafe_allow_html=True
    )

    if st.sidebar.button("Login"):
        if us and pw:
            try:
                con = connexion(us, pw, "kjbusla-kf63760")
                st.session_state['connection'] = con
                st.session_state['logged_in'] = True
            except sc.Error:
                st.sidebar.warning("Invalid Informations")
        else:
            st.sidebar.warning("Please enter both username and password.")

    if 'logged_in' in st.session_state and st.session_state['logged_in']:
        st.sidebar.divider()
        st.title("Welcome to your Snowflake")
        option = st.sidebar.selectbox(
            "Create or Display your warehouses?",
            ("Create", "Display")
        )
        con = connexion(us, pw, "kjbusla-kf63760")
        cursor = con.cursor()    
        if option == "Display":           
            warehouses = dataWarehouses(cursor)
            warehouse = st.sidebar.radio("choose one warehouse", warehouses)
            st.sidebar.write(f"you have selected: {warehouse}")
            option1 = st.sidebar.radio(f"Create or Display Schemas in {warehouse} ?",["Create a schema", "Display schemas"])
            st.sidebar.divider()
            if option1 == "Create a schema":
                schema = st.sidebar.text_input(f"Enter the name of the schema you want to create in {warehouse}")
                if st.sidebar.button("Create"):
                    try:
                        createSchemaInWarehouse(cursor, warehouse, schema)
                        st.sidebar.success(f"Schema {schema} created successfully")
                    except:
                        st.sidebar.error(f"Schema {schema} already exists, try another name") 
            if option1 == "Display schemas":
                allSchemas = displayAllSchemas(cursor, warehouse)
                schemasSelectBox = st.sidebar.radio("",allSchemas)
                st.sidebar.write(f"You have selected {schemasSelectBox} in {warehouse} warehouse")
                st.write(f"Create a table in {schemasSelectBox}")
                table = st.text_input(f"Enter the name of the table you want to create in {schemasSelectBox}")
                numberOfColumns = st.number_input("Number of columns: ", step=1, format="%d" )
                namesOfColumns = []
                typeOfColumns = []
                for i in range(numberOfColumns):
                    namesOfColumns.append(st.text_input(f"Name of column {i+1}:"))
                    typeOfColumns.append(st.text_input(f"Type of column {i+1}:"))
                    st.divider()
                if st.button("Create Table"):
                    try:
                        createTable(cursor, warehouse, schemasSelectBox, table, numberOfColumns, namesOfColumns, typeOfColumns)
                        st.success(f"table {table} created successfully")
                    except:
                        st.error(f"table {table} already exists, try another name") 
                    
                        
        elif option == "Create":
            newWarehouseName = st.sidebar.text_input("Please Enter the name of your Warehouse: ")
            if st.sidebar.button("Create"):
                try:
                    createDataWarehouses(cursor, newWarehouseName)
                    st.sidebar.success("Warehouse created successfully")
                except:
                    st.sidebar.error("Warehouse  already exist, try another name")
        
if __name__ == "__main__":
    main()
