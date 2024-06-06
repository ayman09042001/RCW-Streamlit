import streamlit as st
import snowflake.connector as sc
import pandas as pd
def main():
    st.title("Demo de la connexion à Snowflake depuis streamlit")
    try:
        con = sc.connect(
        account = 'kjbusla-kf63760',
        user = 'AEDDAHBI5',
        password = 'Ziad.akodad02'
        )
        cursor = con.cursor()
        def dataPersons():
            sql = "SELECT * FROM RCW.PERSONS.PERSONS"
            df = cursor.execute(sql).fetchall()
            columns_name = []
            for colonne in cursor.description:
                columns_name.append(colonne[0])
            return pd.DataFrame(df, columns=columns_name)
        donnees = dataPersons()
        st.write(donnees)
    except sc.Error as e:
        st.error(f"Error connecting to Snowflake: {e}")




if __name__ == "__main__":
    main()