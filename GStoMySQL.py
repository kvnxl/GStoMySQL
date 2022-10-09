#Import libraries
import pandas as pd
import mysql.connector
from mysql.connector import Error

"""
https://docs.google.com/spreadsheets/d/157vDklUdJWQCjeCSI92-rEiAebetdfkJ4lej-AAQkU0/edit?usp=sharing
Sheet ID - 157vDklUdJWQCjeCSI92-rEiAebetdfkJ4lej-AAQkU0
Sheet ID is between /d/ and /edit
Using Digimon Dataset from Kaggle - https://www.kaggle.com/datasets/rtatman/digidb?resource=download
The sheet mentioned here is editable by everyone, feel free to play around :)
"""

sheet_id = "157vDklUdJWQCjeCSI92-rEiAebetdfkJ4lej-AAQkU0"
dataframe = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")  #turned into dataframe

def dftosql(dataframe):
    try:
        conn = mysql.connector.connect(host="localhost", user="root", password="1234", database="Test", charset="utf8")
        cur = conn.cursor()
        cur.execute("delete from new_table") # execute query to clear all records, comment this line out to not delete previously entered data
        conn.commit()  # make changes
        for (row, rs) in dataframe.iterrows():
            Number = str(int(rs[0]))
            Digimon = rs[1]
            Stage = rs[2]
            Type = rs[3]
            Attribute = rs[4]
            Memory = str(int(rs[5]))
            Equip_Slots = str(int(rs[6]))
            Lv50_HP = str(int(rs[7]))
            Lv50_SP = str(int(rs[8]))
            Lv50_Atk = str(int(rs[9]))
            Lv50_Def = str(int(rs[10]))
            Lv50_Int = str(int(rs[11]))
            Lv50_Spd = str(int(rs[12]))
            # rs is series, for int - use typecaste str(int(x[x1])), [] defines column name
            query = "INSERT INTO new_table VALUES(" +Number+ "," "'"+Digimon+"'" + "," + "'"+Stage+"'" + "," + "'"+Type+"'" + "," + "'"+Attribute+"'" + "," +Memory+ "," +Equip_Slots+ "," +Lv50_HP+ "," +Lv50_SP+ "," +Lv50_Atk+ "," +Lv50_Def+ "," +Lv50_Int+ "," +Lv50_Spd+ ")"  # query to insert into tree, don't use quotes for int
            cur.execute(query)
        conn.commit()
        cur.close()
    except Error as e:
        print("Error in mysql connection = ", e)
    finally:
        if conn.is_connected():
            conn.close()


print(dataframe)
dftosql(dataframe)
print("\n Dataframe is transferred successfully")
