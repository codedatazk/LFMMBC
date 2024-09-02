import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os,sys
import hash

def writeFileToDatabase(file_path,table_name,connector):
    file_type= os.path.splitext(file_path)[1].lower()
    if file_type == '.csv':
        chunk_size = 10000  #
        csv_reader = pd.read_csv(file_path, chunksize=chunk_size)
        engine = create_engine(f'mysql+pymysql://{connector.username}:{connector.password}@{connector.host}:{connector.port}/{connector.database}')
        try:
            for chunk in csv_reader:
                chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f'Successfully imported data into database {connector.database}')
        except Exception as e:
            print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
            print(f'Error: {e}')
    elif file_type == '.xlsx':
        excel_data = pd.read_excel(file_path)

        engine = create_engine(f'mysql+pymysql://{connector.username}:{connector.password}@{connector.host}:{connector.port}/{connector.database}')

        try:
            excel_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f'Successfully imported data into database {connector.database}')
        except Exception as e:
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)
            print(f'Error: {e}')

def addColumnsToTable(table_name,connector):
    try:
        cursor = connector.connection.cursor()
        
        cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'id'")
        result = cursor.fetchone()

        if result is None:
            print(f"There is no ID column in the {table_name}")
            cursor.execute(f"ALTER TABLE {table_name} ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST")

            connector.connection.commit()
            print(f"Successfully added ID column to the {table_name}")
        connector.connection.commit()
    except pymysql.Error as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
        connector.connection.rollback()


def createAuxiliaryTable(table_name, connector):
    try:
        cursor = connector.connection.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        sql = f"""
        CREATE TABLE {table_name}_pcode (
            id INT PRIMARY KEY,
            pcode1 VARCHAR(255),
            pcode2 VARCHAR(255)
        );
        """
        cursor.execute(sql)

        sql = f"""
            INSERT INTO {table_name}_pcode (id)
            SELECT ROW_NUMBER() OVER () AS id
            FROM {table_name};
            """
        cursor.execute(sql)
        connector.connection.commit()
        print(f"Successfully created table {table_name}_pcode")

    except Exception as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)
        connector.connection.rollback()


def createTriggerTable(table_name,connector):

    try:
        cursor = connector.connection.cursor()
        sql = f"""
            CREATE TABLE {table_name}_trigger_record (
                Nid INT PRIMARY KEY AUTO_INCREMENT,
                id INT(30),
                pcode1_input VARCHAR(255),
                pcode2_input VARCHAR(255)
            );
            """
        cursor.execute(sql)

        connector.connection.commit()
        print(f"Successfully created table {table_name}_trigger_record")

    except pymysql.Error as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
        connector.connection.rollback()

import pymysql

import pymysql

def createViewAndTrigger(table_name, connector):
    try:
        cursor = connector.connection.cursor()

        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        first_column = cursor.fetchone()  
        table_name_id = first_column[0] 

        createViewSql = f"""
            CREATE VIEW `{table_name}_view` AS 
            SELECT 
                ROW_NUMBER() OVER () AS `row_num`,
                `{table_name}`.`{table_name_id}` AS `{table_name_id}` 
            FROM 
                `{table_name}`;
            """
        cursor.execute(createViewSql)
        connector.connection.commit()
        
        createTriggerSql = f"""
        CREATE TRIGGER `after_{table_name}_update`
        AFTER UPDATE ON `{table_name}`
        FOR EACH ROW
        BEGIN
            DECLARE v_id INT;
            SELECT row_num INTO v_id FROM `{table_name}_view` WHERE `{table_name_id}` = NEW.`{table_name_id}`;
            IF v_id IS NOT NULL THEN
                UPDATE `{table_name}_pcode` SET pcode1 = NULL, pcode2 = NULL WHERE id = v_id;
            END IF;
        END
        """
        cursor.execute(createTriggerSql)
        connector.connection.commit()

    except pymysql.Error as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)
        print("Error Message:", e)
        connector.connection.rollback()
    finally:
        cursor.close() 



def addTriggerToTable(table_name, connector):
    try:
        cursor = connector.connection.cursor()

        createInsertTrigger = f"""
            CREATE TRIGGER trigger_insert_{table_name}_logs
            AFTER INSERT ON {table_name}_pcode
            FOR EACH ROW
            BEGIN
                INSERT INTO {table_name}_trigger_record(id, pcode1_input, pcode2_input) VALUES (NEW.id, NEW.pcode1, NEW.pcode2);
            END;
            """
        
        cursor.execute(createInsertTrigger)
        connector.connection.commit()

        createUpdateTrigger = f"""
            CREATE TRIGGER trigger_update_{table_name}_logs
            AFTER UPDATE ON {table_name}_pcode
            FOR EACH ROW
            BEGIN
                INSERT INTO {table_name}_trigger_record(id, pcode1_input, pcode2_input) VALUES (NEW.id, NEW.pcode1, NEW.pcode2);
            END;
            """
        cursor.execute(createUpdateTrigger)
        connector.connection.commit()

        createDeleteTrigger = f"""
            CREATE TRIGGER trigger_delete_{table_name}_logs
            AFTER DELETE ON {table_name}_pcode
            FOR EACH ROW
            BEGIN
                INSERT INTO {table_name}_trigger_record(id, pcode1_input, pcode2_input) VALUES (OLD.id, OLD.pcode1, OLD.pcode2);
            END;
            """

        cursor.execute(createDeleteTrigger)
        connector.connection.commit()

        print("Successfully added triggers for", table_name)

    except pymysql.Error as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
        connector.connection.rollback()


def updatePcode(table_name, connector):
    try:
        cursor = connector.connection.cursor()
  
        cursor.execute(f"SELECT id FROM {table_name}_pcode")  
        rows = cursor.fetchall()  
        
        for row in rows:  
            id_value = row[0]
            id1_value = hash.Link_1(int(id_value))  
            id2_value = hash.Link_2(int(id_value))  
            update_sql = f"UPDATE {table_name}_pcode SET pcode1 = %s, pcode2 = %s WHERE id = %s"  
            cursor.execute(update_sql, (id1_value, id2_value, id_value))  
        
        connector.connection.commit()
        print(f"Successfully updated the pcode in {table_name}")  
    
    except pymysql.Error as e:
        print("An ERROR occurred:", e)
        print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)
        print("Error Message:", e)
        connector.connection.rollback()
    
    finally:
        cursor.close() 




class DatabaseConnector:
    def __init__(self, host, username, password, database,port):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database
            )
            # print("Successfully connected to MySQL database!")
        except pymysql.Error as e:
            print("An ERROR occurred while connecting to the database:", e)
            print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed")