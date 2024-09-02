import pymysql
import hash
from itertools import chain
import databases
import sys
import time

class databasesOperation:
    def __init__(self,n,connector,table_name,args):
        self.n = n
        self.connector = connector
        self.table_name = table_name
        self.args = args

    def read(self):
        curnumber = int(self.n)
        pcode1_correct = hash.Link_1(curnumber)
        pcode2_correct = hash.Link_2(curnumber)
        cursor = self.connector.connection.cursor()
        pcode1_input=input(f"Pcode 1:")
        pcode2_input=input(f"Pcode 2:")
        start_time = time.time()
        try:
            sql = f"SELECT pcode1, pcode2 FROM {self.table_name}_pcode where id={self.n}"

            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                pcode1_table = row[0]
                pcode2_table = row[1]
        except Exception as e:
            pass
            print("An ERROR occurred:", e)
            print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)

        if pcode1_correct == pcode1_table and pcode1_table == pcode1_input and pcode2_correct == pcode2_table and pcode2_table == pcode2_input:
            try:  
                sql1 = f"SELECT * FROM {self.table_name} LIMIT {int(self.n)-1}, 1;"   
                cursor.execute(sql1)  
                row = cursor.fetchone()  
                row = row[:10]
                # print(row)
                if row:  
                    columns = [col[0] for col in cursor.description]  
                    row_dict = dict(zip(columns, row))  
                    for key, value in row_dict.items():  
                        print(f"{key}: {value}")  
            except Exception as e:  
                print("An ERROR occurred:", e)  
                print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno) 
            end_time = time.time()
            endure_time = (end_time - start_time) * 1000
            print(f"read takes {endure_time} ms")
        else:
            print("INFO : database can't read！")


    def write(self):
        cursor = self.connector.connection.cursor()

        try:
            getIDsql = f"SELECT COUNT(*) FROM {self.table_name};"
            cursor.execute(getIDsql)
            result = cursor.fetchone()
        except Exception as e:  
                print(f"An error occurred: {e}")  
                self.connector.connection.rollback()
        id = int(result[0])
        print(f"The number of rows currently inserted is {id+1}")

        cursor.execute(f"SHOW COLUMNS FROM {self.table_name}") 
        columns_info = cursor.fetchall()  
        columns = [column_info[0] for column_info in columns_info] 
        columns = columns[:10]
        col_names = ', '.join(['`' + col + '`' for col in columns])  
        Valuesplace = ', '.join(['%s'] * len(columns))  
        insert_sql = f"INSERT INTO {self.table_name} ({col_names}) VALUES ({Valuesplace})" 
        data_values = []  
        #Get input from keyboard
        for col in columns:  
            data_value = input(f"Add value for {col}: ")  
            data_values.append(data_value)  
        #SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = Cilia_dataset;
        # Check the pcode
        pcode1_correct = hash.Link_1(id+1)
        pcode2_correct = hash.Link_2(id+1)
        pcode1_input=input(f"Pcode 1:")
        pcode2_input=input(f"Pcode 2:")
        start_time = time.time()
        pcodesql = f"INSERT INTO {self.table_name}_pcode (id,pcode1, pcode2) VALUES ({id+1},'{pcode1_correct}', '{pcode2_correct}');"
        # pcodesql = f"UPDATE {self.table_name}_pcode SET pcode1 = '{pcode1_correct}', pcode2 = '{pcode2_correct}' WHERE id = {id+1};"
        try:
            cursor.execute(pcodesql)
        except Exception as e:  
            print(f"An error occurred: {e}")  
            self.connector.connection.rollback()

        if pcode1_correct == pcode1_input and pcode2_correct == pcode2_input:
            print("INFO : Pcode are correct!")
            try:  
                cursor.execute(insert_sql, tuple(data_values))  
                self.connector.connection.commit()
                print(f"INFO : The data has been successfully inserted into {self.table_name}.")  
            except pymysql.Error as e:  
                print(f"An error occurred: {e}")  
                self.connector.connection.rollback()
            finally:
                cursor.close()
            end_time = time.time()
            endure_time = (end_time - start_time) * 1000
            print(f"Write takes {endure_time} ms")
        else :
            print("INFO : Pcode error, please check !")
            try:  
                delete = f"DELETE FROM {self.table_name}_pcode WHERE id = {id+1};"  
                cursor.execute(delete)  
                self.connector.connection.commit()
            except pymysql.Error as e:  
                print("An ERROR occurred during UPDATE:", e)  
                print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            cursor.close() 

    def genpcode(self):
        pcode_first=input(f"Pcode 1:")
        pcode_second=input(f"Pcode 2:")

        cursor = self.connector.connection.cursor()
        checksql = f"SELECT pcode1 FROM {self.table_name}_pcode where id= {self.n}"
        cursor.execute(checksql)
        row = cursor.fetchone()
        if row is not None:
            try:
                update1 = f"UPDATE {self.table_name}_pcode SET pcode1 = '{pcode_first}' WHERE id = {self.n}"
                cursor.execute(update1)
                self.connector.connection.commit()
            except pymysql.Error as e:
                print("An ERROR occurred:", e)
                print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
                self.connector.connection.rollback()

        else:
            try:
                update2 = f"insert into {self.table_name}_pcode(id,pcode1) values ({self.n},'{pcode_first}')"
                cursor.execute(update2)
                self.connector.connection.commit()
            except pymysql.Error as e:
                print("An ERROR occurred:", e)
                print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
                self.connector.connection.rollback()
            self.connector.connection.rollback()

        cursor = self.connector.connection.cursor()
        
        try:
            update3 = f"UPDATE {self.table_name}_pcode SET pcode2 = '{pcode_second}' WHERE id = {self.n}"
            cursor.execute(update3)
            self.connector.connection.commit()
        except pymysql.Error as e:
            print("An ERROR occurred:", e)
            print(sys._getframe().f_code.co_filename,sys._getframe().f_lineno)
        self.connector.connection.rollback()

        check = self.inspect1()
        if (check):
            cursor = self.connector.connection.cursor()

            self.connector.connection.commit()
        else:
            print("INFO : Failed to generate the pcode!")
        cursor.close()
        
    def inspect1(self):  
        pcode1_correct = hash.Link_1(int(self.n))  
        pcode2_correct = hash.Link_2(int(self.n))  
        try:  
            cursor = self.connector.connection.cursor()  
            query = f"SELECT * FROM {self.table_name}_trigger_record WHERE id ={self.n} ORDER BY Nid DESC LIMIT 1"  
            cursor.execute(query)  
            result = cursor.fetchone()  
            if result is None:  
                # print("INFO: No record found :")  
                return False  
            pcode1_input = result[2]  
            pcode2_input = result[3]  
        except pymysql.Error as e:  
            print("An ERROR occurred :", e)  
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            return False 
    
        if pcode1_correct == pcode1_input and pcode2_correct == pcode2_input:  
            print("INFO : Protection Code are correct!")  
            return True  
        else:  
            print("INFO : Inspect can't pass!")  
            try:  
                delete = f"UPDATE {self.table_name}_pcode SET pcode1 = NULL, pcode2 = NULL WHERE id = {self.n};"  
                cursor.execute(delete)  
                self.connector.connection.commit()
                print("INFO: Protection codes updated to NULL.")  
            except pymysql.Error as e:  
                print("An ERROR occurred during UPDATE:", e)  
                print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            finally:  
                cursor.close() 
            return False
 
    def inspection(self, batch_size=100): 
        error_ids = []
        try:    
            while True:  
                self.connector.connect()
                cursor = self.connector.connection.cursor() 
                offset = 0  
                while True:  
                    query = f"SELECT id, pcode1, pcode2 FROM {self.table_name}_pcode LIMIT {batch_size} OFFSET {offset};"  
                    cursor.execute(query)  
                    result = cursor.fetchall()  
                    if not result:  
                        break 
                    for row in result:  
                        id_value = int(row[0])  
                        if id_value in error_ids:
                            continue 
                        pcode1_GetFromTable = row[1]  
                        pcode2_GetFromTable = row[2]  
                        pcode1_Expected = hash.Link_1(id_value)  
                        pcode2_Expected = hash.Link_2(id_value)  
                        if pcode1_GetFromTable != pcode1_Expected or pcode2_GetFromTable != pcode2_Expected: 
                            error_ids.append(id_value)
                            print(f"Warning: Incorrect values in pcode1 and/or pcode2 columns with NO.{id_value}") 
                    offset += batch_size  
                cursor.close()
    
        except pymysql.Error as e:  
            print("An ERROR occurred :", e)  
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            return False
    
    def update(self):
        cursor = self.connector.connection.cursor()
        #Gets the column names in the table
        cursor.execute(f"SHOW COLUMNS FROM {self.table_name}")  
        columns_info = cursor.fetchall()  
        columns = [column_info[0] for column_info in columns_info]  
        columns = columns[:10]  
        
        set_clause = ', '.join([f"`{column_name}`=%s" for column_name in columns])  
        
        try:  
            sql1 = f"SELECT * FROM {self.table_name} LIMIT {int(self.n)-1}, 1;"  
            cursor.execute(sql1)  
            row = cursor.fetchone()  
            
            if row:   
                columns = [col[0] for col in cursor.description][:10]  
                  
                conditions = [f"`{col}` = '{value}'" if isinstance(value, str) else f"`{col}` = {value}"    
                            for col, value in zip(columns, row[:10]) if value is not None]  
                  
                where_clause = " AND ".join(conditions)  
                print(f"WHERE {where_clause}")  
        
        except pymysql.Error as e:  
            print("An ERROR occurred during the query:", e)  
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  

        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};"  
        data_values = []  
        #Get input from keyboard
        print (len(columns))
        for col in columns:  
            data_value = input(f"Add value for {col}: ")  
            data_values.append(data_value)  
        # Check the pcode
        # pcode1_correct = hash.Link_1(int(self.args.number))

        # pcode2_correct = hash.Link_2(int(self.args.number))
        pcode1_correct = '1'
        pcode2_correct = '1'
        pcode1_input=input(f"Pcode 1:")
        pcode2_input=input(f"Pcode 2:")
        start_time = time.time()
        pcodesql = "UPDATE {} SET pcode1=%s, pcode2=%s WHERE id=%s".format(self.table_name + '_pcode') 
        try:
            cursor.execute(pcodesql, (pcode1_correct, pcode2_correct, self.args.number))  
        except Exception as e:  
            print(f"An error occurred: {e}")  
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            self.connector.connection.rollback()

        if pcode1_correct == pcode1_input and pcode2_correct == pcode2_input:
            print("INFO : Pcode are correct!")
            try:  
                cursor.execute(update_sql, tuple(data_values))  
                # print((update_sql, tuple(data_values)))
                self.connector.connection.commit()
                print(f"INFO : The data has been successfully inserted into {self.table_name}.")  
            except pymysql.Error as e:  
                print(f"An error occurred: {e}")  
                print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
                self.connector.connection.rollback()
            finally:
                cursor.close()
            end_time = time.time()
            endure_time = (end_time - start_time) * 1000
            print(f"Update takes {endure_time} ms")
        else :
            print("INFO : Pcode error, please check !")
            try:  
                delete = f"DELETE FROM {self.table_name}_pcode WHERE id = {int(self.args.number)};"  
                cursor.execute(delete)  
                self.connector.connection.commit()
            except pymysql.Error as e:  
                print("An ERROR occurred during UPDATE:", e)  
                print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  
            cursor.close()
            
    def update1(self):
        cursor = self.connector.connection.cursor()
        #Gets the column names in the table
        cursor.execute(f"SHOW COLUMNS FROM {self.table_name}")  
        columns_info = cursor.fetchall()  
        columns = [column_info[0] for column_info in columns_info]  
        columns = columns[:10]  
        
        set_clause = ', '.join([f"{column_name}=%s" for column_name in columns])  
        
        try:  
            sql1 = f"SELECT * FROM {self.table_name} LIMIT {int(self.n)-1}, 1;"  
            cursor.execute(sql1)  
            row = cursor.fetchone()  
            
            if row:   
                columns = [col[0] for col in cursor.description][:10]  
                 
                conditions = [f"{col} = '{value}'" if isinstance(value, str) else f"{col} = {value}"    
                            for col, value in zip(columns, row[:10]) if value is not None]    
                where_clause = " AND ".join(conditions)  
                print(f"WHERE {where_clause}")  
        
        except pymysql.Error as e:  
            print("An ERROR occurred during the query:", e)  
            print(sys._getframe().f_code.co_filename, sys._getframe().f_lineno)  

        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};"  
        data_values = []  
        #Get input from keyboard
        print (len(columns))
        for col in columns:  
            data_value = input(f"Add value for {col}: ")  
            data_values.append(data_value)  
        # Check the pcode
        start_time = time.time()
        
        try:  
            cursor.execute(update_sql, tuple(data_values))  
            self.connector.connection.commit()
            print(f"INFO : The data has been successfully inserted into {self.table_name}.")  
        except pymysql.Error as e:  
            print(f"An error occurred: {e}")  
            self.connector.connection.rollback()
        finally:
            cursor.close()
        end_time = time.time()
        endure_time = (end_time - start_time) * 1000
        print(f"Update takes {endure_time} ms")
            