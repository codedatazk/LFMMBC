import utils
import databases
import databasesOperation
import time
args = utils.parseArguments()

host = 'localhost'
username = 'root'
password = '1234'
database = 'demostration'
port = '3306'

db_connector = databases.DatabaseConnector(host, username, password, database,port)
db_connector.connect()


if args.command == 'import':
    if args.file and args.table_name:
        databases.writeFileToDatabase(args.file,args.table_name,db_connector)
        databases.createAuxiliaryTable(args.table_name,db_connector)
        databases.createTriggerTable(args.table_name,db_connector)
        databases.addTriggerToTable(args.table_name,db_connector)
        databases.createViewAndTrigger(args.table_name,db_connector)
    else : 
       print("ERROR:The parameter is incorrect, please check")
       exit()

    
elif args.command == 'read':
    if args.table_name and args.number:
        db_operation =  databasesOperation.databasesOperation(args.number,db_connector,args.table_name,args)
        db_operation.read()
    else:
        print("ERROR:The parameter is incorrect, please check")
        exit()
elif args.command == 'genpcode':
    if args.table_name:
        databases.updatePcode(args.table_name,db_connector)
    else:
        print("ERROR:The parameter is incorrect, please check")
        exit()
elif args.command == 'inspect':
    db_operation =  databasesOperation.databasesOperation(0,db_connector,args.table_name,args)
    start_time = time.time()
    db_operation.inspection()
    end_time = time.time()
    endure_time = (end_time - start_time) * 1000
elif args.command == 'write':
    db_operation =  databasesOperation.databasesOperation(args.number,db_connector,args.table_name,args)
    if args.number == -1:
        db_operation.write()
    else :
        db_operation.update()
else :
    print("ERROR:parameter error")



db_connector.disconnect()


