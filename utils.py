import argparse


def parseArguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    import_parser = subparsers.add_parser('import', help='Import command')
    import_parser.add_argument('-f', '--file', required=True, help='File path')
    import_parser.add_argument('-t','--table_name',type=str,help="Table name")

    read_parser = subparsers.add_parser('read', help='Read command')
    read_parser.add_argument('-t','--table_name',type=str,help="Table name")
    read_parser.add_argument('-n','--number',type=str,help="The number that needs to be read")

    write_parser = subparsers.add_parser('write', help='Write command')
    write_parser.add_argument('-t', '--table_name', type=str, help="Table name")
    write_parser.add_argument('-n', '--number', type=int, default=-1,help='Number parameter')

    inspect_parser = subparsers.add_parser('inspect', help='Inspect command')
    inspect_parser.add_argument('-t','--table_name',type=str,help="Table name")
    
    inspect_parser = subparsers.add_parser('genpcode', help='Updatepcode command')
    inspect_parser.add_argument('-t','--table_name',type=str,help="Table name")

    args = parser.parse_args()
    return args
