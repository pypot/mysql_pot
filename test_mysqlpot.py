import mysqlpot
import sys
import getopt
    
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:o:u:p:d:v')
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(-2)
    
    para_dict = {}
    for o, v in opts:
        if o == '-h':
            para_dict['host'] = v
        elif o == '-o':
            para_dict['port'] = int(v)
        elif o == '-u':
            para_dict['user'] = v
        elif o == '-p':
            para_dict['passwd'] = v
        elif o == '-d':
            para_dict['db'] = v
    
    sqlConn = mysqlpot.MySqlOperator.get_conn(**para_dict)
    while (True):
        sqlCmd = raw_input("sql>")
        if sqlCmd == 'exit':
            sys.exit(0)
        print sqlConn.execute_with_result(sqlCmd)




