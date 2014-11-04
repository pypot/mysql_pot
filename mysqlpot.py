#!/usr/bin/python
#-*- coding:utf-8 -*-

#author yangfan51
#date   2014-04-15
#brief  wrap the mysql operation
import MySQLdb as mysql
import traceback
import datetime


class MySqlOperator(object):
    """SQL日志工具
    """
    connectPool = dict()

    @staticmethod
    def get_conn(host, port, user, passwd, db):
        '''
            kwargs: host, port, user, passwd, db, charset
        '''
        connSign = "%s:%s|%s@%s|%s" % (host, port, user, passwd, db)
        if connSign in MySqlOperator.connectPool:
            return  MySqlOperator.connectPool[connSign]
        try:
            oprt = MySqlOperator(host, port, user, passwd, db)
            MySqlOperator.connectPool[connSign] = oprt
            return oprt
        except Exception as e:
            print traceback.format_exc()
            return None



    def __init__(self, host, port, user, passwd, db):
        self.sign = "%s:%s|%s@%s|%s" % (host, port, user, passwd, db)
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.conn = None
        self.cursor = None
        self.__connect() #初始化self.conn


    def __del__(self):
        del MySqlOperator.connectPool[self.sign]
        self.close()

    def __connect(self):
        try:
            self.conn = mysql.connect(host=self.host,
                    port=self.port,
                    user=self.user,
                    passwd=self.passwd,
                    db=self.db,
                    charset='utf8')
            self.conn.autocommit(True)
            self.cursor = self.conn.cursor(cursorclass=mysql.cursors.DictCursor)
        except Exception as e:
            print traceback.format_exc()
            raise e


    def close(self):
        self.cursor.close()
        self.conn.close()



    def execute(self, sqlCmd):
        #保持连接
        try:
            #MySQL1.2.2版本后支持自动重连(8小时idle会断线)
            self.conn.ping(True)
        except Exception as e:
            #重试一次连接
            self.__connect()
            #再失败抛异常，不再捕获
        try:
            changeRows = self.cursor.execute(sqlCmd)
            self.conn.commit()
            return changeRows
        except:
            print traceback.format_exc()


    def get_result(self):
        return self.cursor.fetchall()


    def execute_with_result(self, sqlCmd):
        self.execute(sqlCmd)
        return self.get_result()




