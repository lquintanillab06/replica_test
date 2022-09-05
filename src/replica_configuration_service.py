
def resolver_pk(database_connection,table,db):
    sql_pk = """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE (TABLE_SCHEMA = %(db)s)
            AND (TABLE_NAME = %(table)s)
            AND (COLUMN_KEY = 'PRI')
        """
    database_connection.cursor.execute(sql_pk,{'db': db, 'table': table})
    pk =  database_connection.cursor.fetchone()["COLUMN_NAME"]
    return pk


def resolver_columnas(database_connection,table,db,action):
    sql_columns = """"""
    if action == 'update':
        sql_columns = """
             SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE (TABLE_SCHEMA = %(db)s)
             AND (TABLE_NAME = %(table)s)
             AND (COLUMN_KEY <> 'PRI')
        """

    if action == 'insert': 
        sql_columns = """
             SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE (TABLE_SCHEMA = %(db)s)
             AND (TABLE_NAME = %(table)s)
        """
    database_connection.cursor.execute(sql_columns,{'db': db, 'table': table})
    columns =  database_connection.cursor.fetchall()
    return columns

def resolver_update_query(database_connection,table,db):
    pk = resolver_pk(database_connection,table,db)
    columns = resolver_columnas(database_connection,table,db, 'update')
    res = f"UPDATE {table} SET "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            res = res + f"{column['COLUMN_NAME']} = %({column['COLUMN_NAME']})s," 
        if(idx == (len(columns)-1) ):
            res = res + f"{column['COLUMN_NAME']} = %({column['COLUMN_NAME']})s" 
    res+= f" WHERE {pk} = %({pk})s"
    #print(res)
    return res

def resolver_insert_query(database_connection,table,db):
    pk = resolver_pk(database_connection,table,db)
    columns = resolver_columnas(database_connection,table,db, 'insert')
    #print(pk)
    #print(columns)
    columnas = ''
    valores = ''
    res = f"INSERT INTO {table} "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            columnas = columnas+f"{column['COLUMN_NAME']},"
            valores = valores+f"%({column['COLUMN_NAME']})s,"
        if(idx == (len(columns)-1) ):
            columnas = columnas+f"{column['COLUMN_NAME']}"
            valores = valores+f"%({column['COLUMN_NAME']})s"
    res = res+ f"({columnas}) VALUES ({valores})"       
    return res
   

def resolver_insert_query_sin_id(database_connection,table,db):
    pk = resolver_pk(database_connection,table,db)
    columns = resolver_columnas(database_connection,table,db, 'update')
    #print(pk)
    #print(columns)
    columnas = ''
    valores = ''
    res = f"INSERT INTO {table} "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            columnas = columnas+f"{column['COLUMN_NAME']},"
            valores = valores+f"%({column['COLUMN_NAME']})s,"
        if(idx == (len(columns)-1) ):
            columnas = columnas+f"{column['COLUMN_NAME']}"
            valores = valores+f"%({column['COLUMN_NAME']})s"
    res = res+ f"({columnas}) VALUES ({valores})"       
    return res

    