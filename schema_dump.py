#!/bin/python
import argparse
import mysql.connector

class MySQLSchemaDumpAction():
    def __init__(self, db_host, db_port, db_user, db_password, output_file, exclude_schema):
        self.output_file = output_file
        self.exclude_schema = exclude_schema

        self.db = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_password
        )

        self.cursor = self.db.cursor()
        
    def __del__(self):
        self.cursor.close()
        self.db.disconnect()
        
    def list_schemas(self):
        sql = "SHOW DATABASES"

        self.cursor.execute(sql)

        result = [x[0] for x in self.cursor.fetchall()]

        return result
        
    def list_tables(self, schema):
        sql = "SHOW TABLES FROM {}".format(schema) 

        self.cursor.execute(sql)

        result = [x[0] for x in self.cursor.fetchall()]

        return result
    
    def fetch_create_stmt(self, schema, table):
        sql = "SHOW CREATE TABLE `{}`.`{}`".format(schema, table) 

        self.cursor.execute(sql)

        result = self.cursor.fetchone()

        return result[1]
    
    def dump_schema(self, split):
        schemas = self.list_schemas()

        excluded = self.exclude_schema + ["sys", "mysql", "information_schema", "liquibase_uap", "performance_schema"]
        
        for x in excluded:
            if x in schemas:
                print('Excluding schema: {}'.format(x))
                schemas.remove(x)
        
        if split:
            for schema in schemas:
                output_file='{}_{}'.format(schema, self.output_file)
                with open(output_file, 'w') as f:
                    self.write_schema(schema, f)
        else:
            with open(self.output_file, 'w') as f:
                for schema in schemas:
                    self.write_schema(schema, f)
    
    def write_schema(self, schema, f):
        customer_tables = self.list_tables(schema)

        for table in customer_tables:
            tmp = self.fetch_create_stmt(schema, table)
            tmp = tmp.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS `{}`.'.format(schema))
            tmp = "{};\n\n".format(tmp)

            f.write(tmp)

def execute(db_host, db_port, db_user, db_password, output_file, exclude_schema, split=False):
    MySQLSchemaDumpAction(
        db_host,
        db_port,
        db_user,
        db_password,
        output_file,
        exclude_schema
    ).dump_schema(split)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate the schema dump')
    parser.add_argument('--db-host', type=str, required=True, 
                        help='DB Host')
    parser.add_argument('--db-port', type=str, required=False,
                        default='3306',
                        help='DB port')
    parser.add_argument('--db-user', type=str, required=True,
                        help='DB User')
    parser.add_argument('--db-password', type=str, required=True,
                        help='DB Password')
    parser.add_argument('--output-file', type=str, required=True,
                        help='Output file') 
    parser.add_argument('--exclude-schema', type=str, required=False, 
                        default='',
                        help='Comma separated schemas to exclude')

    args = parser.parse_args()

    execute(args.db_host, args.db_port, 
        args.db_user, args.db_password, 
        args.output_file, args.exclude_schema.split(','),
        True)