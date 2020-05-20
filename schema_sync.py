#!/bin/python
import argparse

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from sqlalchemydiff import compare

class SchemaSyncHelper():
    def __init__(self, baseline, destination, schema_name):
        self.baseline = baseline
        self.destination = destination
        self.schema_name = schema_name

        self.left_inspector = reflection.Inspector.from_engine(create_engine(baseline))
        self.right_inspector = reflection.Inspector.from_engine(create_engine(destination))

    def generate_alter_table_stmts(self, schema_name, left, right):
        left_tables = set(self.left_inspector.get_table_names(schema_name))
        right_tables = set(self.right_inspector.get_table_names(schema_name))
        
        stmts = []
        for table_name in left_tables:
            if table_name in right_tables:
                left_table_options = self.left_inspector.get_table_options(table_name)
                right_table_options = self.right_inspector.get_table_options(table_name)
                
                diff = set(left_table_options.items()) - set(right_table_options.items())
                if diff:
                    charset = left_table_options['mysql_default charset']

                    if 'mysql_collate' in left_table_options:
                        collate = left_table_options['mysql_collate']
                    else:
                        collate = 'utf8mb4_unicode_520_ci'
                    
                    stmt = 'ALTER TABLE `{}`.`{}` CHARACTER SET {} COLLATE {};'.format(schema_name, table_name, charset, collate)
                    stmts.append(stmt)

        return stmts

    def __find_column_diff(self, table_name):
        left_columns = self.left_inspector.get_columns(table_name)
        right_columns = self.right_inspector.get_columns(table_name)

        s_left = set([c['name'].lower() for c in left_columns])
        s_right = set([c['name'].lower() for c in right_columns])

        left_only_cols = list(s_left - s_right)
        right_only_cols = list(s_right - s_left)
        
        return left_only_cols, right_only_cols

    def __find_index_diff(self, table_name):
        left_indices = self.left_inspector.get_indexes(table_name)
        right_indices = self.right_inspector.get_indexes(table_name)
        
        s_left = set([c['name'].lower() for c in left_indices])
        s_right = set([c['name'].lower() for c in right_indices])

        left_only = list(s_left - s_right)
        right_only = list(s_right - s_left)
        
        return left_only, right_only
            
    def generate_column_index_diff_report(self, schema_name):
        left_tables = set(self.left_inspector.get_table_names(schema_name))
        right_tables = set(self.right_inspector.get_table_names(schema_name))

        result = []
        
        for table_name in left_tables:
            if table_name in right_tables:
                left_only_cols, right_only_cols = self.__find_column_diff(table_name)
                left_only_indices, right_only_indices = self.__find_index_diff(table_name)
                
                if left_only_cols or right_only_cols:
                    result.append({
                        'table': '`{}`.`{}`'.format(schema_name, table_name),
                        'left_only_cols': left_only_cols,
                        'right_only_cols': right_only_cols,
                        "left_only_indices": left_only_indices, 
                        "right_only_indices": right_only_indices
                    })
                    
                
        return result
    
    def geneate_add_drop_columns(self, schema_name):
        left_tables = set(self.left_inspector.get_table_names(schema_name))
        right_tables = set(self.right_inspector.get_table_names(schema_name))

        add_col_stmts = []
        drop_col_stmts = []
        
        for table_name in left_tables:
            if table_name in right_tables:
                left_only_cols, right_only_cols = self.__find_column_diff(table_name)
                
                left_columns = self.left_inspector.get_columns(table_name)
                
                idx = {}
                for c in left_columns:
                    idx[c['name'].lower()] = c

                for column in left_only_cols:
                    column_def = idx[column]
                    
                    datatype = column_def['type']
                    nullable = '' if column_def['nullable'] else 'NOT NULL'
                    default = 'DEFAULT {}'.format(column_def['default']) if column_def['default'] else ''
                    
                    add_col_stmts.append('ALTER TABLE `{}`.`{}` ADD COLUMN `{}` {} {} {};'.format(schema_name, table_name, column, datatype, nullable, default))
                    
                for column in right_only_cols:
                    drop_col_stmts.append('ALTER TABLE `{}`.`{}` DROP COLUMN `{}`;'.format(schema_name, table_name, column))
                    
        return add_col_stmts, drop_col_stmts

def generate_alter_column_stmts(schema_name, result):
    tables_data = result.errors['tables_data']

    stmts = []
    for table_name in tables_data.keys():
        if 'columns' in tables_data[table_name]:
            if 'diff' in tables_data[table_name]['columns']:
                #print(tables_data[table_name]['columns']['diff'])
                for item in tables_data[table_name]['columns']['diff']:
                    column_name = item['key']
                    data_type = item['left']['type']
                    nullable = '' if item['left']['nullable'] else 'NOT NULL'
                    default = 'DEFAULT {}'.format(item['left']['default']) if item['left']['default'] else ''
                    
                    stmt = 'ALTER TABLE `{}`.`{}` CHANGE COLUMN `{}` `{}` {} {} {};'.format(
                        schema_name,
                        table_name,
                        column_name,
                        column_name,
                        data_type,
                        nullable,
                        default
                    )
                    
                    stmts.append(stmt)
                
    return stmts

def execute(baseline, destination, schemas):
    missing = pd.DataFrame(columns=['table', 'left_only_cols', 'right_only_cols', 'left_only_indices', 'right_only_indices'])
    
    for schema_name in schemas:
        left = '{}/{}'.format(baseline, schema_name)
        right = '{}/{}'.format(destination, schema_name)

        tool = SchemaSyncHelper(left, right, schema_name)
        
        alter_table_stmts = tool.generate_alter_table_stmts(schema_name, left, right)

        result = compare(left, right)
        alter_column_stmts = generate_alter_column_stmts(schema_name, result)
        add_col_stmts, drop_col_stmts = tool.geneate_add_drop_columns(schema_name)

        if alter_table_stmts or alter_column_stmts or add_col_stmts or drop_col_stmts:
            file_name = 'output/{}-ddl.sql'.format(schema_name)
            print("Generating ddl: {}".format(file_name))

            with open(file_name, 'w') as f:
                if alter_table_stmts:
                    f.write('\n\n'.join(alter_table_stmts))
                    f.write('\n\n')

                if alter_column_stmts:    
                    f.write('\n\n'.join(alter_column_stmts))
                    f.write('\n\n')
                    
                if add_col_stmts:    
                    f.write('\n\n'.join(add_col_stmts))
                    f.write('\n\n')
                    
                if drop_col_stmts:    
                    f.write('\n\n'.join(drop_col_stmts))
                    f.write('\n\n')

        diff = tool.generate_column_index_diff_report(schema_name)
        if diff:
            df = pd.DataFrame.from_dict(diff)
            missing = missing.append(df, ignore_index=True)
    
    report_file_name = 'output/missing-cols-indices.csv'
    print("Generating report: {}".format(report_file_name))
    
    missing.to_csv(report_file_name)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate SQL script to sync destination DB with baseline')
    parser.add_argument('--baseline', type=str, required=True, 
                        help='Baseline DB')
    parser.add_argument('--destination', type=str, required=True,
                        help='Destination DB')

    parser.add_argument('--schemas', type=str, required=True,
                        help='Comma separated list of schemas to compare')

    args = parser.parse_args()

    execute(args.baseline, args.destination, args.schemas.split(','))