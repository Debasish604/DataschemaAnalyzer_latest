import pandas as pd
import sqlparse
import re
import logging
from .file_parser import BaseParser

class SQLParser(BaseParser):
    """Parser for SQL files"""
    
    def parse(self, file_path):
        """Parse SQL file and extract table data"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            # Parse SQL statements
            statements = sqlparse.split(sql_content)
            
            tables_data = {}
            
            for statement in statements:
                if statement.strip():
                    parsed = sqlparse.parse(statement)[0]
                    
                    # Look for CREATE TABLE statements
                    if self._is_create_table(statement):
                        table_info = self._extract_table_info(statement)
                        if table_info:
                            tables_data[table_info['name']] = table_info
                    
                    # Look for INSERT statements and extract sample data
                    elif self._is_insert_statement(statement):
                        insert_data = self._extract_insert_data(statement)
                        if insert_data:
                            table_name = insert_data['table']
                            if table_name in tables_data:
                                if 'sample_data' not in tables_data[table_name]:
                                    tables_data[table_name]['sample_data'] = []
                                tables_data[table_name]['sample_data'].extend(insert_data['rows'])
            
            # Convert to DataFrame format
            if tables_data:
                # If multiple tables, combine them or return the first substantial one
                return self._convert_to_dataframe(tables_data)
            else:
                # If no structured data found, create empty DataFrame
                return pd.DataFrame()
                
        except Exception as e:
            logging.error(f"Error parsing SQL file {file_path}: {str(e)}")
            raise Exception(f"Failed to parse SQL file: {str(e)}")
    
    def _is_create_table(self, statement):
        """Check if statement is CREATE TABLE"""
        return re.match(r'\s*CREATE\s+TABLE\s+', statement, re.IGNORECASE)
    
    def _is_insert_statement(self, statement):
        """Check if statement is INSERT"""
        return re.match(r'\s*INSERT\s+INTO\s+', statement, re.IGNORECASE)
    
    def _extract_table_info(self, statement):
        """Extract table information from CREATE TABLE statement"""
        try:
            # Extract table name
            table_match = re.search(r'CREATE\s+TABLE\s+(?:`)?(\w+)(?:`)?', statement, re.IGNORECASE)
            if not table_match:
                return None
            
            table_name = table_match.group(1)
            
            # Extract column definitions
            columns = []
            column_pattern = r'(\w+)\s+(\w+(?:\(\d+\))?)'
            
            # Find the part between parentheses after table name
            paren_match = re.search(r'\((.*)\)', statement, re.DOTALL)
            if paren_match:
                column_defs = paren_match.group(1)
                
                for line in column_defs.split(','):
                    line = line.strip()
                    col_match = re.match(column_pattern, line)
                    if col_match:
                        col_name = col_match.group(1)
                        col_type = col_match.group(2)
                        columns.append({'name': col_name, 'type': col_type})
            
            return {
                'name': table_name,
                'columns': columns,
                'original_statement': statement
            }
            
        except Exception as e:
            logging.warning(f"Could not extract table info: {str(e)}")
            return None
    
    def _extract_insert_data(self, statement):
        """Extract data from INSERT statement"""
        try:
            # Extract table name
            table_match = re.search(r'INSERT\s+INTO\s+(?:`)?(\w+)(?:`)?', statement, re.IGNORECASE)
            if not table_match:
                return None
            
            table_name = table_match.group(1)
            
            # Extract values
            values_match = re.search(r'VALUES\s*(.+)', statement, re.IGNORECASE | re.DOTALL)
            if not values_match:
                return None
            
            values_part = values_match.group(1)
            
            # Parse individual value tuples
            rows = []
            # Simple regex to find value tuples
            tuple_pattern = r'\(([^)]+)\)'
            
            for match in re.finditer(tuple_pattern, values_part):
                values_str = match.group(1)
                # Split by comma and clean up values
                values = [v.strip().strip("'\"") for v in values_str.split(',')]
                rows.append(values)
            
            return {
                'table': table_name,
                'rows': rows
            }
            
        except Exception as e:
            logging.warning(f"Could not extract insert data: {str(e)}")
            return None
    
    def _convert_to_dataframe(self, tables_data):
        """Convert parsed table data to pandas DataFrame"""
        # For simplicity, take the first table with sample data
        for table_name, table_info in tables_data.items():
            if 'sample_data' in table_info and table_info['sample_data']:
                columns = [col['name'] for col in table_info['columns']]
                
                # Create DataFrame from sample data
                df = pd.DataFrame(table_info['sample_data'], columns=columns[:len(table_info['sample_data'][0])])
                return df
        
        # If no sample data, create DataFrame with just column structure
        for table_name, table_info in tables_data.items():
            if table_info['columns']:
                columns = [col['name'] for col in table_info['columns']]
                return pd.DataFrame(columns=columns)
        
        return pd.DataFrame()
