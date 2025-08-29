import pandas as pd
import logging
from abc import ABC, abstractmethod

class BaseParser(ABC):
    """Abstract base class for file parsers"""
    
    @abstractmethod
    def parse(self, file_path):
        """Parse file and return pandas DataFrame"""
        pass

class FileParserFactory:
    """Factory class to get appropriate parser for file type"""
    
    def __init__(self):
        from .csv_parser import CSVParser
        from .sql_parser import SQLParser
        from .excel_parser import ExcelParser
        from .xml_parser import XMLParser
        
        self.parsers = {
            'csv': CSVParser(),
            'sql': SQLParser(),
            'xls': ExcelParser(),
            'xlsx': ExcelParser(),
            'xml': XMLParser()
        }
    
    def get_parser(self, file_type):
        """Get parser for specific file type"""
        parser = self.parsers.get(file_type.lower())
        if not parser:
            raise ValueError(f"Unsupported file type: {file_type}")
        return parser
