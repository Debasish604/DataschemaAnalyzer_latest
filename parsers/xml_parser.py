import pandas as pd
import xml.etree.ElementTree as ET
import logging
from .file_parser import BaseParser

class XMLParser(BaseParser):
    """Parser for XML files"""
    
    def parse(self, file_path):
        """Parse XML file and return pandas DataFrame"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Try to automatically detect the data structure
            data = self._extract_data_from_xml(root)
            
            if data:
                df = pd.DataFrame(data)
                return self._clean_dataframe(df)
            else:
                # If no tabular data found, create a simple representation
                return self._create_simple_representation(root)
                
        except Exception as e:
            logging.error(f"Error parsing XML file {file_path}: {str(e)}")
            raise Exception(f"Failed to parse XML file: {str(e)}")
    
    def _extract_data_from_xml(self, root):
        """Extract tabular data from XML structure"""
        data = []
        
        # Strategy 1: Look for repeating child elements (common in data exports)
        children = list(root)
        if len(children) > 1:
            # Check if children have similar structure
            first_child = children[0]
            first_child_tags = set(child.tag for child in first_child)
            
            # If most children have similar structure, treat as rows
            similar_children = []
            for child in children:
                child_tags = set(subchild.tag for subchild in child)
                if len(child_tags.intersection(first_child_tags)) / len(first_child_tags) > 0.5:
                    similar_children.append(child)
            
            if len(similar_children) > 1:
                # Extract data from similar children
                for child in similar_children:
                    row = {}
                    for subchild in child:
                        row[subchild.tag] = subchild.text or ''
                    
                    # Also include attributes
                    for attr, value in child.attrib.items():
                        row[f'{child.tag}_{attr}'] = value
                    
                    data.append(row)
                
                return data
        
        # Strategy 2: Look for elements with multiple instances of the same tag
        tag_counts = {}
        for elem in root.iter():
            tag_counts[elem.tag] = tag_counts.get(elem.tag, 0) + 1
        
        # Find tags that appear multiple times (potential data rows)
        repeated_tags = {tag: count for tag, count in tag_counts.items() if count > 1}
        
        if repeated_tags:
            # Use the most frequent tag as row identifier
            row_tag = max(repeated_tags, key=repeated_tags.get)
            
            for elem in root.iter(row_tag):
                row = {}
                
                # Get text content
                if elem.text and elem.text.strip():
                    row[row_tag] = elem.text.strip()
                
                # Get attributes
                for attr, value in elem.attrib.items():
                    row[f'{row_tag}_{attr}'] = value
                
                # Get child element values
                for child in elem:
                    row[child.tag] = child.text or ''
                
                if row:  # Only add non-empty rows
                    data.append(row)
        
        return data
    
    def _create_simple_representation(self, root):
        """Create a simple DataFrame representation of XML structure"""
        data = []
        
        def extract_all_elements(element, path=""):
            current_path = f"{path}/{element.tag}" if path else element.tag
            
            row = {
                'element_path': current_path,
                'element_tag': element.tag,
                'element_text': element.text.strip() if element.text else '',
                'attributes': str(element.attrib) if element.attrib else ''
            }
            
            data.append(row)
            
            # Recursively process children
            for child in element:
                extract_all_elements(child, current_path)
        
        extract_all_elements(root)
        
        return pd.DataFrame(data)
    
    def _clean_dataframe(self, df):
        """Clean and standardize the DataFrame"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Replace empty strings with NaN
        df = df.replace('', pd.NA)
        
        return df
