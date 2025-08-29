import pandas as pd
import numpy as np
import logging
from itertools import combinations

class RelationshipAnalyzer:
    """Analyzer for detecting relationships between tables and suggesting join strategies"""
    
    def analyze_relationships(self, dataframes_dict):
        """Analyze relationships between multiple DataFrames"""
        results = {
            'potential_keys': self._identify_potential_keys(dataframes_dict),
            'relationships': self._detect_relationships(dataframes_dict),
            'join_suggestions': self._suggest_joins(dataframes_dict),
            'foreign_key_candidates': self._identify_foreign_keys(dataframes_dict)
        }
        
        return results
    
    def _identify_potential_keys(self, dataframes_dict):
        """Identify potential primary keys in each table"""
        potential_keys = {}
        
        for table_name, df in dataframes_dict.items():
            keys = []
            
            for col in df.columns:
                key_score = self._calculate_key_score(df[col], col)
                
                if key_score['is_potential_key']:
                    keys.append({
                        'column': col,
                        'key_type': key_score['key_type'],
                        'uniqueness': key_score['uniqueness'],
                        'completeness': key_score['completeness'],
                        'score': key_score['score']
                    })
            
            # Sort by score (descending)
            keys.sort(key=lambda x: x['score'], reverse=True)
            potential_keys[table_name] = keys
        
        return potential_keys
    
    def _calculate_key_score(self, column, column_name):
        """Calculate how likely a column is to be a key"""
        col_lower = column_name.lower()
        
        # Basic statistics
        total_count = len(column)
        non_null_count = column.notna().sum()
        unique_count = column.nunique()
        
        if total_count == 0:
            return {'is_potential_key': False, 'score': 0}
        
        completeness = non_null_count / total_count
        uniqueness = unique_count / non_null_count if non_null_count > 0 else 0
        
        # Base score
        score = 0
        key_type = 'unknown'
        
        # Completeness weight (keys should have few nulls)
        score += completeness * 30
        
        # Uniqueness weight (keys should be unique)
        score += uniqueness * 40
        
        # Column name hints
        id_indicators = ['id', 'key', 'pk', 'primary', 'code', 'number', 'no']
        if any(indicator in col_lower for indicator in id_indicators):
            score += 20
            key_type = 'identifier'
        
        # Pattern analysis
        if uniqueness > 0.95 and completeness > 0.9:
            score += 10
            if key_type == 'unknown':
                key_type = 'unique_identifier'
        
        # Check for sequential numeric pattern (auto-increment IDs)
        if column.dtype in ['int64', 'int32'] and uniqueness > 0.95:
            try:
                sorted_values = column.dropna().sort_values()
                if len(sorted_values) > 1:
                    diffs = sorted_values.diff().dropna()
                    if (diffs == 1).mean() > 0.8:  # Mostly incremental by 1
                        score += 15
                        key_type = 'auto_increment'
            except:
                pass
        
        # Penalize very long text keys
        if column.dtype == 'object':
            try:
                avg_length = column.astype(str).str.len().mean()
                if avg_length > 50:
                    score -= 10
            except:
                pass
        
        is_potential_key = score > 50 and uniqueness > 0.8 and completeness > 0.8
        
        return {
            'is_potential_key': is_potential_key,
            'key_type': key_type,
            'uniqueness': uniqueness,
            'completeness': completeness,
            'score': score
        }
    
    def _detect_relationships(self, dataframes_dict):
        """Detect relationships between tables"""
        relationships = []
        
        table_pairs = list(combinations(dataframes_dict.keys(), 2))
        
        for table1, table2 in table_pairs:
            df1 = dataframes_dict[table1]
            df2 = dataframes_dict[table2]
            
            # Find common columns
            common_columns = set(df1.columns) & set(df2.columns)
            
            for col in common_columns:
                relationship = self._analyze_column_relationship(
                    df1[col], df2[col], col, table1, table2
                )
                
                if relationship['strength'] > 0.5:
                    relationships.append(relationship)
            
            # Check for similar column names (potential renamed foreign keys)
            similar_cols = self._find_similar_columns(df1.columns, df2.columns)
            
            for col1, col2, similarity in similar_cols:
                if similarity > 0.7:  # High similarity threshold
                    relationship = self._analyze_column_relationship(
                        df1[col1], df2[col2], f"{col1}↔{col2}", table1, table2
                    )
                    
                    if relationship['strength'] > 0.4:  # Lower threshold for renamed columns
                        relationship['is_renamed'] = True
                        relationships.append(relationship)
        
        return relationships
    
    def _analyze_column_relationship(self, col1, col2, column_name, table1, table2):
        """Analyze relationship between two columns"""
        # Get non-null values
        values1 = set(col1.dropna())
        values2 = set(col2.dropna())
        
        if not values1 or not values2:
            return {
                'column': column_name,
                'table1': table1,
                'table2': table2,
                'strength': 0,
                'relationship_type': 'no_data'
            }
        
        # Calculate overlap
        intersection = values1 & values2
        union = values1 | values2
        
        # Jaccard similarity
        jaccard = len(intersection) / len(union) if union else 0
        
        # Value containment ratios
        containment_1_in_2 = len(intersection) / len(values1)
        containment_2_in_1 = len(intersection) / len(values2)
        
        # Determine relationship type
        relationship_type = 'unknown'
        strength = jaccard
        
        if jaccard > 0.8:
            relationship_type = 'strong_overlap'
        elif containment_1_in_2 > 0.9:
            relationship_type = 'table1_subset_of_table2'
            strength = containment_1_in_2
        elif containment_2_in_1 > 0.9:
            relationship_type = 'table2_subset_of_table1'
            strength = containment_2_in_1
        elif jaccard > 0.3:
            relationship_type = 'partial_overlap'
        elif len(intersection) > 0:
            relationship_type = 'weak_overlap'
        else:
            relationship_type = 'no_overlap'
            strength = 0
        
        return {
            'column': column_name,
            'table1': table1,
            'table2': table2,
            'strength': strength,
            'relationship_type': relationship_type,
            'jaccard_similarity': jaccard,
            'values_in_common': len(intersection),
            'unique_to_table1': len(values1 - values2),
            'unique_to_table2': len(values2 - values1),
            'containment_1_in_2': containment_1_in_2,
            'containment_2_in_1': containment_2_in_1
        }
    
    def _find_similar_columns(self, cols1, cols2):
        """Find columns with similar names between two sets"""
        similar_pairs = []
        
        for col1 in cols1:
            for col2 in cols2:
                if col1 != col2:  # Don't compare identical names
                    similarity = self._calculate_name_similarity(col1, col2)
                    if similarity > 0.6:  # Threshold for similarity
                        similar_pairs.append((col1, col2, similarity))
        
        return similar_pairs
    
    def _calculate_name_similarity(self, name1, name2):
        """Calculate similarity between two column names"""
        name1_lower = name1.lower().strip()
        name2_lower = name2.lower().strip()
        
        # Exact match
        if name1_lower == name2_lower:
            return 1.0
        
        # Simple substring matching
        if name1_lower in name2_lower or name2_lower in name1_lower:
            return 0.8
        
        # Check for common patterns (e.g., id vs _id, customer_id vs customerid)
        clean1 = name1_lower.replace('_', '').replace('-', '')
        clean2 = name2_lower.replace('_', '').replace('-', '')
        
        if clean1 == clean2:
            return 0.9
        
        # Levenshtein distance-based similarity (simple implementation)
        return self._simple_levenshtein_similarity(name1_lower, name2_lower)
    
    def _simple_levenshtein_similarity(self, s1, s2):
        """Calculate similarity using simple Levenshtein distance"""
        if len(s1) == 0 or len(s2) == 0:
            return 0
        
        # Create matrix
        matrix = [[0] * (len(s2) + 1) for _ in range(len(s1) + 1)]
        
        # Initialize first row and column
        for i in range(len(s1) + 1):
            matrix[i][0] = i
        for j in range(len(s2) + 1):
            matrix[0][j] = j
        
        # Fill matrix
        for i in range(1, len(s1) + 1):
            for j in range(1, len(s2) + 1):
                if s1[i-1] == s2[j-1]:
                    cost = 0
                else:
                    cost = 1
                
                matrix[i][j] = min(
                    matrix[i-1][j] + 1,      # deletion
                    matrix[i][j-1] + 1,      # insertion
                    matrix[i-1][j-1] + cost  # substitution
                )
        
        # Calculate similarity as 1 - (distance / max_length)
        distance = matrix[len(s1)][len(s2)]
        max_length = max(len(s1), len(s2))
        
        return 1 - (distance / max_length) if max_length > 0 else 0
    
    def _identify_foreign_keys(self, dataframes_dict):
        """Identify potential foreign key relationships"""
        foreign_keys = []
        
        # Get potential primary keys first
        potential_keys = self._identify_potential_keys(dataframes_dict)
        
        for source_table, source_df in dataframes_dict.items():
            for target_table, target_df in dataframes_dict.items():
                if source_table == target_table:
                    continue
                
                # Get best primary key from target table
                target_keys = potential_keys.get(target_table, [])
                if not target_keys:
                    continue
                
                best_target_key = target_keys[0]['column']
                target_values = set(target_df[best_target_key].dropna())
                
                # Check each column in source table for foreign key potential
                for col in source_df.columns:
                    if col == best_target_key:
                        continue  # Skip if same column name (already handled in relationships)
                    
                    source_values = set(source_df[col].dropna())
                    
                    if not source_values:
                        continue
                    
                    # Calculate foreign key score
                    fk_score = self._calculate_foreign_key_score(
                        source_values, target_values, col, best_target_key
                    )
                    
                    if fk_score['is_foreign_key']:
                        foreign_keys.append({
                            'source_table': source_table,
                            'source_column': col,
                            'target_table': target_table,
                            'target_column': best_target_key,
                            'score': fk_score['score'],
                            'referential_integrity': fk_score['referential_integrity'],
                            'value_overlap': fk_score['value_overlap']
                        })
        
        # Sort by score
        foreign_keys.sort(key=lambda x: x['score'], reverse=True)
        
        return foreign_keys
    
    def _calculate_foreign_key_score(self, source_values, target_values, source_col, target_col):
        """Calculate foreign key likelihood score"""
        if not source_values or not target_values:
            return {'is_foreign_key': False, 'score': 0}
        
        # Referential integrity: what percentage of source values exist in target
        valid_references = len(source_values & target_values)
        referential_integrity = valid_references / len(source_values)
        
        # Value overlap
        value_overlap = len(source_values & target_values) / len(source_values | target_values)
        
        # Base score from referential integrity
        score = referential_integrity * 60
        
        # Bonus for high overlap
        score += value_overlap * 20
        
        # Column name similarity bonus
        name_similarity = self._calculate_name_similarity(source_col, target_col)
        score += name_similarity * 20
        
        # Penalty if source has values not in target (bad referential integrity)
        if referential_integrity < 0.9:
            score -= (1 - referential_integrity) * 30
        
        is_foreign_key = score > 60 and referential_integrity > 0.7
        
        return {
            'is_foreign_key': is_foreign_key,
            'score': score,
            'referential_integrity': referential_integrity,
            'value_overlap': value_overlap
        }
    
    def _suggest_joins(self, dataframes_dict):
        """Suggest appropriate join types for table relationships"""
        join_suggestions = []
        
        relationships = self._detect_relationships(dataframes_dict)
        
        for rel in relationships:
            if rel['strength'] < 0.5:
                continue
            
            table1 = rel['table1']
            table2 = rel['table2']
            column = rel['column']
            
            # Determine join type based on relationship characteristics
            join_type = self._determine_join_type(rel)
            
            join_suggestion = {
                'table1': table1,
                'table2': table2,
                'join_column': column,
                'recommended_join_type': join_type['type'],
                'confidence': join_type['confidence'],
                'reasoning': join_type['reasoning'],
                'relationship_strength': rel['strength'],
                'expected_result_size': join_type['expected_result_size']
            }
            
            join_suggestions.append(join_suggestion)
        
        return join_suggestions
    
    def _determine_join_type(self, relationship):
        """Determine the best join type for a relationship"""
        rel_type = relationship['relationship_type']
        containment_1_in_2 = relationship['containment_1_in_2']
        containment_2_in_1 = relationship['containment_2_in_1']
        strength = relationship['strength']
        
        if rel_type == 'strong_overlap' and strength > 0.9:
            return {
                'type': 'INNER JOIN',
                'confidence': 0.9,
                'reasoning': 'High overlap suggests most records will match',
                'expected_result_size': 'similar_to_smaller_table'
            }
        
        elif rel_type == 'table1_subset_of_table2':
            return {
                'type': 'LEFT JOIN (table2 LEFT JOIN table1)',
                'confidence': 0.8,
                'reasoning': 'Table1 values are subset of table2, left join preserves all table2 records',
                'expected_result_size': 'same_as_table2'
            }
        
        elif rel_type == 'table2_subset_of_table1':
            return {
                'type': 'LEFT JOIN (table1 LEFT JOIN table2)',
                'confidence': 0.8,
                'reasoning': 'Table2 values are subset of table1, left join preserves all table1 records',
                'expected_result_size': 'same_as_table1'
            }
        
        elif rel_type == 'partial_overlap':
            if containment_1_in_2 > 0.7:
                return {
                    'type': 'LEFT JOIN (table2 LEFT JOIN table1)',
                    'confidence': 0.6,
                    'reasoning': 'Most table1 values exist in table2, left join recommended',
                    'expected_result_size': 'between_table_sizes'
                }
            elif containment_2_in_1 > 0.7:
                return {
                    'type': 'LEFT JOIN (table1 LEFT JOIN table2)',
                    'confidence': 0.6,
                    'reasoning': 'Most table2 values exist in table1, left join recommended',
                    'expected_result_size': 'between_table_sizes'
                }
            else:
                return {
                    'type': 'FULL OUTER JOIN',
                    'confidence': 0.5,
                    'reasoning': 'Partial overlap with significant unique values in both tables',
                    'expected_result_size': 'larger_than_both_tables'
                }
        
        else:
            return {
                'type': 'CROSS JOIN (with caution)',
                'confidence': 0.2,
                'reasoning': 'Weak relationship detected, consider if join is necessary',
                'expected_result_size': 'very_large'
            }
