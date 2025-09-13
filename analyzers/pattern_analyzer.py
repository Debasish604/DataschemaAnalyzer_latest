import pandas as pd
import numpy as np
import logging
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

class PatternAnalyzer:
    """Analyzer for detecting patterns, outliers, and data quality issues"""
    
    def analyze(self, df):
        """Perform comprehensive pattern analysis on DataFrame"""
        results = {
            'outliers': self._detect_outliers(df),
            'patterns': self._detect_patterns(df),
            'correlations': self._analyze_correlations(df),
            'data_quality': self._assess_data_quality(df),
            'distributions': self._analyze_distributions(df)
        }
        
        return results
    
    def _detect_outliers(self, df):
        """Detect outliers in numeric columns"""
        outliers = {}
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            col_outliers = {}
            data = df[col].dropna()
            
            if len(data) < 4:  # Need minimum data for outlier detection
                continue
            
            # Z-score method
            z_scores = np.abs(stats.zscore(data))
            z_outliers = data[z_scores > 3]
            col_outliers['z_score'] = {
                'count': len(z_outliers),
                'percentage': len(z_outliers) / len(data) * 100,
                'values': z_outliers.tolist()[:10]  # Show first 10
            }
            
            # IQR method
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            iqr_outliers = data[(data < lower_bound) | (data > upper_bound)]
            col_outliers['iqr'] = {
                'count': len(iqr_outliers),
                'percentage': len(iqr_outliers) / len(data) * 100,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'values': iqr_outliers.tolist()[:10]
            }
            
            # Modified Z-score (using median)
            median = data.median()
            mad = np.median(np.abs(data - median))
            modified_z_scores = 0.6745 * (data - median) / mad if mad != 0 else np.zeros_like(data)
            mod_z_outliers = data[np.abs(modified_z_scores) > 3.5]
            
            col_outliers['modified_z'] = {
                'count': len(mod_z_outliers),
                'percentage': len(mod_z_outliers) / len(data) * 100,
                'values': mod_z_outliers.tolist()[:10]
            }
            
            outliers[col] = col_outliers

            # [Debug] output for detected outliers
            print(f"[Outliers Detected] Column: {col} | "
                  f"Z-Score Outliers: {col_outliers['z_score']['count']} "
                  f"({col_outliers['z_score']['percentage']:.2f}%), "
                  f"IQR Outliers: {col_outliers['iqr']['count']} "
                  f"({col_outliers['iqr']['percentage']:.2f}%), "
                  f"Modified Z Outliers: {col_outliers['modified_z']['count']} "
                  f"({col_outliers['modified_z']['percentage']:.2f}%)")
            
        return outliers
    
    def _detect_patterns(self, df):
        """Detect various data patterns"""
        patterns = {}
        
        # Sequence patterns in numeric columns
        patterns['sequences'] = self._detect_sequences(df)
        
        # Repeating patterns in text columns
        patterns['repeating_values'] = self._detect_repeating_patterns(df)
        
        # Missing data patterns
        patterns['missing_data'] = self._analyze_missing_patterns(df)
        
        # Clustering patterns for numeric data
        patterns['clusters'] = self._detect_clusters(df)
        
        return patterns
    
    def _detect_sequences(self, df):
        """Detect sequential patterns in numeric columns"""
        sequences = {}
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            data = df[col].dropna()
            
            if len(data) < 3:
                continue
            
            # Check for arithmetic sequences
            diffs = data.diff().dropna()
            
            # If differences are mostly constant, it's an arithmetic sequence
            if len(diffs.unique()) <= 3 and len(diffs) > 2:
                common_diff = diffs.mode().iloc[0] if len(diffs.mode()) > 0 else diffs.mean()
                sequences[col] = {
                    'type': 'arithmetic',
                    'common_difference': common_diff,
                    'consistency': (diffs == common_diff).mean()
                }
            
            # Check for geometric sequences
            elif all(data > 0):  # Geometric sequences need positive values
                ratios = (data / data.shift(1)).dropna()
                if len(ratios.unique()) <= 3 and len(ratios) > 2:
                    common_ratio = ratios.mode().iloc[0] if len(ratios.mode()) > 0 else ratios.mean()
                    sequences[col] = {
                        'type': 'geometric',
                        'common_ratio': common_ratio,
                        'consistency': (ratios == common_ratio).mean()
                    }
        
        return sequences
    
    def _detect_repeating_patterns(self, df):
        """Detect repeating patterns in text/categorical columns"""
        patterns = {}
        
        text_cols = df.select_dtypes(include=['object']).columns
        
        for col in text_cols:
            data = df[col].dropna()
            
            if len(data) == 0:
                continue
            
            # Analyze value frequency
            value_counts = data.value_counts()
            
            # Pattern characteristics
            total_values = len(data)
            unique_values = len(value_counts)
            
            patterns[col] = {
                'total_values': total_values,
                'unique_values': unique_values,
                'uniqueness_ratio': unique_values / total_values,
                'most_common': value_counts.head(5).to_dict(),
                'repetition_score': (value_counts ** 2).sum() / total_values ** 2  # Herfindahl index
            }
            
            # Check for cyclical patterns (if data has an inherent order)
            if len(data) > 10:
                # Simple autocorrelation check for categorical data
                # Convert to numeric codes for analysis
                codes = pd.Categorical(data).codes
                if len(np.unique(codes)) > 1:
                    autocorr = np.corrcoef(codes[:-1], codes[1:])[0, 1]
                    patterns[col]['autocorrelation'] = autocorr
        
        return patterns
    
    def _analyze_missing_patterns(self, df):
        """Analyze patterns in missing data"""
        missing_patterns = {}
        
        # Overall missing data statistics
        missing_counts = df.isnull().sum()
        total_rows = len(df)
        
        missing_patterns['by_column'] = {
            col: {
                'count': int(count),
                'percentage': count / total_rows * 100
            }
            for col, count in missing_counts.items() if count > 0
        }
        
        # Missing data correlation (columns that tend to be missing together)
        if missing_counts.sum() > 0:
            missing_matrix = df.isnull()
            missing_corr = missing_matrix.corr()
            
            # Find pairs of columns with high missing data correlation
            high_corr_pairs = []
            for i in range(len(missing_corr.columns)):
                for j in range(i + 1, len(missing_corr.columns)):
                    corr_val = missing_corr.iloc[i, j]
                    if abs(corr_val) > 0.5 and not pd.isna(corr_val):
                        high_corr_pairs.append({
                            'column1': missing_corr.columns[i],
                            'column2': missing_corr.columns[j],
                            'correlation': corr_val
                        })
            
            missing_patterns['correlated_missing'] = high_corr_pairs
        
        # Rows with multiple missing values
        missing_per_row = df.isnull().sum(axis=1)
        missing_patterns['rows_with_multiple_missing'] = {
            'count': (missing_per_row > 1).sum(),
            'percentage': (missing_per_row > 1).mean() * 100
        }
        
        return missing_patterns
    
    def _detect_clusters(self, df):
        """Detect clusters in numeric data using DBSCAN"""
        clusters = {}
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return clusters
        
        # Use pairs of numeric columns for clustering
        for i, col1 in enumerate(numeric_cols):
            for col2 in numeric_cols[i + 1:]:
                data = df[[col1, col2]].dropna()
                
                if len(data) < 10:  # Need minimum data for clustering
                    continue
                
                try:
                    # Standardize the data
                    scaler = StandardScaler()
                    scaled_data = scaler.fit_transform(data)
                    
                    # Apply DBSCAN
                    dbscan = DBSCAN(eps=0.5, min_samples=5)
                    cluster_labels = dbscan.fit_predict(scaled_data)
                    
                    n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
                    n_noise = list(cluster_labels).count(-1)
                    
                    if n_clusters > 1:
                        clusters[f"{col1}_vs_{col2}"] = {
                            'n_clusters': n_clusters,
                            'n_noise_points': n_noise,
                            'noise_percentage': n_noise / len(data) * 100,
                            'silhouette_score': self._calculate_silhouette_score(scaled_data, cluster_labels)
                        }
                
                except Exception as e:
                    logging.warning(f"Clustering failed for {col1} vs {col2}: {str(e)}")
                    continue
        
        return clusters
    
    def _calculate_silhouette_score(self, data, labels):
        """Calculate silhouette score for clustering"""
        try:
            from sklearn.metrics import silhouette_score
            # Only calculate if we have clusters (not just noise)
            if len(set(labels)) > 1 and not all(label == -1 for label in labels):
                return silhouette_score(data, labels)
        except:
            pass
        return None
    
    def _analyze_correlations(self, df):
        """Analyze correlations between numeric columns"""
        correlations = {}
        
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return correlations
        
        # Calculate correlation matrix
        corr_matrix = numeric_df.corr()
        
        # Find strong correlations
        strong_correlations = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                if abs(corr_val) > 0.7 and not pd.isna(corr_val):
                    strong_correlations.append({
                        'column1': corr_matrix.columns[i],
                        'column2': corr_matrix.columns[j],
                        'correlation': corr_val,
                        'strength': 'very_strong' if abs(corr_val) > 0.9 else 'strong'
                    })
        
        correlations['strong_correlations'] = strong_correlations
        correlations['correlation_matrix'] = corr_matrix.to_dict()
        
        return correlations
    
    def _assess_data_quality(self, df):
        """Assess overall data quality"""
        quality = {}
        
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        
        quality['completeness'] = {
            'score': (total_cells - missing_cells) / total_cells * 100,
            'missing_cells': missing_cells,
            'total_cells': total_cells
        }
        
        # [Debug] output for data quality summary
        print(f"[Data Quality] Completeness: {quality['completeness']['score']:.2f}% "
              f"({quality['completeness']['missing_cells']} missing out of {quality['completeness']['total_cells']})")
        
        # Consistency checks
        quality['consistency'] = {}
        
        # Check for duplicate rows
        duplicate_rows = df.duplicated().sum()
        quality['consistency']['duplicate_rows'] = {
            'count': duplicate_rows,
            'percentage': duplicate_rows / len(df) * 100
        }
        
        # [Debug] output for duplicate rows
        print(f"[Data Quality] Duplicate Rows: {duplicate_rows} "
              f"({quality['consistency']['duplicate_rows']['percentage']:.2f}%)")
        
        # Check for columns with single value (zero variance)
        zero_variance_cols = []
        for col in df.columns:
            if df[col].nunique() <= 1:
                zero_variance_cols.append(col)
        
        quality['consistency']['zero_variance_columns'] = zero_variance_cols
        
        # Data type consistency
        mixed_type_cols = []
        for col in df.select_dtypes(include=['object']).columns:
            sample_types = set()
            for val in df[col].dropna().head(100):  # Check first 100 non-null values
                if isinstance(val, (int, float)):
                    sample_types.add('numeric')
                elif isinstance(val, str):
                    sample_types.add('string')
                else:
                    sample_types.add('other')
            
            if len(sample_types) > 1:
                mixed_type_cols.append(col)
        
        quality['consistency']['mixed_type_columns'] = mixed_type_cols
        
        return quality
    
    def _analyze_distributions(self, df):
        """Analyze statistical distributions of numeric columns"""
        distributions = {}
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            data = df[col].dropna()
            
            if len(data) < 10:
                continue
            
            dist_info = {
                'mean': data.mean(),
                'median': data.median(),
                'std': data.std(),
                'skewness': stats.skew(data),
                'kurtosis': stats.kurtosis(data),
                'min': data.min(),
                'max': data.max(),
                'quartiles': {
                    'q1': data.quantile(0.25),
                    'q2': data.quantile(0.5),
                    'q3': data.quantile(0.75)
                }
            }
            
            # Test for normality
            if len(data) >= 8:  # Minimum sample size for Shapiro-Wilk
                try:
                    _, p_value = stats.shapiro(data[:5000])  # Limit sample size for performance
                    dist_info['normality_test'] = {
                        'p_value': p_value,
                        'is_normal': p_value > 0.05
                    }
                except:
                    dist_info['normality_test'] = {'error': 'Test failed'}
            
            distributions[col] = dist_info
        
        return distributions
