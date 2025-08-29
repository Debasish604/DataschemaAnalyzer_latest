import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

class DataInsights:
    """Utility class for generating intelligent insights about data"""
    
    @staticmethod
    def analyze_date_column(series, column_name):
        """Analyze a date column and provide insights about what type of dates it contains"""
        insights = {
            'column_name': column_name,
            'date_type': 'unknown',
            'patterns': [],
            'insights': [],
            'recommendations': []
        }
        
        # Convert to datetime if not already
        try:
            date_series = pd.to_datetime(series, errors='coerce')
            valid_dates = date_series.dropna()
            
            if len(valid_dates) == 0:
                insights['insights'].append("No valid dates found in column")
                return insights
            
        except Exception:
            insights['insights'].append("Could not parse dates in column")
            return insights
        
        # Calculate date range
        min_date = valid_dates.min()
        max_date = valid_dates.max()
        date_range = max_date - min_date
        
        insights['date_range'] = {
            'min': min_date.strftime('%Y-%m-%d'),
            'max': max_date.strftime('%Y-%m-%d'),
            'span_days': date_range.days
        }
        
        # Analyze date patterns
        current_date = datetime.now()
        current_year = current_date.year
        
        # Check for birth dates
        if all(1900 <= date.year <= current_year - 5 for date in valid_dates):
            insights['date_type'] = 'birth_dates'
            insights['insights'].append("Appears to contain birth dates (years 1900-2019)")
            
            # Age analysis
            ages = [(current_date - date).days // 365 for date in valid_dates]
            avg_age = np.mean(ages)
            insights['insights'].append(f"Average age based on birth dates: {avg_age:.1f} years")
        
        # Check for recent dates (business activity)
        elif all(date.year >= current_year - 5 for date in valid_dates):
            insights['date_type'] = 'recent_activity'
            insights['insights'].append("Contains recent dates, likely business activity or transactions")
            
            # Check for business hours pattern
            business_hours = [date for date in valid_dates if 9 <= date.hour <= 17]
            if len(business_hours) / len(valid_dates) > 0.8:
                insights['patterns'].append("Most dates occur during business hours (9 AM - 5 PM)")
        
        # Check for historical dates
        elif all(date.year < current_year - 10 for date in valid_dates):
            insights['date_type'] = 'historical_dates'
            insights['insights'].append("Contains historical dates")
        
        # Check for future dates
        elif any(date > current_date for date in valid_dates):
            future_dates = [date for date in valid_dates if date > current_date]
            insights['date_type'] = 'includes_future'
            insights['insights'].append(f"Contains {len(future_dates)} future dates - possibly scheduled events or deadlines")
        
        # Analyze weekday patterns
        weekdays = [date.weekday() for date in valid_dates]
        weekday_counts = pd.Series(weekdays).value_counts()
        
        # Check if mostly weekdays (0-4) vs weekends (5-6)
        weekday_ratio = sum(weekday_counts[0:5]) / len(weekdays) if len(weekdays) > 0 else 0
        
        if weekday_ratio > 0.8:
            insights['patterns'].append("Most dates fall on weekdays - likely business-related")
        elif weekday_ratio < 0.3:
            insights['patterns'].append("Most dates fall on weekends - likely personal/leisure events")
        
        # Analyze monthly patterns
        months = [date.month for date in valid_dates]
        month_counts = pd.Series(months).value_counts()
        
        # Check for seasonal patterns
        if month_counts.max() / month_counts.min() > 3:
            peak_month = month_counts.idxmax()
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            insights['patterns'].append(f"Strong seasonal pattern - peak in {month_names[peak_month-1]}")
        
        # Generate recommendations
        if insights['date_type'] == 'birth_dates':
            insights['recommendations'].append("Consider calculating age as a derived column")
            insights['recommendations'].append("Check for unrealistic birth dates (too old/young)")
        
        elif insights['date_type'] == 'recent_activity':
            insights['recommendations'].append("Good for time-series analysis")
            insights['recommendations'].append("Consider grouping by day/week/month for trends")
        
        elif insights['date_type'] == 'includes_future':
            insights['recommendations'].append("Validate future dates are intentional")
            insights['recommendations'].append("Consider separating past and future dates for analysis")
        
        return insights
    
    @staticmethod
    def analyze_numeric_column(series, column_name):
        """Analyze numeric column and provide insights"""
        insights = {
            'column_name': column_name,
            'numeric_type': 'unknown',
            'patterns': [],
            'insights': [],
            'recommendations': []
        }
        
        numeric_data = pd.to_numeric(series, errors='coerce').dropna()
        
        if len(numeric_data) == 0:
            insights['insights'].append("No valid numeric data found")
            return insights
        
        # Basic statistics
        stats = {
            'min': numeric_data.min(),
            'max': numeric_data.max(),
            'mean': numeric_data.mean(),
            'median': numeric_data.median(),
            'std': numeric_data.std()
        }
        
        insights['statistics'] = stats
        
        # Determine numeric type based on patterns
        
        # Check for IDs (integers, unique, possibly sequential)
        if all(x == int(x) for x in numeric_data) and series.nunique() == len(series):
            if all(x > 0 for x in numeric_data):
                insights['numeric_type'] = 'identifier'
                insights['insights'].append("Appears to be an ID column (unique positive integers)")
                
                # Check if sequential
                sorted_values = sorted(numeric_data)
                if all(sorted_values[i+1] - sorted_values[i] == 1 for i in range(len(sorted_values)-1)):
                    insights['patterns'].append("Sequential ID (auto-increment)")
        
        # Check for monetary values
        elif all(x >= 0 for x in numeric_data) and stats['max'] > 10:
            # Look for decimal places typical of currency
            decimal_places = [len(str(x).split('.')[-1]) if '.' in str(x) else 0 for x in numeric_data]
            if max(decimal_places) <= 2 and sum(decimal_places) > 0:
                insights['numeric_type'] = 'monetary'
                insights['insights'].append("Appears to be monetary values (positive, max 2 decimal places)")
                insights['recommendations'].append("Consider currency formatting for display")
        
        # Check for percentages
        elif all(0 <= x <= 100 for x in numeric_data):
            insights['numeric_type'] = 'percentage'
            insights['insights'].append("Values in 0-100 range, likely percentages")
            insights['recommendations'].append("Confirm if values are percentages and format accordingly")
        
        # Check for ratings/scores
        elif all(1 <= x <= 5 for x in numeric_data) or all(1 <= x <= 10 for x in numeric_data):
            max_val = stats['max']
            insights['numeric_type'] = 'rating_score'
            insights['insights'].append(f"Values in 1-{int(max_val)} range, likely rating or score")
            
            # Check distribution
            value_counts = numeric_data.value_counts()
            if len(value_counts) <= 10:
                insights['patterns'].append("Discrete rating scale")
        
        # Check for counts/quantities
        elif all(x >= 0 and x == int(x) for x in numeric_data):
            insights['numeric_type'] = 'count_quantity'
            insights['insights'].append("Non-negative integers, likely counts or quantities")
            
            if stats['max'] < 1000:
                insights['patterns'].append("Small counts (< 1000)")
            else:
                insights['patterns'].append("Large counts (>= 1000)")
        
        # Check for measurements
        else:
            insights['numeric_type'] = 'measurement'
            insights['insights'].append("Continuous numeric values, likely measurements")
            
            # Check for outliers
            q1 = numeric_data.quantile(0.25)
            q3 = numeric_data.quantile(0.75)
            iqr = q3 - q1
            outliers = numeric_data[(numeric_data < q1 - 1.5*iqr) | (numeric_data > q3 + 1.5*iqr)]
            
            if len(outliers) > 0:
                insights['patterns'].append(f"Contains {len(outliers)} potential outliers")
        
        # Distribution analysis
        if stats['std'] > 0:
            cv = stats['std'] / abs(stats['mean']) if stats['mean'] != 0 else float('inf')
            
            if cv < 0.1:
                insights['patterns'].append("Low variability (coefficient of variation < 0.1)")
            elif cv > 1.0:
                insights['patterns'].append("High variability (coefficient of variation > 1.0)")
        
        # Skewness check
        try:
            from scipy import stats as scipy_stats
            skewness = scipy_stats.skew(numeric_data)
            
            if abs(skewness) < 0.5:
                insights['patterns'].append("Approximately symmetric distribution")
            elif skewness > 0.5:
                insights['patterns'].append("Right-skewed distribution (long tail on right)")
            elif skewness < -0.5:
                insights['patterns'].append("Left-skewed distribution (long tail on left)")
        except ImportError:
            pass
        
        return insights
    
    @staticmethod
    def analyze_text_column(series, column_name):
        """Analyze text column and provide insights"""
        insights = {
            'column_name': column_name,
            'text_type': 'unknown',
            'patterns': [],
            'insights': [],
            'recommendations': []
        }
        
        text_data = series.dropna().astype(str)
        
        if len(text_data) == 0:
            insights['insights'].append("No valid text data found")
            return insights
        
        # Basic text statistics
        lengths = text_data.str.len()
        
        stats = {
            'avg_length': lengths.mean(),
            'min_length': lengths.min(),
            'max_length': lengths.max(),
            'unique_values': series.nunique(),
            'total_values': len(text_data)
        }
        
        insights['statistics'] = stats
        
        # Determine text type
        col_lower = column_name.lower()
        
        # Check for names
        if any(indicator in col_lower for indicator in ['name', 'first', 'last', 'fname', 'lname']):
            insights['text_type'] = 'personal_name'
            insights['insights'].append("Appears to contain personal names")
            
            # Check name patterns
            has_spaces = text_data.str.contains(' ').sum()
            if has_spaces / len(text_data) > 0.8:
                insights['patterns'].append("Most entries contain spaces (full names)")
            
            # Check capitalization
            properly_capitalized = text_data.str.istitle().sum()
            if properly_capitalized / len(text_data) > 0.8:
                insights['patterns'].append("Most names are properly capitalized")
            else:
                insights['recommendations'].append("Consider standardizing name capitalization")
        
        # Check for emails
        elif '@' in text_data.str.cat() and '.' in text_data.str.cat():
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            email_matches = text_data.str.match(email_pattern).sum()
            
            if email_matches / len(text_data) > 0.8:
                insights['text_type'] = 'email'
                insights['insights'].append("Contains email addresses")
                insights['recommendations'].append("Validate email format consistency")
        
        # Check for URLs
        elif text_data.str.contains('http').sum() > 0 or text_data.str.contains('www').sum() > 0:
            insights['text_type'] = 'url'
            insights['insights'].append("Contains URLs")
        
        # Check for addresses
        elif any(indicator in col_lower for indicator in ['address', 'street', 'city', 'location']):
            insights['text_type'] = 'address'
            insights['insights'].append("Appears to contain address information")
            
            # Check for common address patterns
            has_numbers = text_data.str.contains(r'\d').sum()
            if has_numbers / len(text_data) > 0.5:
                insights['patterns'].append("Many entries contain numbers (street addresses)")
        
        # Check for categorical data
        elif stats['unique_values'] / stats['total_values'] < 0.1:
            insights['text_type'] = 'categorical'
            insights['insights'].append("Low uniqueness ratio suggests categorical data")
            
            # Show most common values
            value_counts = text_data.value_counts().head(5)
            insights['patterns'].append(f"Top categories: {', '.join(value_counts.index.tolist())}")
        
        # Check for codes/IDs
        elif stats['avg_length'] < 10 and all(len(x.split()) == 1 for x in text_data.head(100)):
            insights['text_type'] = 'code_identifier'
            insights['insights'].append("Short, single-word entries suggest codes or identifiers")
            
            # Check for alphanumeric patterns
            alphanumeric = text_data.str.match(r'^[A-Za-z0-9]+$').sum()
            if alphanumeric / len(text_data) > 0.8:
                insights['patterns'].append("Alphanumeric codes")
        
        # Check for descriptions/comments
        elif stats['avg_length'] > 50:
            insights['text_type'] = 'long_text'
            insights['insights'].append("Long text entries, likely descriptions or comments")
            
            # Check for sentence patterns
            has_punctuation = text_data.str.contains(r'[.!?]').sum()
            if has_punctuation / len(text_data) > 0.5:
                insights['patterns'].append("Contains sentence punctuation")
        
        # Data quality checks
        
        # Check for inconsistent formatting
        case_consistency = (text_data.str.islower().sum() + text_data.str.isupper().sum() + 
                           text_data.str.istitle().sum())
        
        if case_consistency / len(text_data) < 0.8:
            insights['recommendations'].append("Consider standardizing text case")
        
        # Check for leading/trailing whitespace
        trimmed = text_data.str.strip()
        needs_trimming = (trimmed != text_data).sum()
        
        if needs_trimming > 0:
            insights['recommendations'].append(f"{needs_trimming} entries have leading/trailing whitespace")
        
        # Check for empty strings
        empty_strings = (text_data == '').sum()
        if empty_strings > 0:
            insights['recommendations'].append(f"{empty_strings} entries are empty strings")
        
        return insights
    
    @staticmethod
    def generate_column_summary(df):
        """Generate a comprehensive summary of all columns"""
        summary = {
            'total_columns': len(df.columns),
            'total_rows': len(df),
            'columns_by_type': {},
            'data_quality_score': 0,
            'recommendations': []
        }
        
        # Analyze each column
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                col_type = 'numeric'
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_type = 'datetime'
            else:
                col_type = 'text'
            
            if col_type not in summary['columns_by_type']:
                summary['columns_by_type'][col_type] = []
            
            summary['columns_by_type'][col_type].append(col)
        
        # Calculate data quality score
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        completeness = (total_cells - missing_cells) / total_cells
        
        # Factor in duplicates
        duplicate_penalty = df.duplicated().sum() / len(df) * 0.1
        
        summary['data_quality_score'] = max(0, (completeness - duplicate_penalty) * 100)
        
        # General recommendations
        if summary['data_quality_score'] < 80:
            summary['recommendations'].append("Data quality could be improved - address missing values and duplicates")
        
        if len(summary['columns_by_type'].get('text', [])) > len(summary['columns_by_type'].get('numeric', [])):
            summary['recommendations'].append("Many text columns detected - consider data type optimization")
        
        return summary
