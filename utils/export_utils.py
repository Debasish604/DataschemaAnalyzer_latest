import json
import csv
import pandas as pd
import os
from datetime import datetime
import logging

class ExportUtils:
    """Utility class for exporting analysis results in various formats"""
    
    def __init__(self):
        self.export_dir = "exports"
        os.makedirs(self.export_dir, exist_ok=True)
    
    def export(self, results, format_type, session_name):
        """Export analysis results in specified format"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{session_name}_{timestamp}"
        
        if format_type.lower() == 'json':
            return self._export_json(results, filename)
        elif format_type.lower() == 'csv':
            return self._export_csv(results, filename)
        elif format_type.lower() == 'html':
            return self._export_html(results, filename)
        elif format_type.lower() == 'txt':
            return self._export_text(results, filename)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")
    
    def _export_json(self, results, filename):
        """Export results as JSON"""
        filepath = os.path.join(self.export_dir, f"{filename}.json")
        
        # Convert numpy types to Python types for JSON serialization
        serializable_results = self._make_json_serializable(results)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def _export_csv(self, results, filename):
        """Export results as CSV (flattened structure)"""
        filepath = os.path.join(self.export_dir, f"{filename}.csv")
        
        # Flatten the results into tabular format
        flattened_data = self._flatten_results_for_csv(results)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if flattened_data:
                writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                writer.writeheader()
                writer.writerows(flattened_data)
        
        return filepath
    
    def _export_html(self, results, filename):
        """Export results as HTML report"""
        filepath = os.path.join(self.export_dir, f"{filename}.html")
        
        html_content = self._generate_html_report(results, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return filepath
    
    def _export_text(self, results, filename):
        """Export results as plain text report"""
        filepath = os.path.join(self.export_dir, f"{filename}.txt")
        
        text_content = self._generate_text_report(results, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(text_content)
        
        return filepath
    
    def _make_json_serializable(self, obj):
        """Convert numpy types and other non-serializable objects to JSON-compatible types"""
        import numpy as np
        import pandas as pd
        
        if isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        elif hasattr(obj, 'isoformat'):  # datetime objects
            return obj.isoformat()
        else:
            return obj
    
    def _flatten_results_for_csv(self, results):
        """Flatten nested results structure for CSV export"""
        flattened = []
        
        # Extract data type analysis
        if 'data_types' in results:
            for filename, file_results in results['data_types'].items():
                for column, analysis in file_results.items():
                    row = {
                        'file_name': filename,
                        'analysis_type': 'data_type',
                        'column_name': column,
                        'inferred_type': analysis.get('inferred_type', ''),
                        'confidence': analysis.get('confidence', 0),
                        'unique_count': analysis.get('unique_count', 0),
                        'null_count': analysis.get('null_count', 0),
                        'sample_values': ', '.join(str(v) for v in analysis.get('sample_values', [])[:3])
                    }
                    flattened.append(row)
        
        # Extract pattern analysis
        if 'patterns' in results:
            for filename, pattern_data in results['patterns'].items():
                if 'outliers' in pattern_data:
                    for column, outlier_info in pattern_data['outliers'].items():
                        for method, details in outlier_info.items():
                            row = {
                                'file_name': filename,
                                'analysis_type': 'outlier',
                                'column_name': column,
                                'method': method,
                                'outlier_count': details.get('count', 0),
                                'outlier_percentage': details.get('percentage', 0),
                                'details': str(details)
                            }
                            flattened.append(row)
        
        # Extract relationships
        if 'relationships' in results and 'relationships' in results['relationships']:
            for rel in results['relationships']['relationships']:
                row = {
                    'analysis_type': 'relationship',
                    'table1': rel.get('table1', ''),
                    'table2': rel.get('table2', ''),
                    'column': rel.get('column', ''),
                    'relationship_type': rel.get('relationship_type', ''),
                    'strength': rel.get('strength', 0),
                    'values_in_common': rel.get('values_in_common', 0)
                }
                flattened.append(row)
        
        return flattened
    
    def _generate_html_report(self, results, filename):
        """Generate HTML report"""
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Analysis Report - {filename}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .section {{ margin-bottom: 30px; }}
        .subsection {{ margin-left: 20px; margin-bottom: 15px; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .metric {{ background-color: #e7f3ff; padding: 10px; border-radius: 3px; margin: 5px 0; }}
        .warning {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; }}
        .success {{ background-color: #d4edda; border-left: 4px solid #28a745; padding: 10px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Analysis Report</h1>
        <p><strong>Session:</strong> {filename}</p>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
"""
        
        # Add summary section
        if 'summary' in results:
            summary = results['summary']
            html += f"""
    <div class="section">
        <h2>Summary</h2>
        <div class="metric">Total Files: {summary.get('total_files', 0)}</div>
        <div class="metric">Total Rows: {summary.get('total_rows', 0)}</div>
        <div class="metric">Total Columns: {summary.get('total_columns', 0)}</div>
        <div class="metric">Data Quality Score: {summary.get('data_quality_score', 0):.1f}%</div>
    </div>
"""
        
        # Add data types section
        if 'data_types' in results:
            html += """
    <div class="section">
        <h2>Data Type Analysis</h2>
"""
            for filename, file_results in results['data_types'].items():
                html += f"""
        <div class="subsection">
            <h3>File: {filename}</h3>
            <table>
                <tr>
                    <th>Column</th>
                    <th>Inferred Type</th>
                    <th>Confidence</th>
                    <th>Unique Values</th>
                    <th>Missing Values</th>
                </tr>
"""
                for column, analysis in file_results.items():
                    html += f"""
                <tr>
                    <td>{column}</td>
                    <td>{analysis.get('inferred_type', 'unknown')}</td>
                    <td>{analysis.get('confidence', 0):.2f}</td>
                    <td>{analysis.get('unique_count', 0)}</td>
                    <td>{analysis.get('null_count', 0)}</td>
                </tr>
"""
                html += """
            </table>
        </div>
"""
            html += """
    </div>
"""
        
        # Add relationships section
        if 'relationships' in results and 'relationships' in results['relationships']:
            html += """
    <div class="section">
        <h2>Table Relationships</h2>
        <table>
            <tr>
                <th>Table 1</th>
                <th>Table 2</th>
                <th>Column</th>
                <th>Relationship Type</th>
                <th>Strength</th>
            </tr>
"""
            for rel in results['relationships']['relationships']:
                html += f"""
            <tr>
                <td>{rel.get('table1', '')}</td>
                <td>{rel.get('table2', '')}</td>
                <td>{rel.get('column', '')}</td>
                <td>{rel.get('relationship_type', '')}</td>
                <td>{rel.get('strength', 0):.2f}</td>
            </tr>
"""
            html += """
        </table>
    </div>
"""
        
        html += """
</body>
</html>
"""
        
        return html
    
    def _generate_text_report(self, results, filename):
        """Generate plain text report"""
        report = f"""
DATA ANALYSIS REPORT
{'=' * 50}

Session: {filename}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
        
        # Add summary
        if 'summary' in results:
            summary = results['summary']
            report += f"""
SUMMARY
{'-' * 20}
Total Files: {summary.get('total_files', 0)}
Total Rows: {summary.get('total_rows', 0)}
Total Columns: {summary.get('total_columns', 0)}
Data Quality Score: {summary.get('data_quality_score', 0):.1f}%

"""
        
        # Add data types
        if 'data_types' in results:
            report += """
DATA TYPE ANALYSIS
{'-' * 30}
"""
            for filename, file_results in results['data_types'].items():
                report += f"\nFile: {filename}\n"
                report += f"{'Column':<20} {'Type':<20} {'Confidence':<12} {'Unique':<8} {'Missing':<8}\n"
                report += f"{'-' * 70}\n"
                
                for column, analysis in file_results.items():
                    report += f"{column:<20} {analysis.get('inferred_type', 'unknown'):<20} "
                    report += f"{analysis.get('confidence', 0):<12.2f} {analysis.get('unique_count', 0):<8} "
                    report += f"{analysis.get('null_count', 0):<8}\n"
                
                report += "\n"
        
        # Add relationships
        if 'relationships' in results and 'relationships' in results['relationships']:
            report += """
TABLE RELATIONSHIPS
{'-' * 30}
"""
            for rel in results['relationships']['relationships']:
                report += f"Tables: {rel.get('table1', '')} ↔ {rel.get('table2', '')}\n"
                report += f"Column: {rel.get('column', '')}\n"
                report += f"Type: {rel.get('relationship_type', '')}\n"
                report += f"Strength: {rel.get('strength', 0):.2f}\n"
                report += f"{'-' * 40}\n"
        
        return report
