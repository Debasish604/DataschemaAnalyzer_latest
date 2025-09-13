import json
import numpy as np
import pandas as pd
from analyzers.data_type_analyzer import DataTypeAnalyzer
from analyzers.pattern_analyzer import PatternAnalyzer
from analyzers.relationship_analyzer import RelationshipAnalyzer
from routes import generate_file_insights, generate_analysis_summary
from parsers.file_parser import FileParserFactory


class AnalysisExporter:
    def __init__(self):
        self.data_type_analyzer = DataTypeAnalyzer()
        self.pattern_analyzer = PatternAnalyzer()
        self.relationship_analyzer = RelationshipAnalyzer()

    def run_full_analysis(self, parsed_data):
        """
        Run full analysis pipeline and return results as dictionary
        """
        results = {
            "data_types": {},
            "patterns": {},
            "insights": {},
            "relationships": {},
            "summary": {}
        }

        all_dataframes = {}

        # Per-file analysis
        for filename, file_info in parsed_data.items():
            data = file_info["data"]
            if data is not None and not data.empty:
                all_dataframes[filename] = data

                # Data Type Analysis
                results["data_types"][filename] = self.data_type_analyzer.analyze(data)

                # Pattern Analysis
                results["patterns"][filename] = self.pattern_analyzer.analyze(data)

                # File Insights
                results["insights"][filename] = generate_file_insights(data, filename)

        # Relationship Analysis (only if more than one file)
        if len(all_dataframes) > 1:
            results["relationships"] = self.relationship_analyzer.analyze_relationships(all_dataframes)

        # Summary
        results["summary"] = generate_analysis_summary(results, all_dataframes)

        return results

    def export_to_json(self, parsed_data, output_file="analysis_results.json"):
        """
        Run analysis and save results to a JSON file
        """
        results = self.run_full_analysis(parsed_data)

        def default_converter(o):
            if isinstance(o, (np.integer,)):
                return int(o)
            elif isinstance(o, (np.floating,)):
                return float(o)
            elif isinstance(o, (np.bool_, bool)):
                return bool(o)
            elif isinstance(o, (pd.Timestamp,)):
                return o.isoformat()
            return str(o)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False, default=default_converter)

        return output_file


if __name__ == "__main__":
    # Example: load your files (simulate Flask upload)
    file_parser = FileParserFactory()
    parsed_data = {}

    # Replace with your actual file paths
    files = ["uploads/9_Customers.xlsx", "uploads/9_Orders.xlsx"]

    for file in files:
        ext = file.split(".")[-1]
        parser = file_parser.get_parser(ext)
        df = parser.parse(file)
        parsed_data[file] = {"data": df, "file_type": ext}

    # Run analysis and export
    exporter = AnalysisExporter()
    output = exporter.export_to_json(parsed_data, "analysis_output.json")
    print(f"✅ Analysis results saved to {output}")
