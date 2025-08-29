from app import db
from datetime import datetime
import json
import numpy as np
import pandas as pd

class AnalysisSession(db.Model):
    """Model to store analysis sessions and results"""
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    file_count = db.Column(db.Integer, default=0)
    analysis_results = db.Column(db.Text)  # JSON string of results
    
    def _make_json_serializable(self, obj):
        """Convert numpy types and other non-serializable objects to JSON-compatible types"""
        if isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, tuple):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        elif pd.isnull(obj):
            return None
        elif hasattr(obj, 'isoformat'):  # datetime objects
            return obj.isoformat()
        elif hasattr(obj, 'item'):  # numpy scalars
            return obj.item()
        else:
            return obj
    
    def set_results(self, results_dict):
        """Store analysis results as JSON"""
        serializable_results = self._make_json_serializable(results_dict)
        self.analysis_results = json.dumps(serializable_results)
    
    def get_results(self):
        """Retrieve analysis results as dictionary"""
        if self.analysis_results:
            return json.loads(self.analysis_results)
        return {}

class UploadedFile(db.Model):
    """Model to track uploaded files"""
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    file_type = db.Column(db.String(10), nullable=False)
    file_path = db.Column(db.String(500), nullable=False)
    session_id = db.Column(db.Integer, db.ForeignKey('analysis_session.id'), nullable=False)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    file_size = db.Column(db.Integer)
    
    session = db.relationship('AnalysisSession', backref=db.backref('files', lazy=True))
