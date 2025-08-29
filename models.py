from app import db
from datetime import datetime
import json

class AnalysisSession(db.Model):
    """Model to store analysis sessions and results"""
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    file_count = db.Column(db.Integer, default=0)
    analysis_results = db.Column(db.Text)  # JSON string of results
    
    def set_results(self, results_dict):
        """Store analysis results as JSON"""
        self.analysis_results = json.dumps(results_dict)
    
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
