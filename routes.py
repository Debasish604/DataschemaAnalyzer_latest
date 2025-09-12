import os
import logging
from flask import render_template, request, redirect, url_for, flash, jsonify, send_file, current_app
from werkzeug.utils import secure_filename
from models import db, AnalysisSession, UploadedFile
from parsers.file_parser import FileParserFactory
from analyzers.data_type_analyzer import DataTypeAnalyzer
from analyzers.pattern_analyzer import PatternAnalyzer
from analyzers.relationship_analyzer import RelationshipAnalyzer
from utils.export_utils import ExportUtils

ALLOWED_EXTENSIONS = {'csv', 'sql', 'xls', 'xlsx', 'xml'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def register_routes(app):
    """Register all routes with the Flask app"""

    # =======================
    # UI ROUTES (Templates Only)
    # =======================
    @app.route('/')
    def index():
        """Home page"""
        return render_template('index.html')

    @app.route('/upload')
    def upload_page():
        """File upload page"""
        return render_template('upload.html')

    @app.route('/analyze/<int:session_id>')
    def analysis_page(session_id):
        """Analysis results page"""
        return render_template('analysis.html', session_id=session_id)

    @app.route('/session/<int:session_id>')
    def session_page(session_id):
        """View a specific analysis session page"""
        return render_template('analysis.html', session_id=session_id)

    # =======================
    # API ROUTES (JSON Only)
    # =======================
    @app.route('/api/sessions')
    def api_get_sessions():
        """Get recent analysis sessions"""
        recent_sessions = AnalysisSession.query.order_by(AnalysisSession.created_at.desc()).limit(5).all()
        return jsonify({
            'status': 'success',
            'sessions': [{
                'id': session.id,
                'session_name': session.session_name,
                'file_count': session.file_count,
                'created_at': session.created_at.isoformat()
            } for session in recent_sessions]
        })

    @app.route('/api/upload', methods=['POST'])
    def api_upload_files():
        """API endpoint for file upload"""
        try:
            session_name = request.form.get('session_name', 'Unnamed Session')
            files = request.files.getlist('files[]')
            
            if not files or all(file.filename == '' for file in files):
                return jsonify({
                    'status': 'error',
                    'message': 'No files selected'
                }), 400
            
            # Create new analysis session
            session = AnalysisSession(session_name=session_name)
            db.session.add(session)
            db.session.commit()
            
            uploaded_files = []
            invalid_files = []
            
            for file in files:
                if file and file.filename and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{session.id}_{filename}")
                    file.save(file_path)
                    
                    # Get file extension
                    file_type = filename.rsplit('.', 1)[1].lower()
                    
                    # Create file record
                    uploaded_file = UploadedFile(
                        filename=filename,
                        file_type=file_type,
                        file_path=file_path,
                        session_id=session.id,
                        file_size=os.path.getsize(file_path)
                    )
                    db.session.add(uploaded_file)
                    uploaded_files.append(uploaded_file)
                else:
                    invalid_files.append(file.filename)
            
            session.file_count = len(uploaded_files)
            db.session.commit()
            
            if uploaded_files:
                return jsonify({
                    'status': 'success',
                    'message': f'Successfully uploaded {len(uploaded_files)} files',
                    'session_id': session.id,
                    'uploaded_files': [f.filename for f in uploaded_files],
                    'invalid_files': invalid_files
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'No valid files were uploaded',
                    'invalid_files': invalid_files
                }), 400
                
        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Upload failed: {str(e)}'
            }), 500

    @app.route('/api/analyze/<int:session_id>', methods=['POST'])
    def api_analyze_data(session_id):
        """API endpoint to analyze uploaded data"""
        session = AnalysisSession.query.get(session_id)
        
        if not session:
            return jsonify({
                'status': 'error',
                'message': 'Session not found'
            }), 404
        
        try:
            # Parse all files in the session
            parsed_data = {}
            file_parser = FileParserFactory()
            
            for uploaded_file in session.files:
                logging.info(f"Parsing file: {uploaded_file.filename}")
                parser = file_parser.get_parser(uploaded_file.file_type)
                data = parser.parse(uploaded_file.file_path)
                parsed_data[uploaded_file.filename] = {
                    'data': data,
                    'file_type': uploaded_file.file_type
                }
            
            # Perform analysis
            analysis_results = perform_comprehensive_analysis(parsed_data)
            
            # Store results in session
            session.set_results(analysis_results)
            db.session.commit()
            
            # Get the serialized results to return
            serialized_results = session.get_results()
            
            return jsonify({
                'status': 'success',
                'message': 'Analysis completed successfully',
                'session_id': session.id,
                'results': serialized_results
            })
        
        except Exception as e:
            logging.error(f"Analysis error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Analysis failed: {str(e)}'
            }), 500

    @app.route('/api/session/<int:session_id>')
    def api_get_session(session_id):
        """API endpoint to get session details"""
        session = AnalysisSession.query.get(session_id)
        
        if not session:
            return jsonify({
                'status': 'error',
                'message': 'Session not found'
            }), 404
        
        results = session.get_results()
        
        return jsonify({
            'status': 'success',
            'session': {
                'id': session.id,
                'session_name': session.session_name,
                'file_count': session.file_count,
                'created_at': session.created_at.isoformat(),
                'files': [{
                    'id': f.id,
                    'filename': f.filename,
                    'file_type': f.file_type,
                    'file_size': f.file_size
                } for f in session.files]
            },
            'results': results
        })

    @app.route('/api/export/<int:session_id>/<format>')
    def api_export_results(session_id, format):
        """API endpoint for export analysis results"""
        session = AnalysisSession.query.get(session_id)
        
        if not session:
            return jsonify({
                'status': 'error',
                'message': 'Session not found'
            }), 404
        
        results = session.get_results()
        
        if not results:
            return jsonify({
                'status': 'error',
                'message': 'No analysis results to export'
            }), 400
        
        try:
            export_utils = ExportUtils()
            file_path = export_utils.export(results, format, session.session_name)
            return send_file(file_path, as_attachment=True)
        
        except Exception as e:
            logging.error(f"Export error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Export failed: {str(e)}'
            }), 500

    @app.route('/api/delete_session/<int:session_id>', methods=['DELETE'])
    def api_delete_session(session_id):
        """API endpoint to delete an analysis session and its files"""
        session = AnalysisSession.query.get(session_id)
        
        if not session:
            return jsonify({
                'status': 'error',
                'message': 'Session not found'
            }), 404
        
        try:
            # Delete uploaded files
            deleted_files = []
            for uploaded_file in session.files:
                try:
                    if os.path.exists(uploaded_file.file_path):
                        os.remove(uploaded_file.file_path)
                        deleted_files.append(uploaded_file.filename)
                except Exception as e:
                    logging.warning(f"Could not delete file {uploaded_file.file_path}: {str(e)}")
            
            session_name = session.session_name
            
            # Delete from database
            db.session.delete(session)
            db.session.commit()
            
            return jsonify({
                'status': 'success',
                'message': 'Session deleted successfully',
                'session_name': session_name,
                'deleted_files': deleted_files
            })
        
        except Exception as e:
            logging.error(f"Delete session error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Delete failed: {str(e)}'
            }), 500

def perform_comprehensive_analysis(parsed_data):
    """Perform comprehensive data analysis"""
    results = {
        'data_types': {},
        'patterns': {},
        'relationships': {},
        'insights': {},
        'summary': {}
    }
    
    # Initialize analyzers
    data_type_analyzer = DataTypeAnalyzer()
    pattern_analyzer = PatternAnalyzer()
    relationship_analyzer = RelationshipAnalyzer()
    
    all_dataframes = {}
    
    # Analyze each file
    for filename, file_info in parsed_data.items():
        data = file_info['data']
        
        if data is not None and not data.empty:
            all_dataframes[filename] = data
            
            # Data type analysis
            results['data_types'][filename] = data_type_analyzer.analyze(data)
            
            # Pattern analysis
            results['patterns'][filename] = pattern_analyzer.analyze(data)
            
            # Generate insights for this file
            results['insights'][filename] = generate_file_insights(data, filename)
    
    # Relationship analysis across all files
    if len(all_dataframes) > 1:
        results['relationships'] = relationship_analyzer.analyze_relationships(all_dataframes)
    
    # Generate summary
    results['summary'] = generate_analysis_summary(results, all_dataframes)
    
    return results

def generate_file_insights(data, filename):
    """Generate insights for a specific file"""
    # Basic statistics
    memory_usage = data.memory_usage(deep=True).sum()
    missing_data_count = data.isnull().sum().sum()
    total_cells = len(data) * len(data.columns)
    missing_percentage = (missing_data_count / total_cells * 100) if total_cells > 0 else 0
    
    insights = {
        'summary': {
            'rows': len(data),
            'columns': len(data.columns), 
            'memory_usage': f"{memory_usage / 1024:.1f} KB" if memory_usage < 1024*1024 else f"{memory_usage / (1024*1024):.1f} MB",
            'missing_data_percentage': missing_percentage
        },
        'key_insights': [],
        'recommendations': [],
        'data_quality_issues': []
    }
    
    # Generate key insights
    if len(data) > 0:
        insights['key_insights'].append(f"Dataset contains {len(data):,} rows across {len(data.columns)} columns")
        
        numeric_cols = len(data.select_dtypes(include=['number']).columns)
        if numeric_cols > 0:
            insights['key_insights'].append(f"Found {numeric_cols} numeric columns for statistical analysis")
            
        text_cols = len(data.select_dtypes(include=['object']).columns)  
        if text_cols > 0:
            insights['key_insights'].append(f"Identified {text_cols} text columns for pattern analysis")
            
        datetime_cols = len(data.select_dtypes(include=['datetime']).columns)
        if datetime_cols > 0:
            insights['key_insights'].append(f"Detected {datetime_cols} date/time columns for temporal analysis")
    
    # Generate recommendations
    if missing_percentage > 10:
        insights['recommendations'].append(f"Consider addressing missing data ({missing_percentage:.1f}% of cells are empty)")
        
    duplicate_rows = data.duplicated().sum()
    if duplicate_rows > 0:
        insights['recommendations'].append(f"Found {duplicate_rows} duplicate rows that could be removed")
        
    # Check for potential ID columns
    for col in data.columns:
        if data[col].nunique() == len(data) and 'id' in col.lower():
            insights['key_insights'].append(f"Column '{col}' appears to be a unique identifier")
            
    # Data quality issues
    if missing_percentage > 20:
        insights['data_quality_issues'].append(f"High missing data rate: {missing_percentage:.1f}% of cells are empty")
        
    if duplicate_rows > len(data) * 0.1:
        insights['data_quality_issues'].append(f"High duplicate rate: {duplicate_rows} duplicate rows ({duplicate_rows/len(data)*100:.1f}%)")
    
    return insights

def generate_analysis_summary(results, dataframes):
    """Generate overall analysis summary"""
    summary = {
        'total_files': len(dataframes),
        'total_rows': sum(len(df) for df in dataframes.values()),
        'total_columns': sum(len(df.columns) for df in dataframes.values()),
        'common_patterns': [],
        'potential_keys': [],
        'data_quality_score': 0
    }
    
    # Calculate data quality score (basic implementation)
    if dataframes:
        missing_data_ratio = sum(
            sum(df.isnull().sum()) / (len(df) * len(df.columns))
            for df in dataframes.values()
        ) / len(dataframes)
        
        summary['data_quality_score'] = max(0, 100 - (missing_data_ratio * 100))
    
    return summary