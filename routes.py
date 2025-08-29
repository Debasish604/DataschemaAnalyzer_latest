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

    @app.route('/')
    def index():
        """Home page"""
        recent_sessions = AnalysisSession.query.order_by(AnalysisSession.created_at.desc()).limit(5).all()
        return render_template('index.html', recent_sessions=recent_sessions)

    @app.route('/upload', methods=['GET', 'POST'])
    def upload_files():
        """File upload page"""
        if request.method == 'POST':
            session_name = request.form.get('session_name', 'Unnamed Session')
            files = request.files.getlist('files[]')
            
            if not files or all(file.filename == '' for file in files):
                flash('No files selected', 'error')
                return redirect(request.url)
            
            # Create new analysis session
            session = AnalysisSession(session_name=session_name)
            db.session.add(session)
            db.session.commit()
            
            uploaded_files = []
            
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
                    flash(f'Invalid file type: {file.filename}', 'warning')
            
            session.file_count = len(uploaded_files)
            db.session.commit()
            
            if uploaded_files:
                flash(f'Successfully uploaded {len(uploaded_files)} files', 'success')
                return redirect(url_for('analyze_data', session_id=session.id))
            else:
                flash('No valid files were uploaded', 'error')
        
        return render_template('upload.html')

    @app.route('/analyze/<int:session_id>')
    def analyze_data(session_id):
        """Analyze uploaded data and show results"""
        session = AnalysisSession.query.get_or_404(session_id)
        
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
            
            return render_template('analysis.html', 
                                 session=session, 
                                 results=analysis_results,
                                 parsed_data=parsed_data)
        
        except Exception as e:
            logging.error(f"Analysis error: {str(e)}")
            flash(f'Analysis failed: {str(e)}', 'error')
            return redirect(url_for('index'))

    @app.route('/export/<int:session_id>/<format>')
    def export_results(session_id, format):
        """Export analysis results"""
        session = AnalysisSession.query.get_or_404(session_id)
        results = session.get_results()
        
        if not results:
            flash('No analysis results to export', 'error')
            return redirect(url_for('index'))
        
        try:
            export_utils = ExportUtils()
            file_path = export_utils.export(results, format, session.session_name)
            return send_file(file_path, as_attachment=True)
        
        except Exception as e:
            logging.error(f"Export error: {str(e)}")
            flash(f'Export failed: {str(e)}', 'error')
            return redirect(url_for('analyze_data', session_id=session_id))

    @app.route('/session/<int:session_id>')
    def view_session(session_id):
        """View a specific analysis session"""
        session = AnalysisSession.query.get_or_404(session_id)
        results = session.get_results()
        
        if not results:
            flash('No analysis results found for this session', 'warning')
            return redirect(url_for('index'))
        
        return render_template('analysis.html', 
                             session=session, 
                             results=results)

    @app.route('/delete_session/<int:session_id>', methods=['POST'])
    def delete_session(session_id):
        """Delete an analysis session and its files"""
        session = AnalysisSession.query.get_or_404(session_id)
        
        # Delete uploaded files
        for uploaded_file in session.files:
            try:
                if os.path.exists(uploaded_file.file_path):
                    os.remove(uploaded_file.file_path)
            except Exception as e:
                logging.warning(f"Could not delete file {uploaded_file.file_path}: {str(e)}")
        
        # Delete from database
        db.session.delete(session)
        db.session.commit()
        
        flash('Session deleted successfully', 'success')
        return redirect(url_for('index'))

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
    insights = {
        'row_count': len(data),
        'column_count': len(data.columns),
        'memory_usage': data.memory_usage(deep=True).sum(),
        'missing_data': data.isnull().sum().to_dict(),
        'duplicate_rows': data.duplicated().sum(),
        'numeric_columns': list(data.select_dtypes(include=['number']).columns),
        'text_columns': list(data.select_dtypes(include=['object']).columns),
        'datetime_columns': list(data.select_dtypes(include=['datetime']).columns)
    }
    
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