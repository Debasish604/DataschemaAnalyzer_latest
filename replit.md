# Data Analysis Tool

## Overview

This is a Flask-based web application that provides intelligent analysis of database files. The tool accepts multiple file formats (CSV, SQL, Excel, XML) and performs comprehensive data analysis including data type detection, pattern analysis, relationship discovery, and quality assessment. Users can upload files in sessions, view detailed analysis results, and export findings in various formats.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: Bootstrap 5 with dark theme for responsive UI
- **JavaScript Libraries**: Chart.js for data visualization, Feather Icons for consistent iconography
- **Template Engine**: Jinja2 templates with Flask for server-side rendering
- **File Upload**: Drag-and-drop interface with progress tracking and file validation
- **Interactive Elements**: Dynamic charts, tooltips, and hover effects for enhanced user experience

### Backend Architecture
- **Web Framework**: Flask with SQLAlchemy ORM for database operations
- **File Processing**: Factory pattern for parsers supporting CSV, SQL, Excel (.xls/.xlsx), and XML formats
- **Analysis Engine**: Three-tier analysis system:
  - **Data Type Analyzer**: Identifies column types, patterns, and characteristics using regex and statistical methods
  - **Pattern Analyzer**: Detects outliers, correlations, and data quality issues using scipy and sklearn
  - **Relationship Analyzer**: Discovers table relationships and suggests join strategies
- **Session Management**: File upload sessions with metadata tracking and result persistence
- **Export System**: Multi-format export utility (JSON, CSV, HTML, TXT) with data serialization

### Data Storage Solutions
- **Primary Database**: SQLAlchemy with configurable backends (SQLite default, PostgreSQL via DATABASE_URL)
- **Models**: Two main entities:
  - `AnalysisSession`: Stores session metadata and JSON-serialized analysis results
  - `UploadedFile`: Tracks individual files with metadata and foreign key relationships
- **File Storage**: Local filesystem storage in uploads directory with secure filename handling

### Authentication and Authorization
- **Session Management**: Flask sessions with configurable secret key
- **File Security**: Secure filename validation and file type restrictions
- **Upload Limits**: 50MB maximum file size with proper error handling

## External Dependencies

### Core Libraries
- **Flask**: Web framework with SQLAlchemy extension for database operations
- **Pandas**: Primary data manipulation and analysis library
- **NumPy**: Numerical computing support for statistical operations
- **SQLParse**: SQL statement parsing for SQL file analysis
- **OpenPyXL/XlRd**: Excel file reading capabilities

### Analysis Libraries
- **SciPy**: Statistical functions for outlier detection and data analysis
- **Scikit-learn**: Machine learning utilities for clustering and preprocessing (DBSCAN, StandardScaler)

### Frontend Dependencies
- **Bootstrap 5**: CSS framework delivered via CDN
- **Chart.js**: Data visualization library for interactive charts
- **Feather Icons**: Icon library for consistent UI elements

### Infrastructure
- **Werkzeug**: WSGI utilities including ProxyFix for deployment
- **File System**: Local storage for uploaded files and export generation
- **Environment Variables**: Configuration through DATABASE_URL and SESSION_SECRET