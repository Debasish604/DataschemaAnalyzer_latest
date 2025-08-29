"""
Data Analysis Tool Setup Instructions

To run this application on your local system:

1. Install Python 3.11+ if not already installed

2. Create a virtual environment:
   python -m venv data_analysis_env
   
3. Activate the virtual environment:
   - Windows: data_analysis_env\Scripts\activate
   - Mac/Linux: source data_analysis_env/bin/activate

4. Install required packages:
   pip install Flask==2.3.3
   pip install Flask-SQLAlchemy==3.0.5
   pip install Werkzeug==2.3.7
   pip install gunicorn==21.2.0
   pip install pandas==2.1.1
   pip install numpy==1.25.2
   pip install openpyxl==3.1.2
   pip install xlrd==2.0.1
   pip install scikit-learn==1.3.0
   pip install scipy==1.11.3
   pip install sqlparse==0.4.4
   pip install psycopg2-binary==2.9.7
   pip install email-validator==2.0.0

5. Set environment variables (optional):
   - SESSION_SECRET=your-secret-key-here
   - DATABASE_URL=sqlite:///data_analysis.db (default)

6. Run the application:
   python app.py
   
   Or with gunicorn:
   gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app

7. Open your browser and go to: http://localhost:5000

File Structure:
├── main.py                     # Entry point
├── app.py                      # Flask app configuration
├── models.py                   # Database models
├── routes.py                   # Web routes
├── analyzers/
│   ├── data_type_analyzer.py   # Data type detection
│   ├── pattern_analyzer.py     # Pattern analysis
│   └── relationship_analyzer.py # Table relationships
├── parsers/
│   ├── file_parser.py          # Base parser
│   ├── csv_parser.py           # CSV parser
│   ├── sql_parser.py           # SQL parser
│   ├── excel_parser.py         # Excel parser
│   └── xml_parser.py           # XML parser
├── utils/
│   ├── data_insights.py        # Data insights generation
│   └── export_utils.py         # Export functionality
├── templates/                  # HTML templates
│   ├── base.html
│   ├── index.html
│   ├── upload.html
│   └── analysis.html
└── static/                     # CSS and JS files
    ├── css/custom.css
    └── js/analysis.js
"""