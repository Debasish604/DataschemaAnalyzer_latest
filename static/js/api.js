// API communication layer for Data Analysis Tool

class DataAnalysisAPI {
    constructor() {
        this.baseUrl = '';
    }

    // Helper method to make HTTP requests
    async makeRequest(url, options = {}) {
        try {
            // Don't set Content-Type for FormData (browser will set correct boundary)
            const headers = options.body instanceof FormData 
                ? (options.headers || {})
                : { 'Content-Type': 'application/json', ...(options.headers || {}) };
            
            const response = await fetch(url, {
                headers,
                ...options
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || `HTTP error! status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('API Request failed:', error);
            throw error;
        }
    }

    // Get recent sessions
    async getSessions() {
        return this.makeRequest('/api/sessions');
    }

    // Get specific session details
    async getSession(sessionId) {
        return this.makeRequest(`/api/session/${sessionId}`);
    }

    // Upload files
    async uploadFiles(sessionName, files) {
        const formData = new FormData();
        formData.append('session_name', sessionName);
        
        files.forEach(file => {
            formData.append('files[]', file);
        });

        return this.makeRequest('/api/upload', {
            method: 'POST',
            headers: {}, // Don't set Content-Type for FormData
            body: formData
        });
    }

    // Analyze session data
    async analyzeSession(sessionId) {
        return this.makeRequest(`/api/analyze/${sessionId}`, {
            method: 'POST'
        });
    }

    // Delete session
    async deleteSession(sessionId) {
        return this.makeRequest(`/api/delete_session/${sessionId}`, {
            method: 'DELETE'
        });
    }

    // Export results
    downloadExport(sessionId, format) {
        window.open(`/api/export/${sessionId}/${format}`, '_blank');
    }
}

// Global API instance
const dataAPI = new DataAnalysisAPI();

// UI Helper functions
class UIHelpers {
    static showAlert(message, type = 'info') {
        const alertContainer = document.getElementById('alertContainer') || document.body;
        
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show`;
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        alertContainer.insertBefore(alert, alertContainer.firstChild);
        
        // Auto dismiss after 5 seconds
        setTimeout(() => {
            if (alert.parentNode) {
                alert.remove();
            }
        }, 5000);
    }

    static showLoading(element, message = 'Loading...') {
        element.innerHTML = `
            <div class="d-flex align-items-center justify-content-center py-4">
                <div class="spinner-border text-primary me-2" role="status"></div>
                <span>${message}</span>
            </div>
        `;
    }

    static formatDateTime(dateString) {
        return new Date(dateString).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    static formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
}

// File upload handler
class FileUploadHandler {
    constructor(dropZone, fileInput, uploadForm) {
        this.dropZone = dropZone;
        this.fileInput = fileInput;
        this.uploadForm = uploadForm;
        this.selectedFiles = [];
        
        this.initializeEventListeners();
    }

    initializeEventListeners() {
        // Drag and drop events
        if (this.dropZone) {
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
                this.dropZone.addEventListener(eventName, this.preventDefaults, false);
            });

            ['dragenter', 'dragover'].forEach(eventName => {
                this.dropZone.addEventListener(eventName, () => this.highlight(), false);
            });

            ['dragleave', 'drop'].forEach(eventName => {
                this.dropZone.addEventListener(eventName, () => this.unhighlight(), false);
            });

            this.dropZone.addEventListener('drop', this.handleDrop.bind(this), false);
        }

        // File input change event
        if (this.fileInput) {
            this.fileInput.addEventListener('change', this.handleFileSelect.bind(this));
        }

        // Form submit event
        if (this.uploadForm) {
            this.uploadForm.addEventListener('submit', this.handleSubmit.bind(this));
        }
    }

    preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    highlight() {
        this.dropZone.classList.add('drag-over');
    }

    unhighlight() {
        this.dropZone.classList.remove('drag-over');
    }

    handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        this.handleFiles(files);
    }

    handleFileSelect(e) {
        const files = e.target.files;
        this.handleFiles(files);
    }

    handleFiles(files) {
        this.selectedFiles = Array.from(files);
        this.updateFileList();
    }

    updateFileList() {
        const fileListContainer = document.getElementById('fileList');
        const selectedFilesContainer = document.getElementById('selectedFiles');
        
        if (!fileListContainer) return;

        if (this.selectedFiles.length === 0) {
            fileListContainer.innerHTML = '<p class="text-muted">No files selected</p>';
            if (selectedFilesContainer) selectedFilesContainer.style.display = 'none';
            return;
        }

        // Show the selected files container
        if (selectedFilesContainer) selectedFilesContainer.style.display = 'block';

        const fileListHTML = this.selectedFiles.map(file => `
            <div class="selected-file d-flex justify-content-between align-items-center p-2 border rounded mb-2">
                <div>
                    <strong>${file.name}</strong>
                    <div class="small text-muted">${UIHelpers.formatFileSize(file.size)}</div>
                </div>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="uploadHandler.removeFile('${file.name}')">
                    <i data-feather="x"></i>
                </button>
            </div>
        `).join('');

        fileListContainer.innerHTML = fileListHTML;
        feather.replace();
    }

    removeFile(fileName) {
        this.selectedFiles = this.selectedFiles.filter(file => file.name !== fileName);
        this.updateFileList();
    }

    async handleSubmit(e) {
        e.preventDefault();
        
        if (this.selectedFiles.length === 0) {
            UIHelpers.showAlert('Please select files to upload', 'warning');
            return;
        }

        const sessionName = document.getElementById('sessionName').value || 'Unnamed Session';
        const submitBtn = this.uploadForm.querySelector('button[type="submit"]');
        
        try {
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Uploading...';

            const result = await dataAPI.uploadFiles(sessionName, this.selectedFiles);
            
            UIHelpers.showAlert(result.message, 'success');
            
            // Redirect to analysis page after successful upload
            setTimeout(() => {
                window.location.href = `/analyze/${result.session_id}`;
            }, 1000);

        } catch (error) {
            UIHelpers.showAlert('Upload failed: ' + error.message, 'danger');
        } finally {
            submitBtn.disabled = false;
            submitBtn.innerHTML = '<i data-feather="upload"></i> Upload Files';
            feather.replace();
        }
    }
}

// Session management
class SessionManager {
    static async loadRecentSessions() {
        const container = document.getElementById('recentSessions');
        if (!container) return;

        try {
            UIHelpers.showLoading(container, 'Loading recent sessions...');
            
            const result = await dataAPI.getSessions();
            this.renderSessions(container, result.sessions);
            
        } catch (error) {
            container.innerHTML = `<p class="text-danger">Failed to load sessions: ${error.message}</p>`;
        }
    }

    static renderSessions(container, sessions) {
        if (sessions.length === 0) {
            container.innerHTML = `
                <p class="text-muted text-center py-3">
                    <i data-feather="inbox"></i><br>
                    No recent sessions.<br>
                    <a href="/upload">Start your first analysis</a>
                </p>
            `;
            feather.replace();
            return;
        }

        const sessionsHTML = sessions.map(session => `
            <div class="d-flex justify-content-between align-items-center mb-3 p-2 border rounded">
                <div>
                    <h6 class="mb-1">${session.session_name}</h6>
                    <small class="text-muted">
                        ${session.file_count} files • 
                        ${UIHelpers.formatDateTime(session.created_at)}
                    </small>
                </div>
                <div class="btn-group-vertical btn-group-sm">
                    <a href="/session/${session.id}" class="btn btn-outline-primary btn-sm">
                        <i data-feather="eye"></i>
                    </a>
                    <button type="button" class="btn btn-outline-danger btn-sm" 
                            onclick="SessionManager.deleteSession(${session.id}, '${session.session_name}')">
                        <i data-feather="trash-2"></i>
                    </button>
                </div>
            </div>
        `).join('');

        container.innerHTML = sessionsHTML;
        feather.replace();
    }

    static async deleteSession(sessionId, sessionName) {
        if (!confirm(`Are you sure you want to delete session "${sessionName}"?`)) {
            return;
        }

        try {
            const result = await dataAPI.deleteSession(sessionId);
            UIHelpers.showAlert(result.message, 'success');
            
            // Reload sessions list
            this.loadRecentSessions();
            
        } catch (error) {
            UIHelpers.showAlert('Delete failed: ' + error.message, 'danger');
        }
    }
}

// Analysis page functionality
class AnalysisPage {
    constructor(sessionId) {
        this.sessionId = sessionId;
        this.sessionData = null;
        this.analysisResults = null;
    }

    async initialize() {
        try {
            await this.loadSessionData();
            this.setupExportHandlers();
            this.renderAnalysisResults();
            
        } catch (error) {
            UIHelpers.showAlert('Failed to load analysis data: ' + error.message, 'danger');
        }
    }

    async loadSessionData() {
        const result = await dataAPI.getSession(this.sessionId);
        this.sessionData = result.session;
        this.analysisResults = result.results;
    }

    async startAnalysis() {
        try {
            const analysisBtn = document.getElementById('startAnalysisBtn');
            if (analysisBtn) {
                analysisBtn.disabled = true;
                analysisBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
            }

            const result = await dataAPI.analyzeSession(this.sessionId);
            this.analysisResults = result.results;
            
            UIHelpers.showAlert('Analysis completed successfully!', 'success');
            this.renderAnalysisResults();
            
        } catch (error) {
            UIHelpers.showAlert('Analysis failed: ' + error.message, 'danger');
        } finally {
            if (analysisBtn) {
                analysisBtn.disabled = false;
                analysisBtn.innerHTML = '<i data-feather="play"></i> Start Analysis';
                feather.replace();
            }
        }
    }

    setupExportHandlers() {
        const exportButtons = document.querySelectorAll('[data-export-format]');
        exportButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                const format = btn.dataset.exportFormat;
                dataAPI.downloadExport(this.sessionId, format);
            });
        });
    }

    renderAnalysisResults() {
        if (!this.analysisResults) {
            // Show analysis not started yet
            const container = document.getElementById('analysisContent');
            if (container) {
                container.innerHTML = `
                    <div class="text-center py-5">
                        <i data-feather="play-circle" style="width: 64px; height: 64px;" class="text-muted mb-3"></i>
                        <h4>Ready to analyze your data</h4>
                        <p class="text-muted">Click the button below to start the analysis process.</p>
                        <button id="startAnalysisBtn" class="btn btn-primary btn-lg" onclick="analysisPage.startAnalysis()">
                            <i data-feather="play"></i> Start Analysis
                        </button>
                    </div>
                `;
                feather.replace();
            }
            return;
        }

        // Render full analysis results (implementation would go here)
        // For now, we'll show a success message
        const container = document.getElementById('analysisContent');
        if (container) {
            container.innerHTML = `
                <div class="alert alert-success">
                    <i data-feather="check-circle"></i>
                    Analysis completed! Results are displayed below.
                </div>
            `;
            feather.replace();
        }
    }
}

// Global variables for page-specific functionality
let uploadHandler;
let analysisPage;

// Initialize page-specific functionality
document.addEventListener('DOMContentLoaded', function() {
    // Initialize upload functionality if on upload page
    const uploadForm = document.getElementById('uploadForm');
    if (uploadForm) {
        const dropZone = document.getElementById('dropZone');
        const fileInput = document.getElementById('fileInput');
        uploadHandler = new FileUploadHandler(dropZone, fileInput, uploadForm);
    }

    // Initialize recent sessions if on home page
    SessionManager.loadRecentSessions();

    // Initialize analysis page if session ID is present
    const sessionIdElement = document.getElementById('sessionId');
    if (sessionIdElement) {
        const sessionId = sessionIdElement.value;
        analysisPage = new AnalysisPage(sessionId);
        analysisPage.initialize();
    }
});