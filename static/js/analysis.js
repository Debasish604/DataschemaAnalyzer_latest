// Analysis page JavaScript functionality

let analysisChart = null;

function initializeAnalysis() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Add click handlers for interactive elements
    addChartClickHandlers();
    
    // Initialize any existing chart data
    setupChartData();
}

function addChartClickHandlers() {
    // Add click handlers for data visualization
    const confidenceBars = document.querySelectorAll('.progress-bar');
    confidenceBars.forEach(bar => {
        bar.style.cursor = 'pointer';
        bar.addEventListener('click', function() {
            const confidence = parseFloat(this.textContent.replace('%', ''));
            showConfidenceChart(confidence);
        });
    });

    // Add hover effects for correlation badges
    const correlationBadges = document.querySelectorAll('.badge');
    correlationBadges.forEach(badge => {
        badge.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.1)';
        });
        badge.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
        });
    });
}

function setupChartData() {
    // Extract data quality information for visualization
    const qualityCards = document.querySelectorAll('.card .progress-bar');
    if (qualityCards.length > 0) {
        console.log('Data quality metrics available for visualization');
    }
}

function showConfidenceChart(confidence) {
    const modal = new bootstrap.Modal(document.getElementById('chartModal'));
    const ctx = document.getElementById('analysisChart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (analysisChart) {
        analysisChart.destroy();
    }
    
    // Create confidence visualization
    analysisChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Confidence', 'Uncertainty'],
            datasets: [{
                data: [confidence, 100 - confidence],
                backgroundColor: [
                    'rgba(13, 110, 253, 0.8)',  // Primary blue
                    'rgba(108, 117, 125, 0.3)'  // Muted gray
                ],
                borderColor: [
                    'rgba(13, 110, 253, 1)',
                    'rgba(108, 117, 125, 0.5)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Data Type Confidence Level',
                    color: '#fff'
                },
                legend: {
                    labels: {
                        color: '#fff'
                    }
                }
            }
        }
    });
    
    modal.show();
}

function showCorrelationHeatmap(correlationData) {
    const modal = new bootstrap.Modal(document.getElementById('chartModal'));
    const ctx = document.getElementById('analysisChart').getContext('2d');
    
    if (analysisChart) {
        analysisChart.destroy();
    }
    
    // Create correlation heatmap (simplified as scatter plot)
    analysisChart = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Correlations',
                data: correlationData,
                backgroundColor: 'rgba(13, 110, 253, 0.6)',
                borderColor: 'rgba(13, 110, 253, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Column Correlations',
                    color: '#fff'
                },
                legend: {
                    labels: {
                        color: '#fff'
                    }
                }
            },
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Column 1',
                        color: '#fff'
                    },
                    ticks: {
                        color: '#fff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Column 2',
                        color: '#fff'
                    },
                    ticks: {
                        color: '#fff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            }
        }
    });
    
    modal.show();
}

function showOutlierDistribution(outlierData) {
    const modal = new bootstrap.Modal(document.getElementById('chartModal'));
    const ctx = document.getElementById('analysisChart').getContext('2d');
    
    if (analysisChart) {
        analysisChart.destroy();
    }
    
    // Create outlier distribution chart
    analysisChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: Object.keys(outlierData),
            datasets: [{
                label: 'Outlier Count',
                data: Object.values(outlierData),
                backgroundColor: 'rgba(255, 193, 7, 0.8)',
                borderColor: 'rgba(255, 193, 7, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Outliers by Column',
                    color: '#fff'
                },
                legend: {
                    labels: {
                        color: '#fff'
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#fff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                y: {
                    ticks: {
                        color: '#fff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            }
        }
    });
    
    modal.show();
}

function showDataQualityOverview() {
    const qualityScores = extractQualityScores();
    
    if (qualityScores.length === 0) {
        console.log('No quality data available');
        return;
    }
    
    const modal = new bootstrap.Modal(document.getElementById('chartModal'));
    const ctx = document.getElementById('analysisChart').getContext('2d');
    
    if (analysisChart) {
        analysisChart.destroy();
    }
    
    analysisChart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: ['Completeness', 'Consistency', 'Uniqueness', 'Validity', 'Accuracy'],
            datasets: [{
                label: 'Data Quality Metrics',
                data: qualityScores,
                backgroundColor: 'rgba(13, 110, 253, 0.2)',
                borderColor: 'rgba(13, 110, 253, 1)',
                borderWidth: 2,
                pointBackgroundColor: 'rgba(13, 110, 253, 1)',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: 'rgba(13, 110, 253, 1)'
            }]
        },
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Data Quality Assessment',
                    color: '#fff'
                },
                legend: {
                    labels: {
                        color: '#fff'
                    }
                }
            },
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        color: '#fff'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    angleLines: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    pointLabels: {
                        color: '#fff'
                    }
                }
            }
        }
    });
    
    modal.show();
}

function extractQualityScores() {
    // Extract quality scores from the page
    const scores = [];
    
    // Look for completeness score
    const completenessElement = document.querySelector('[style*="width:"] .progress-bar');
    if (completenessElement) {
        const width = completenessElement.style.width;
        const score = parseFloat(width.replace('%', ''));
        scores.push(score);
    }
    
    // Add mock scores for demonstration (in real implementation, these would come from analysis)
    scores.push(85, 78, 92, 88); // Consistency, Uniqueness, Validity, Accuracy
    
    return scores;
}

// Utility functions for data visualization
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

function getColorByConfidence(confidence) {
    if (confidence >= 0.8) return 'rgba(40, 167, 69, 0.8)';   // Success green
    if (confidence >= 0.6) return 'rgba(255, 193, 7, 0.8)';   // Warning yellow
    if (confidence >= 0.4) return 'rgba(253, 126, 20, 0.8)';  // Orange
    return 'rgba(220, 53, 69, 0.8)';                          // Danger red
}

// Export functionality
function exportChart() {
    if (analysisChart) {
        const link = document.createElement('a');
        link.download = 'analysis_chart.png';
        link.href = analysisChart.toBase64Image();
        link.click();
    }
}

// Search and filter functionality
function filterAnalysisResults(searchTerm) {
    const tables = document.querySelectorAll('.table tbody tr');
    
    tables.forEach(row => {
        const text = row.textContent.toLowerCase();
        if (text.includes(searchTerm.toLowerCase())) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

// Add search functionality if search input exists
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('analysisSearch');
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            filterAnalysisResults(this.value);
        });
    }
});

// Enhanced table interactions
function addTableInteractions() {
    const tableRows = document.querySelectorAll('.table tbody tr');
    
    tableRows.forEach(row => {
        row.addEventListener('click', function() {
            // Highlight selected row
            tableRows.forEach(r => r.classList.remove('table-active'));
            this.classList.add('table-active');
            
            // Show row details if available
            showRowDetails(this);
        });
    });
}

function showRowDetails(row) {
    const cells = row.querySelectorAll('td');
    if (cells.length > 0) {
        console.log('Row details:', Array.from(cells).map(cell => cell.textContent));
        // Could show a detail panel or modal here
    }
}

// Progressive disclosure for large datasets
function addProgressiveDisclosure() {
    const largeTables = document.querySelectorAll('.table tbody');
    
    largeTables.forEach(tbody => {
        const rows = tbody.querySelectorAll('tr');
        if (rows.length > 10) {
            // Hide rows beyond the first 10
            for (let i = 10; i < rows.length; i++) {
                rows[i].style.display = 'none';
            }
            
            // Add "Show more" button
            const showMoreBtn = document.createElement('button');
            showMoreBtn.className = 'btn btn-outline-secondary btn-sm mt-2';
            showMoreBtn.textContent = `Show ${rows.length - 10} more rows`;
            
            showMoreBtn.addEventListener('click', function() {
                for (let i = 10; i < rows.length; i++) {
                    rows[i].style.display = '';
                }
                this.style.display = 'none';
            });
            
            tbody.parentNode.appendChild(showMoreBtn);
        }
    });
}

// Initialize enhanced features
document.addEventListener('DOMContentLoaded', function() {
    addTableInteractions();
    addProgressiveDisclosure();
});
