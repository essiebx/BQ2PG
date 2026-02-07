// Global state
let currentStep = 1;
let credentialsData = null;
let migrationJobId = null;
let migrationInterval = null;
let startTime = null;

// API base URL
const API_BASE = window.location.origin;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    updateProgressTracker();
});

// Event Listeners
function setupEventListeners() {
    // File upload
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('credentialsFile');

    uploadArea.addEventListener('click', () => fileInput.click());
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleDrop);
    fileInput.addEventListener('change', handleFileSelect);

    // Step 1: Validate credentials
    document.getElementById('validateCredentials').addEventListener('click', validateCredentials);

    // Step 2: Test BigQuery
    document.getElementById('testBigQuery').addEventListener('click', testBigQuery);

    // Step 3: Test PostgreSQL
    document.getElementById('testPostgres').addEventListener('click', testPostgres);

    // Step 4: Start migration
    document.getElementById('startMigration').addEventListener('click', startMigration);
}

// File Upload Handlers
function handleDragOver(e) {
    e.preventDefault();
    e.currentTarget.classList.add('dragover');
}

function handleDragLeave(e) {
    e.currentTarget.classList.remove('dragover');
}

function handleDrop(e) {
    e.preventDefault();
    e.currentTarget.classList.remove('dragover');

    const files = e.dataTransfer.files;
    if (files.length > 0) {
        handleFile(files[0]);
    }
}

function handleFileSelect(e) {
    const files = e.target.files;
    if (files.length > 0) {
        handleFile(files[0]);
    }
}

function handleFile(file) {
    if (!file.name.endsWith('.json')) {
        showStatus('credentialsStatus', 'error', 'Invalid format. Please upload a JSON file');
        return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            credentialsData = JSON.parse(e.target.result);
            showStatus('credentialsStatus', 'success', `File loaded: ${file.name}`);
            document.getElementById('validateCredentials').disabled = false;
        } catch (error) {
            showStatus('credentialsStatus', 'error', 'Invalid JSON format detected');
            credentialsData = null;
        }
    };
    reader.readAsText(file);
}

// Step 1: Validate Credentials
async function validateCredentials() {
    showStatus('credentialsStatus', 'info', 'Validating credentials...');

    try {
        const response = await fetch('/api/validate-credentials', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ credentials: credentialsData })
        });

        const result = await response.json();

        if (result.valid) {
            showStatus('credentialsStatus', 'success', 'Credentials validated successfully');

            // Auto-fill project ID if available
            if (credentialsData.project_id) {
                document.getElementById('projectId').value = credentialsData.project_id;
            }

            setTimeout(() => goToStep(2), 1000);
        } else {
            showStatus('credentialsStatus', 'error', `Validation failed: ${result.error || 'Invalid credentials'}`);
        }
    } catch (error) {
        showStatus('credentialsStatus', 'error', `Connection error: ${error.message}`);
    }
}

// Step 2: Test BigQuery Connection
async function testBigQuery() {
    const projectId = document.getElementById('projectId').value;
    const dataset = document.getElementById('dataset').value;
    const table = document.getElementById('table').value;

    if (!projectId || !dataset || !table) {
        showStatus('bqStatus', 'error', 'All fields required');
        return;
    }

    showStatus('bqStatus', 'info', 'Testing connection...');

    try {
        const response = await fetch('/api/test-bigquery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                credentials: credentialsData,
                project_id: projectId,
                dataset: dataset,
                table: table
            })
        });

        const result = await response.json();

        if (result.success) {
            showStatus('bqStatus', 'success',
                `Connected. Found ${result.row_count?.toLocaleString() || 'unknown'} records`);
            setTimeout(() => goToStep(3), 1000);
        } else {
            showStatus('bqStatus', 'error', `Failed: ${result.error || 'Connection error'}`);
        }
    } catch (error) {
        showStatus('bqStatus', 'error', `Error: ${error.message}`);
    }
}

// Step 3: Test PostgreSQL Connection
async function testPostgres() {
    const config = {
        host: document.getElementById('pgHost').value,
        port: document.getElementById('pgPort').value,
        database: document.getElementById('pgDatabase').value,
        user: document.getElementById('pgUser').value,
        password: document.getElementById('pgPassword').value,
        table: document.getElementById('pgTable').value
    };

    if (!config.host || !config.database || !config.user || !config.password || !config.table) {
        showStatus('pgStatus', 'error', 'All fields required');
        return;
    }

    showStatus('pgStatus', 'info', 'Testing connection...');

    try {
        const response = await fetch('/api/test-postgres', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });

        const result = await response.json();

        if (result.success) {
            showStatus('pgStatus', 'success', 'Connection established');
            updateSummary();
            setTimeout(() => goToStep(4), 1000);
        } else {
            showStatus('pgStatus', 'error', `Failed: ${result.error || 'Connection error'}`);
        }
    } catch (error) {
        showStatus('pgStatus', 'error', `Error: ${error.message}`);
    }
}

// Step 4: Start Migration
async function startMigration() {
    const config = {
        credentials: credentialsData,
        bigquery: {
            project_id: document.getElementById('projectId').value,
            dataset: document.getElementById('dataset').value,
            table: document.getElementById('table').value,
            row_limit: document.getElementById('rowLimit').value || null,
            where_clause: document.getElementById('whereClause').value || null
        },
        postgres: {
            host: document.getElementById('pgHost').value,
            port: document.getElementById('pgPort').value,
            database: document.getElementById('pgDatabase').value,
            user: document.getElementById('pgUser').value,
            password: document.getElementById('pgPassword').value,
            table: document.getElementById('pgTable').value,
            drop_table: document.getElementById('dropTable').checked
        }
    };

    // Hide start button, show progress
    document.getElementById('startMigration').style.display = 'none';
    document.getElementById('backToConfig').style.display = 'none';
    document.getElementById('migrationProgress').style.display = 'block';

    startTime = Date.now();

    try {
        const response = await fetch('/api/start-migration', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });

        const result = await response.json();

        if (result.job_id) {
            migrationJobId = result.job_id;
            // Poll for status
            migrationInterval = setInterval(() => checkMigrationStatus(), 1000);
        } else {
            showMigrationError(result.error || 'Failed to start migration');
        }
    } catch (error) {
        showMigrationError(error.message);
    }
}

// Check Migration Status
async function checkMigrationStatus() {
    try {
        const response = await fetch(`/api/migration-status/${migrationJobId}`);
        const status = await response.json();

        updateMigrationUI(status);

        if (status.status === 'completed' || status.status === 'failed') {
            clearInterval(migrationInterval);
            showMigrationResult(status);
        }
    } catch (error) {
        console.error('Error checking status:', error);
    }
}

// Update Migration UI
function updateMigrationUI(status) {
    const { rows_extracted, rows_loaded, rows_failed, progress, message } = status;

    document.getElementById('rowsExtracted').textContent = (rows_extracted || 0).toLocaleString();
    document.getElementById('rowsLoaded').textContent = (rows_loaded || 0).toLocaleString();
    document.getElementById('rowsFailed').textContent = (rows_failed || 0).toLocaleString();

    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    document.getElementById('elapsedTime').textContent = formatTime(elapsed);

    document.getElementById('progressFill').style.width = `${progress || 0}%`;
    document.getElementById('progressPercent').textContent = `${progress || 0}%`;
    document.getElementById('progressText').textContent = message || 'Processing...';
}

// Show Migration Result
function showMigrationResult(status) {
    document.getElementById('migrationProgress').style.display = 'none';
    document.getElementById('migrationResult').style.display = 'block';
    document.getElementById('resetBtn').style.display = 'block';

    const isSuccess = status.status === 'completed';
    const icon = document.getElementById('resultIcon');
    const title = document.getElementById('resultTitle');
    const message = document.getElementById('resultMessage');

    if (isSuccess) {
        icon.innerHTML = '&#10003;';
        icon.style.color = 'var(--teal-accent)';
        title.textContent = 'Migration Completed';
        message.textContent = `${status.rows_loaded?.toLocaleString() || 0} records successfully loaded to PostgreSQL`;
    } else {
        icon.innerHTML = '&#10005;';
        icon.style.color = 'var(--error)';
        title.textContent = 'Migration Failed';
        message.textContent = status.error || 'An error occurred during the migration process';
    }
}

function showMigrationError(error) {
    clearInterval(migrationInterval);
    document.getElementById('migrationProgress').style.display = 'none';
    document.getElementById('migrationResult').style.display = 'block';
    document.getElementById('resetBtn').style.display = 'block';

    const icon = document.getElementById('resultIcon');
    icon.innerHTML = '&#10005;';
    icon.style.color = 'var(--error)';
    document.getElementById('resultTitle').textContent = 'Migration Failed';
    document.getElementById('resultMessage').textContent = error;
}

// Download Logs
async function downloadLogs() {
    if (!migrationJobId) return;

    try {
        const response = await fetch(`/api/migration-logs/${migrationJobId}`);
        const logs = await response.text();

        const blob = new Blob([logs], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `migration-${migrationJobId}.log`;
        a.click();
        URL.revokeObjectURL(url);
    } catch (error) {
        alert('Failed to download logs: ' + error.message);
    }
}

// Navigation
function goToStep(step) {
    const currentStepEl = document.querySelector('.step-content.active');
    const nextStepEl = document.getElementById(`step${step}`);

    if (currentStepEl) {
        currentStepEl.style.opacity = '0';
        currentStepEl.style.transform = 'translateY(-10px)';

        setTimeout(() => {
            currentStepEl.classList.remove('active');
            currentStepEl.style.opacity = '';
            currentStepEl.style.transform = '';

            activateNextStep(nextStepEl, step);
        }, 300);
    } else {
        activateNextStep(nextStepEl, step);
    }
}

function activateNextStep(el, step) {
    currentStep = step;
    el.classList.add('active');
    el.style.opacity = '0';
    el.style.transform = 'translateY(10px)';

    // Force reflow
    el.offsetHeight;

    el.style.opacity = '1';
    el.style.transform = 'translateY(0)';

    updateProgressTracker();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function updateProgressTracker() {
    document.querySelectorAll('.step-number').forEach((el, index) => {
        const stepNum = index + 1;
        el.classList.remove('active', 'completed');

        if (stepNum < currentStep) {
            el.classList.add('completed');
            el.textContent = '✓';
        } else if (stepNum === currentStep) {
            el.classList.add('active');
            el.textContent = stepNum;
        } else {
            el.textContent = stepNum;
        }
    });
}

function updateSummary() {
    const source = `${document.getElementById('projectId').value}.${document.getElementById('dataset').value}.${document.getElementById('table').value}`;
    const dest = `${document.getElementById('pgHost').value}:${document.getElementById('pgPort').value}/${document.getElementById('pgDatabase').value}.${document.getElementById('pgTable').value}`;
    const rows = document.getElementById('rowLimit').value || 'All records';

    document.getElementById('summarySource').textContent = source;
    document.getElementById('summaryDest').textContent = dest;
    document.getElementById('summaryRows').textContent = rows;
}

function resetMigration() {
    currentStep = 1;
    credentialsData = null;
    migrationJobId = null;

    document.getElementById('credentialsFile').value = '';
    document.getElementById('bigqueryForm').reset();
    document.getElementById('postgresForm').reset();

    document.getElementById('migrationProgress').style.display = 'none';
    document.getElementById('migrationResult').style.display = 'none';
    document.getElementById('startMigration').style.display = 'block';
    document.getElementById('backToConfig').style.display = 'block';
    document.getElementById('resetBtn').style.display = 'none';

    goToStep(1);
}

// Utility Functions
function showStatus(elementId, type, message) {
    const el = document.getElementById(elementId);
    el.className = `status-message ${type}`;
    el.textContent = message;
    el.style.display = 'flex';
}

function formatTime(seconds) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;

    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
}
