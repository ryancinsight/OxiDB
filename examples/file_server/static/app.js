// Global state
let authToken = localStorage.getItem('authToken');
let currentUser = null;

// API base URL
const API_BASE = '/api';

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    if (authToken) {
        checkAuth();
    }
    
    // Setup form handlers
    document.getElementById('login-form').addEventListener('submit', handleLogin);
    document.getElementById('register-form').addEventListener('submit', handleRegister);
    document.getElementById('upload-form').addEventListener('submit', handleUpload);
    document.getElementById('share-form').addEventListener('submit', handleShare);
});

// Auth functions
async function handleLogin(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = {
        username: formData.get('username'),
        password: formData.get('password')
    };
    
    try {
        const response = await fetch(`${API_BASE}/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            const result = await response.json();
            authToken = result.token;
            localStorage.setItem('authToken', authToken);
            currentUser = result.user;
            showFileManager();
        } else {
            const error = await response.json();
            document.getElementById('login-error').textContent = error.error || 'Login failed';
        }
    } catch (err) {
        document.getElementById('login-error').textContent = 'Network error';
    }
}

async function handleRegister(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = {
        username: formData.get('username'),
        email: formData.get('email'),
        password: formData.get('password')
    };
    
    try {
        const response = await fetch(`${API_BASE}/auth/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            document.getElementById('register-error').textContent = '';
            alert('Registration successful! Please login.');
            showTab('login');
            e.target.reset();
        } else {
            const error = await response.json();
            document.getElementById('register-error').textContent = error.error || 'Registration failed';
        }
    } catch (err) {
        document.getElementById('register-error').textContent = 'Network error';
    }
}

async function logout() {
    try {
        await fetch(`${API_BASE}/auth/logout`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${authToken}` }
        });
    } catch (err) {
        console.error('Logout error:', err);
    }
    
    authToken = null;
    currentUser = null;
    localStorage.removeItem('authToken');
    showAuthForm();
}

async function checkAuth() {
    try {
        const response = await fetch(`${API_BASE}/users/me`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });
        
        if (response.ok) {
            currentUser = await response.json();
            showFileManager();
        } else {
            authToken = null;
            localStorage.removeItem('authToken');
            showAuthForm();
        }
    } catch (err) {
        console.error('Auth check error:', err);
        showAuthForm();
    }
}

// UI functions
function showTab(tab) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.auth-form').forEach(form => form.classList.add('hidden'));
    
    if (tab === 'login') {
        document.querySelector('.tab-btn:first-child').classList.add('active');
        document.getElementById('login-form').classList.remove('hidden');
    } else {
        document.querySelector('.tab-btn:last-child').classList.add('active');
        document.getElementById('register-form').classList.remove('hidden');
    }
}

function showAuthForm() {
    document.getElementById('auth-container').classList.remove('hidden');
    document.getElementById('file-manager').classList.add('hidden');
    document.getElementById('user-info').classList.add('hidden');
}

function showFileManager() {
    document.getElementById('auth-container').classList.add('hidden');
    document.getElementById('file-manager').classList.remove('hidden');
    document.getElementById('user-info').classList.remove('hidden');
    document.getElementById('username').textContent = currentUser.username;
    loadFiles();
}

// File operations
async function handleUpload(e) {
    e.preventDefault();
    
    const fileInput = document.getElementById('file-input');
    const file = fileInput.files[0];
    if (!file) return;
    
    const formData = new FormData();
    formData.append('file', file);
    
    const progressBar = document.getElementById('upload-progress');
    const progressFill = progressBar.querySelector('.progress-fill');
    progressBar.classList.remove('hidden');
    
    try {
        const response = await fetch(`${API_BASE}/files`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${authToken}` },
            body: formData
        });
        
        if (response.ok) {
            progressFill.style.width = '100%';
            setTimeout(() => {
                progressBar.classList.add('hidden');
                progressFill.style.width = '0%';
                fileInput.value = '';
                loadFiles();
            }, 500);
        } else {
            const error = await response.json();
            alert(error.error || 'Upload failed');
            progressBar.classList.add('hidden');
            progressFill.style.width = '0%';
        }
    } catch (err) {
        alert('Network error');
        progressBar.classList.add('hidden');
        progressFill.style.width = '0%';
    }
}

async function loadFiles() {
    const includeShared = document.getElementById('show-shared').checked;
    
    try {
        const response = await fetch(`${API_BASE}/files?include_shared=${includeShared}`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });
        
        if (response.ok) {
            const data = await response.json();
            displayFiles(data);
        } else {
            console.error('Failed to load files');
        }
    } catch (err) {
        console.error('Network error:', err);
    }
}

function displayFiles(data) {
    const filesList = document.getElementById('files-list');
    filesList.innerHTML = '';
    
    // Display owned files
    data.owned_files.forEach(file => {
        filesList.appendChild(createFileElement(file, true));
    });
    
    // Display shared files
    if (data.shared_files) {
        data.shared_files.forEach(item => {
            const fileElement = createFileElement(item.file, false);
            const badge = document.createElement('span');
            badge.className = 'shared-badge';
            badge.textContent = `Shared by ${item.owner}`;
            fileElement.querySelector('.file-info').appendChild(badge);
            filesList.appendChild(fileElement);
        });
    }
    
    if (data.owned_files.length === 0 && (!data.shared_files || data.shared_files.length === 0)) {
        filesList.innerHTML = '<p style="text-align: center; color: #7f8c8d;">No files found</p>';
    }
}

function createFileElement(file, isOwner) {
    const div = document.createElement('div');
    div.className = 'file-item';
    
    const fileInfo = document.createElement('div');
    fileInfo.className = 'file-info';
    fileInfo.innerHTML = `
        <h3>${file.original_name}</h3>
        <div class="file-meta">
            ${formatFileSize(file.size)} • Uploaded ${formatDate(file.uploaded_at)}
        </div>
    `;
    
    const actions = document.createElement('div');
    actions.className = 'file-actions';
    
    // Download button
    const downloadBtn = document.createElement('button');
    downloadBtn.className = 'download-btn';
    downloadBtn.textContent = 'Download';
    downloadBtn.onclick = () => downloadFile(file.id, file.original_name);
    actions.appendChild(downloadBtn);
    
    if (isOwner) {
        // Share button
        const shareBtn = document.createElement('button');
        shareBtn.className = 'share-btn';
        shareBtn.textContent = 'Share';
        shareBtn.onclick = () => openShareModal(file.id);
        actions.appendChild(shareBtn);
        
        // Delete button
        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'delete-btn';
        deleteBtn.textContent = 'Delete';
        deleteBtn.onclick = () => deleteFile(file.id);
        actions.appendChild(deleteBtn);
    }
    
    div.appendChild(fileInfo);
    div.appendChild(actions);
    
    return div;
}

async function downloadFile(fileId, filename) {
    try {
        const response = await fetch(`${API_BASE}/files/${fileId}/download`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } else {
            alert('Failed to download file');
        }
    } catch (err) {
        alert('Network error');
    }
}

async function deleteFile(fileId) {
    if (!confirm('Are you sure you want to delete this file?')) return;
    
    try {
        const response = await fetch(`${API_BASE}/files/${fileId}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${authToken}` }
        });
        
        if (response.ok) {
            loadFiles();
        } else {
            alert('Failed to delete file');
        }
    } catch (err) {
        alert('Network error');
    }
}

// Share functions
function openShareModal(fileId) {
    document.getElementById('share-file-id').value = fileId;
    document.getElementById('share-username').value = '';
    document.getElementById('share-permissions').value = 'read';
    document.getElementById('share-error').textContent = '';
    document.getElementById('share-modal').classList.remove('hidden');
}

function closeShareModal() {
    document.getElementById('share-modal').classList.add('hidden');
}

async function handleShare(e) {
    e.preventDefault();
    
    const fileId = document.getElementById('share-file-id').value;
    const username = document.getElementById('share-username').value;
    const permissions = document.getElementById('share-permissions').value;
    
    try {
        const response = await fetch(`${API_BASE}/files/${fileId}/share`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ file_id: fileId, username, permissions })
        });
        
        if (response.ok) {
            closeShareModal();
            alert('File shared successfully!');
        } else {
            const error = await response.json();
            document.getElementById('share-error').textContent = error.error || 'Failed to share file';
        }
    } catch (err) {
        document.getElementById('share-error').textContent = 'Network error';
    }
}

// Utility functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}