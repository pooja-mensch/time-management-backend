"""
FastAPI Server for Document Processing
Web API interface for the document processing pipeline.
"""

import os
import tempfile
import shutil
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncio
import uuid

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Form
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from document_processor import DocumentProcessor, ProcessingError, get_processing_summary


# Pydantic models for API requests/responses
class ProcessingOptions(BaseModel):
    anonymize: bool = True
    restructure: bool = True
    save_results: bool = True


class ProcessingStatus(BaseModel):
    task_id: str
    status: str  # 'pending', 'processing', 'completed', 'failed'
    progress: str
    created_at: str
    completed_at: Optional[str] = None
    error_message: Optional[str] = None


class ServiceStatus(BaseModel):
    extractor_available: bool
    anonymizer_available: bool
    restructurer_available: bool
    processing_stats: Dict[str, Any]


# Initialize FastAPI app
app = FastAPI(
    title="Document Processing API",
    description="API for extracting, anonymizing, and restructuring document content",
    version="1.0.0"
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global processor instance
processor = DocumentProcessor(
    spacy_model="de_core_news_sm",
    llm_base_url=os.getenv('LLM_BASE_URL', 'http://localhost:1234/v1/chat/completions'),
    llm_model=os.getenv('LLM_MODEL', 'phi-3-mini-4k-instruct')
)

# In-memory storage for processing tasks (use Redis/DB for production)
processing_tasks = {}
results_storage = {}


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Document Processing API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": processor.get_service_status()
    }


@app.get("/status", response_model=ServiceStatus)
async def get_service_status():
    """Get the status of all processing services."""
    status = processor.get_service_status()
    return ServiceStatus(
        extractor_available=status['extractor']['available'],
        anonymizer_available=status['anonymizer']['available'],
        restructurer_available=status['restructurer']['available'],
        processing_stats=status['processing_stats']
    )


@app.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    password: Optional[str] = Form(None),
    anonymize: bool = Form(True),
    restructure: bool = Form(True)
):
    """
    Upload and process a document file.

    Returns a task ID that can be used to check processing status.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    file_extension = os.path.splitext(file.filename)[1].lower()
    supported_extensions = ['.pdf', '.xlsx', '.xls']
    if file_extension not in supported_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Supported: {', '.join(supported_extensions)}"
        )

    task_id = str(uuid.uuid4())

    processing_tasks[task_id] = ProcessingStatus(
        task_id=task_id,
        status='pending',
        progress='File uploaded, queued for processing',
        created_at=datetime.now().isoformat()
    )

    # Save file immediately (to avoid closed file issue)
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)

    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Queue background task using only the path
    background_tasks.add_task(
        process_file_background,
        task_id=task_id,
        file_path=temp_file_path,
        password=password,
        anonymize=anonymize,
        restructure=restructure
    )

    return {
        "task_id": task_id,
        "status": "queued",
        "message": "File uploaded successfully, processing started"
    }


async def process_file_background(
    task_id: str,
    file_path: str,
    password: Optional[str],
    anonymize: bool,
    restructure: bool
):
    try:
        processing_tasks[task_id].status = 'processing'
        processing_tasks[task_id].progress = 'Processing document'

        result = processor.process_document(
            file_path=file_path,
            password=password,
            anonymize=anonymize,
            restructure=restructure
        )

        results_storage[task_id] = result
        processing_tasks[task_id].status = 'completed'
        processing_tasks[task_id].progress = 'Processing completed successfully'
        processing_tasks[task_id].completed_at = datetime.now().isoformat()

    except Exception as e:
        processing_tasks[task_id].status = 'failed'
        processing_tasks[task_id].progress = 'Processing failed'
        processing_tasks[task_id].error_message = str(e)
        processing_tasks[task_id].completed_at = datetime.now().isoformat()

    finally:
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)

@app.get("/task/{task_id}/status", response_model=ProcessingStatus)
async def get_task_status(task_id: str):
    """Get the status of a processing task."""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return processing_tasks[task_id]


@app.get("/task/{task_id}/result")
async def get_task_result(task_id: str):
    """Get the processing result for a completed task."""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = processing_tasks[task_id]
    
    if task.status == 'processing' or task.status == 'pending':
        raise HTTPException(status_code=202, detail="Task still processing")
    
    if task.status == 'failed':
        raise HTTPException(status_code=500, detail=f"Task failed: {task.error_message}")
    
    if task_id not in results_storage:
        raise HTTPException(status_code=404, detail="Results not found")
    
    return results_storage[task_id]


@app.get("/task/{task_id}/summary")
async def get_task_summary(task_id: str):
    """Get a human-readable summary of processing results."""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = processing_tasks[task_id]
    
    if task.status != 'completed':
        raise HTTPException(status_code=400, detail="Task not completed")
    
    if task_id not in results_storage:
        raise HTTPException(status_code=404, detail="Results not found")
    
    result = results_storage[task_id]
    summary = get_processing_summary(result)
    
    return {
        "task_id": task_id,
        "summary": summary,
        "phases_completed": result['phases_completed'],
        "errors": result['errors'],
        "warnings": result['warnings']
    }


@app.post("/process-url")
async def process_from_url(
    background_tasks: BackgroundTasks,
    url: str,
    password: Optional[str] = None,
    anonymize: bool = True,
    restructure: bool = True
):
    """Process a document from a URL (for future implementation)."""
    raise HTTPException(
        status_code=501, 
        detail="URL processing not implemented yet"
    )


@app.get("/tasks")
async def list_tasks():
    """List all processing tasks."""
    return {
        "tasks": list(processing_tasks.values()),
        "total": len(processing_tasks)
    }


@app.delete("/task/{task_id}")
async def delete_task(task_id: str):
    """Delete a processing task and its results."""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Remove from both storages
    processing_tasks.pop(task_id, None)
    results_storage.pop(task_id, None)
    
    return {"message": f"Task {task_id} deleted successfully"}


@app.post("/reset-stats")
async def reset_processing_stats():
    """Reset processing statistics."""
    processor.reset_stats()
    return {"message": "Processing statistics reset successfully"}


# Development endpoints
@app.get("/dev/test-services")
async def test_services():
    """Test all services (development endpoint)."""
    try:
        return {
            "extractor": {
                "available": processor.extractor.is_available()
            },
            "anonymizer": {
                "available": processor.anonymizer.is_available()
            },
            "restructurer": {
                "available": processor.restructurer.is_available()
            },
            "llm_url": processor.llm_base_url,
            "llm_model": processor.llm_model
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Service check failed: {str(e)}")