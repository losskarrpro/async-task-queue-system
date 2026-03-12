from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from api.routes import tasks, queue, monitoring

app = FastAPI(
    title="Async Task Queue System API",
    description="API for managing asynchronous task queues",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(tasks.router, prefix="/api/v1/tasks", tags=["tasks"])
app.include_router(queue.router, prefix="/api/v1/queue", tags=["queue"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])


@app.get("/")
def read_root():
    return {"message": "Async Task Queue System API is running"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}


def create_app() -> FastAPI:
    """Factory function to create the FastAPI app"""
    return app


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7501)