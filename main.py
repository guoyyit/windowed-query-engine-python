"""服务入口：uvicorn main:app"""
from windowed.service import create_app

app = create_app()
