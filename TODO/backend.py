from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
import os
import config_todo as config
# PostgreSQL connection settings
DB_HOST = config.DB_HOST
DB_PORT = config.DB_PORT
DB_USER = config.DB_USER
DB_PASSWORD = config.DB_PASSWORD
DB_NAME = config.DB_DATABASE

app = FastAPI()

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection pool
async def get_db_connection():
    return await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Models
class Category(BaseModel):
    name: str

class Task(BaseModel):
    category_id: int
    title: str
    completed: bool = False

# Initialize database tables
@app.on_event("startup")
async def init_db():
    conn = await get_db_connection()
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            category_id INT REFERENCES categories(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            completed BOOLEAN DEFAULT FALSE
        );
        """
    )
    await conn.close()

# Routes
@app.post("/categories/")
async def create_category(category: Category):
    conn = await get_db_connection()
    try:
        await conn.execute("INSERT INTO categories (name) VALUES ($1)", category.name)
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=400, detail="Category already exists")
    finally:
        await conn.close()
    return {"message": "Category created successfully"}

@app.get("/categories/")
async def list_categories():
    conn = await get_db_connection()
    categories = await conn.fetch("SELECT * FROM categories")
    await conn.close()
    return categories

@app.delete("/categories/{category_id}")
async def delete_category(category_id: int):
    conn = await get_db_connection()
    result = await conn.execute("DELETE FROM categories WHERE id = $1", category_id)
    await conn.close()
    return {"message": "Category deleted successfully"}

@app.post("/tasks/")
async def create_task(task: Task):
    conn = await get_db_connection()
    await conn.execute(
        "INSERT INTO tasks (category_id, title, completed) VALUES ($1, $2, $3)",
        task.category_id, task.title, task.completed
    )
    await conn.close()
    return {"message": "Task added successfully"}

@app.get("/tasks/")
async def list_tasks():
    conn = await get_db_connection()
    tasks = await conn.fetch("SELECT * FROM tasks")
    await conn.close()
    return tasks

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: int):
    conn = await get_db_connection()
    result = await conn.execute("DELETE FROM tasks WHERE id = $1", task_id)
    await conn.close()
    return {"message": "Task deleted successfully"}

@app.put("/tasks/{task_id}")
async def update_task(task_id: int, completed: bool):
    conn = await get_db_connection()
    await conn.execute("UPDATE tasks SET completed = $1 WHERE id = $2", completed, task_id)
    await conn.close()
    return {"message": "Task updated successfully"}

# Run with: uvicorn filename:app --reload
