from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os
import time
from pathlib import Path
import uvicorn

from nl_to_cypher import NLToCypherConverter

app = FastAPI(title="F1 Temporal Knowledge Graph", version="1.0.0")

# Set up templates directory
templates = Jinja2Templates(directory="templates")

# Initialize the query converter
converter = None

def get_converter():
    global converter
    if converter is None:
        converter = NLToCypherConverter(
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )
    return converter

@app.on_event("startup")
async def startup_event():
    print("Starting F1 Knowledge Graph API...")
    print("Connecting to Neo4j...")

    # Test the connection
    try:
        conv = get_converter()
        # Try to get basic stats
        with conv.driver.session() as session:
            session.run("MATCH (n) RETURN count(n) LIMIT 1")
        print("Neo4j connection successful")
    except Exception as e:
        print(f"Neo4j connection failed: {e}")
        print("Make sure Neo4j Desktop is running on localhost:7687")

@app.on_event("shutdown")
async def shutdown_event():
    global converter
    if converter:
        converter.close()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        conv = get_converter()
    except Exception:
        stats = None

    return templates.TemplateResponse("index.html", {
        "request": request,
    })

@app.post("/", response_class=HTMLResponse)
async def ask_question(request: Request, question: str = Form(...)):
    start_time = time.time()

    try:
        conv = get_converter()
        result = conv.answer_question(question, use_llm=True)
        query_time = time.time() - start_time

        return templates.TemplateResponse("index.html", {
            "request": request,
            "question": question,
            "result": result,
            "query_time": query_time,
        })

    except Exception as e:
        error_result = {
            "answer": f"Error processing question: {str(e)}",
            "approach": "error",
            "cypher": None,
            "results": []
        }

        return templates.TemplateResponse("index.html", {
            "request": request,
            "question": question,
            "result": error_result,
            "query_time": 0,
        })

@app.get("/api/ask")
async def api_ask(question: str):
    start_time = time.time()

    try:
        conv = get_converter()
        result = conv.answer_question(question, use_llm=True)
        query_time = time.time() - start_time

        return {
            "question": question,
            "answer": result["answer"],
            "cypher": result["cypher"],
            "approach": result["approach"],
            "query_time_ms": round(query_time * 1000),
            "success": True
        }

    except Exception as e:
        return {
            "question": question,
            "error": str(e),
            "success": False
        }

if __name__ == "__main__":
    print("Starting F1 Temporal Knowledge Graph Web Interface")
    print("Make sure you have:")
    print("   - Neo4j Desktop running (localhost:7687)")
    print("   - OPENAI_API_KEY environment variable set")
    print("   - Data loaded into Neo4j")
    print()
    print("Starting server at http://localhost:8000")

    uvicorn.run(app, host="0.0.0.0", port=8000)
