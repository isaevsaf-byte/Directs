# This file acts as a proxy entrypoint for deployment platforms
# that cannot find the app deep inside src/api/main.py

from src.api.main import app

# Optional: Allow running locally with `python main.py`
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
