from fastapi import FastAPI

app = FastAPI(title="Globant Data Engineering Challenge - API")

@app.get("/")
async def root():
    return {"status": "ok", "message": "Hola Jose Luis — API lista"}

@app.get("/health")
async def health():
    return {"status": "healthy"}
