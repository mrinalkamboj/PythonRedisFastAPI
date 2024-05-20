from fastapi import FastAPI
from adavisual import router as adavisual_router


app = FastAPI()
app.include_router(adavisual_router, prefix="/av")

@app.get("/")
def fast_api_welcome():
    return "Hello! Welcome to the World of FastAPI "

# Run the server with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)