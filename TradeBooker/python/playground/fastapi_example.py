import utils.fastapi_utils as fau
import fastapi as fastapi

import utils.env_utils as envu
logger = envu.get_logger()

app: fastapi.FastAPI = fau.create_fastapi()
@app.get("/")
async def read_root():
    logger.info("Root endpoint called")
    return {"Hello": "World"}

@app.get("/hello")
async def read_hello():
    logger.info("Hello endpoint called")
    return {"message": "Hello, FastAPI!"}

@app.get("/goodbye")
async def read_goodbye():
    logger.info("Goodbye endpoint called")
    return {"message": "Goodbye, FastAPI!"}

if __name__ == "__main__":
    import utils.fastapi_utils as fau
    fau.run_server(callerFastAPIInstanceName='app')