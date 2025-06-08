from fastapi import FastAPI, Request, HTTPException
import logging
import requests
import time

app = FastAPI()
logging.basicConfig(level=logging.DEBUG)

@app.post("/api/embed")
async def embed(request: Request):
    body = await request.json()
    logging.debug(f"Received request body: {body}")

    model = body.get("model")
    input_text = body.get("input")

    if not model or not input_text:
        raise HTTPException(status_code=400, detail="Missing model or input")

    try:
        start_time = time.time()
        response = requests.post("http://localhost:11434/api/embed", json={
            "model": model,
            "input": input_text
        })
        duration = time.time() - start_time

        response.raise_for_status()
        data = response.json()

        embeddings_list = data.get("embedding") or data.get("embeddings")
        if not embeddings_list or not isinstance(embeddings_list, list):
            raise ValueError("Invalid embeddings format returned")

        # Ensure it's nested as [[...]]
        embedding = [embeddings_list] if isinstance(embeddings_list[0], float) else embeddings_list

        logging.debug(f"Received embedding: {embedding}")
    except Exception as e:
        logging.error(f"Error calling embedding API: {e}")
        raise HTTPException(status_code=500, detail="Embedding service error")

    return {
        "model": model,
        "embeddings": embedding,
        "total_duration": int(duration * 1_000_000),  # microseconds
        "load_duration": 1019500,  # You can dynamically get this if Ollama supports it
        "prompt_eval_count": len(input_text.split())  # Approximate token count
    }
