"""
Minimal embedding service: hosts sentence-transformers/all-MiniLM-L6-v2 (384 dims, CPU).
POST /embed  {texts: [str, ...]}  →  {vectors: [[float, ...], ...]}
POST /health                      →  {ok: true}
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("embed")
logging.basicConfig(level=logging.INFO)

MODEL_NAME = os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
MAX_TEXTS = int(os.getenv("EMBED_MAX_TEXTS", "256"))

model: SentenceTransformer = None  # loaded in lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    global model
    logger.info("loading model %s", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)
    logger.info("model loaded, dims=%d", model.get_sentence_embedding_dimension())
    yield
    model = None


app = FastAPI(lifespan=lifespan)


class EmbedRequest(BaseModel):
    texts: List[str]


class EmbedResponse(BaseModel):
    vectors: List[List[float]]


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    if len(req.texts) == 0:
        return EmbedResponse(vectors=[])
    if len(req.texts) > MAX_TEXTS:
        raise HTTPException(status_code=400, detail=f"max {MAX_TEXTS} texts per request")
    if model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    vecs = model.encode(req.texts, normalize_embeddings=True).tolist()
    return EmbedResponse(vectors=vecs)


@app.get("/health")
def health():
    return {"ok": model is not None}
