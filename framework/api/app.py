from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import os
import json

import os
import joblib

MODEL_PATH = os.getenv("MODEL_PATH", "/app/mlops_artifacts/model.pkl")



app = FastAPI(title="MLOps API")


model = joblib.load(MODEL_PATH)

class Customer(BaseModel):
    age: int
    country: str
    has_email: bool

@app.post("/predict")
def predict(payload: Customer):
    # ejemplo: features simples
    X = [[payload.age, 1 if payload.country == "PE" else 0, 1 if payload.has_email else 0]]
    pred = model.predict(X)[0]
    proba = float(model.predict_proba(X)[0][1]) if hasattr(model, "predict_proba") else None
    return {"prediction": int(pred), "probability": proba}
