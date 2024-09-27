from datetime import date, datetime
from typing import Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()


class Item(BaseModel):
    name: str
    description: str | None = None
    price: float
    tax: float | None = None
    tags: list[str] = []
    properties: dict[str, str] = {}
    createdAt: datetime = None
    validUntil: date = None
    revisedAt: list[date] = []


items = {
    1: Item(name="Portal Gun", price=42.0, tags=["sci-fi"], revisedAt=["2007-10-10", "2022-12-08"]),
    2: Item(name="Plumbus", price=32.0, validUntil="2999-01-01", properties={"feeble": "schleem"}),
}


@app.get("/")
def root():
    return {"Hello": "World"}


@app.get("/search")
def search_items(q: Union[str, None] = None) -> list[Item]:
    return items.values()


@app.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    return items[item_id]
