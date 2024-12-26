from datetime import date, datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    id: str
    name: str
    description: str | None = None
    price: float
    tax: float | None = None
    tags: list[str] = []
    properties: dict[str, str] = {}
    createdAt: datetime = None
    validUntil: date = None
    revisedAt: list[date] = []

class ItemFilter(BaseModel):
    item_ids: list[str] = []

items = {
    1: Item(id="1", name="Portal Gun", price=42.0, tags=["sci-fi"], revisedAt=["2007-10-10", "2022-12-08"]),
    2: Item(id="2", name="Plumbus", price=32.0, validUntil="2999-01-01", properties={"feeble": "schleem"}),
}

@app.get("/")
def root():
    return {"Hello": "World"}

@app.post("/search")
def search_items(q: ItemFilter) -> list[Item]:
    return filter(lambda item: item.id in q.item_ids,items.values())

@app.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    return items[item_id]
