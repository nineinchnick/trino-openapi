import logging
from datetime import date, datetime
from typing import Callable

from fastapi import APIRouter, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel
from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse


def log_info(req_body, res_body):
    logging.info(f"Request: {req_body}")
    logging.info(f"Response: {res_body}")


class LoggingRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            req_body = await request.body()
            response = await original_route_handler(request)
            tasks = response.background

            if isinstance(response, StreamingResponse):
                chunks = []
                async for chunk in response.body_iterator:
                    chunks.append(chunk)
                res_body = b"".join(chunks)

                task = BackgroundTask(log_info, req_body, res_body)
                response = Response(
                    content=res_body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )
            else:
                task = BackgroundTask(log_info, req_body, response.body)

            # check if the original response had background tasks already attached to it
            if tasks:
                tasks.add_task(task)  # add the new task to the tasks list
                response.background = tasks
            else:
                response.background = task

            return response

        return custom_route_handler


app = FastAPI()
router = APIRouter(route_class=LoggingRoute)
logging.basicConfig(format="%(asctime)s %(levelname)s:%(name)s:%(message)s", level=logging.DEBUG)


class UnicornException(Exception):
    def __init__(self, name: str, *args: object):
        super().__init__(*args)
        self.name = name


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


@router.get("/")
def root():
    return {"Hello": "World"}


@router.post("/search")
def search_items(q: ItemFilter) -> list[Item]:
    return filter(lambda item: item.id in q.item_ids, items.values())


@router.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    return items[item_id]


@router.get("/error")
def error() -> Item:
    raise UnicornException(name="Inevitable error")


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=418,
        content={"message": f"Oops! {exc.name} happened. There goes a rainbow..."},
    )

app.include_router(router)
