import logging
from datetime import date, datetime
from typing import Annotated, Callable

from fastapi import APIRouter, Cookie, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field
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


class Category(BaseModel):
    id: str
    name: str


class Item(BaseModel):
    id: str
    name: str
    description: str = None
    price: float
    tax: float = None
    tags: list[str] = []
    categories: list[Category] = []
    properties: dict[str, str] = {}
    createdAt: datetime = None
    validUntil: date = None
    revisedAt: list[date] = []


class ItemFilter(BaseModel):
    item_ids: list[str] = []


class Note(BaseModel):
    id: int
    text: str


class NoteBody(BaseModel):
    text: str


class HasMoreResults(BaseModel):
    results: list[Item]
    has_more: bool


class TotalPagesResults(BaseModel):
    results: list[Item]
    total_pages: int


class PageFilter(BaseModel):
    page: int = 1


class DecimalText(BaseModel):
    decimal_text: str


class Echo(BaseModel):
    path: str
    query: str
    header: str | None = None
    cookie: str | None = None


class UnionWrapper(BaseModel):
    name: str
    value: int | dict[str, int]


class UnionEcho(BaseModel):
    wrapper: UnionWrapper


class MapEcho(BaseModel):
    labels: dict[str, int]


class UnionValueEcho(BaseModel):
    value: int | dict[str, int]


class Dimensions(BaseModel):
    width: int
    height: int
    unit: str


# pydantic doesn't set formats of numbers, so they're declared explicitly
class Gadget(BaseModel):
    in_stock: bool
    serial_number: Annotated[int, Field(json_schema_extra={"format": "int64"})]
    weight: Annotated[float, Field(json_schema_extra={"format": "float"})]
    density: Annotated[float, Field(json_schema_extra={"format": "double"})]
    dimensions: Dimensions


class Results(BaseModel):
    results: list[Item]
    total_results: int


items = {
    1: Item(id="1", name="Portal Gun", price=42.0, tags=["sci-fi"], revisedAt=[date.fromisoformat("2007-10-10"), date.fromisoformat("2022-12-08")], categories=[Category(id="1", name="main")]),
    2: Item(id="2", name="Plumbus", price=32.0, validUntil=date.fromisoformat("2999-01-01"), properties={"feeble": "schleem"}),
}


notes: dict[int, Note] = {}

# kept as raw JSON, because Python floats can't hold all decimal values exactly
numbers = [
    '{"id": 1, "real_value": -1.5, "decimal_value": 1234567890.12345678, '
    '"real_values": [1.5, -2.25], "decimal_values": [0.00000001, -9999999999.99999999], '
    '"decimal_map": {"a": 1234567890.12345678}}',
]

# the connector ignores the format of OpenAPI 3.1 number schemas, so REAL columns use the non-standard float type
NUMBERS_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "real_value": {"type": "float"},
        "decimal_value": {"type": "number"},
        "real_values": {"type": "array", "items": {"type": "float"}},
        "decimal_values": {"type": "array", "items": {"type": "number"}},
        "decimal_map": {"type": "object", "additionalProperties": {"type": "number"}},
    },
}


@router.get("/")
def root():
    return {"Hello": "World"}


@router.post("/search")
def search_items(q: ItemFilter) -> list[Item]:
    return list(filter(lambda item: item.id in q.item_ids, items.values()))


@router.get("/item_categories")
def item_categories() -> Results:
    return Results(
        results=list(items.values()),
        total_results=len(items))


@router.get("/items")
def list_item(page: int = None, per_page: int = None) -> Results:
    if page is None:
        page = 1
    if per_page is None:
        per_page = 1
    offset = (page - 1) * per_page
    return Results(
        results=list(items.values())[offset:offset+per_page],
        total_results=len(items))


@router.get("/clamped_items")
def list_clamped_items(page: int = 1) -> Results:
    # out of range page numbers return the last page, so only the total number of results can end pagination
    page = min(max(page, 1), len(items))
    return Results(
        results=list(items.values())[page - 1:page],
        total_results=len(items))


@router.get("/clamped_sized_items")
def list_clamped_sized_items(page: int = 1, size: int = 3) -> Results:
    # out of range page numbers return the last page, and the total is not configured in the spec,
    # so only a page with fewer items than requested can end pagination
    last_page = max(1, -(-len(items) // size))
    page = min(max(page, 1), last_page)
    return Results(
        results=list(items.values())[(page - 1) * size:page * size],
        total_results=len(items))


@router.get("/capped_items")
def list_capped_items(page: int = 1, size: int = 1) -> Results:
    # return at most one item per page, even when asked for more
    size = min(size, 1)
    return Results(
        results=list(items.values())[(page - 1) * size:page * size],
        total_results=len(items))


# the following endpoints return the last page for out of range page numbers,
# so each one needs a different signal to end pagination
@router.get("/has_more_items")
def list_has_more_items(page: int = 1) -> HasMoreResults:
    page = min(max(page, 1), len(items))
    return HasMoreResults(
        results=list(items.values())[page - 1:page],
        has_more=page < len(items))


@router.get("/total_pages_items")
def list_total_pages_items(page: int = 1) -> TotalPagesResults:
    page = min(max(page, 1), len(items))
    return TotalPagesResults(
        results=list(items.values())[page - 1:page],
        total_pages=len(items))


@router.get("/max_pages_items")
def list_max_pages_items(page: int = 1) -> Results:
    page = min(max(page, 1), len(items))
    return Results(
        results=list(items.values())[page - 1:page],
        total_results=len(items))


@router.post("/paged_search")
def paged_search(q: PageFilter) -> Results:
    return Results(
        results=list(items.values())[q.page - 1:q.page],
        total_results=len(items))


@router.get("/items_by_ids")
def items_by_ids(item_ids: Annotated[list[int], Query()]) -> list[Item]:
    return [items[item_id] for item_id in item_ids if item_id in items]


@router.get("/items_by_delimited_ids")
def items_by_delimited_ids(item_ids: str) -> list[Item]:
    return [items[int(item_id)] for item_id in item_ids.split(",") if int(item_id) in items]


@router.get("/items_by_header")
def items_by_header(x_item_ids: Annotated[str, Header()]) -> list[Item]:
    return [items[int(item_id)] for item_id in x_item_ids.split(",") if int(item_id) in items]


@router.get("/items_by_cookie")
def items_by_cookie(item_id: Annotated[int, Cookie()]) -> list[Item]:
    return [items[item_id]] if item_id in items else []


@router.get("/notes")
def list_notes() -> list[Note]:
    return list(notes.values())


@router.get("/notes/{note_id}")
def get_note(note_id: int) -> Note:
    if note_id not in notes:
        raise HTTPException(status_code=404, detail="Note not found")
    return notes[note_id]


@router.post("/notes/{note_id}", status_code=201)
def create_note(note_id: int, body: NoteBody) -> Note:
    if note_id in notes:
        raise HTTPException(status_code=409, detail="Note already exists")
    notes[note_id] = Note(id=note_id, text=body.text)
    return notes[note_id]


@router.put("/notes/{note_id}", status_code=202)
def update_note(note_id: int, body: NoteBody) -> Note:
    if note_id not in notes:
        raise HTTPException(status_code=404, detail="Note not found")
    notes[note_id] = Note(id=note_id, text=body.text)
    return notes[note_id]


@router.delete("/notes/{note_id}", status_code=204)
def delete_note(note_id: int):
    if note_id not in notes:
        raise HTTPException(status_code=404, detail="Note not found")
    del notes[note_id]


@router.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    return items[item_id]


@router.get("/numbers")
def list_numbers() -> Response:
    return Response(content="[" + ",".join(numbers) + "]", media_type="application/json")


@router.post("/numbers")
async def create_numbers(request: Request) -> Response:
    numbers.append((await request.body()).decode())
    return Response(content="{}", media_type="application/json")


@router.post("/echo_numbers")
async def echo_numbers(request: Request) -> Response:
    return Response(content=await request.body(), media_type="application/json")


@router.post("/echo_union")
def echo_union(body: UnionEcho) -> UnionEcho:
    # validating the body rejects union values sent as quoted JSON strings
    return body


@router.post("/echo_map")
def echo_map(body: MapEcho) -> MapEcho:
    return body


@router.post("/echo_union_value")
def echo_union_value(body: UnionValueEcho) -> UnionValueEcho:
    # validating the body rejects union values sent as quoted JSON strings
    return body


@router.post("/echo_gadget")
def echo_gadget(body: Gadget) -> Gadget:
    return body


@router.get("/decimal_parameters")
def decimal_parameters(decimal_value: str) -> DecimalText:
    # returns the exact text of the parameter, to check how it was formatted
    return DecimalText(decimal_text=decimal_value)


# parameter serialization is described only in echo.yml, since FastAPI can't parse most parameter styles;
# the path converter also matches values with encoded slashes, because they're decoded before routing
@router.get("/echo/{rest:path}")
def echo(request: Request, rest: str) -> Echo:
    # return the request exactly as it was received, before any percent-decoding
    return Echo(
        path=request.scope["raw_path"].decode(),
        query=request.scope["query_string"].decode(),
        header=request.headers.get("x-color"),
        cookie=request.headers.get("cookie"))


last_posted_echo = None


@router.post("/echo/{rest:path}")
def echo_post(request: Request, rest: str) -> dict:
    # responses of writes are ignored, so save the request to check it using /echo_last_post
    global last_posted_echo
    last_posted_echo = echo(request, rest)
    return {}


@router.get("/echo_last_post")
def echo_last_post() -> Echo:
    return last_posted_echo


@router.get("/error")
def error() -> Item:
    raise UnicornException(name="Inevitable error")


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=418,
        content={"message": f"Oops! {exc.name} happened. There goes a rainbow..."},
    )


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="FastAPI",
        version="0.1.0",
        routes=app.routes,
    )
    openapi_schema["paths"]["/error"]["get"]["x-trino"] = {
        "errorPath": "$response.body#/message"
    }
    openapi_schema["paths"]["/item_categories"]["get"]["x-trino"] = {
        "resultsPath": "$response.body#/results/0/categories"
    }
    openapi_schema["paths"]["/items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "per_page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
    }
    openapi_schema["paths"]["/clamped_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
    }
    openapi_schema["paths"]["/clamped_sized_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "size",
        "resultsPath": "$response.body#/results",
    }
    openapi_schema["paths"]["/capped_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "size",
        "resultsPath": "$response.body#/results",
    }
    capped_size = next(parameter for parameter in openapi_schema["paths"]["/capped_items"]["get"]["parameters"] if parameter["name"] == "size")
    capped_size["schema"]["maximum"] = 1
    openapi_schema["paths"]["/has_more_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "hasMorePath": "$response.body#/has_more",
    }
    openapi_schema["paths"]["/total_pages_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalPagesPath": "$response.body#/total_pages",
    }
    openapi_schema["paths"]["/max_pages_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "maxPages": len(items),
    }
    openapi_schema["paths"]["/paged_search"]["post"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
    }
    # FastAPI can't parse delimited arrays, so declare them only in the spec
    delimited_ids = openapi_schema["paths"]["/items_by_delimited_ids"]["get"]["parameters"][0]
    delimited_ids["schema"] = {"type": "array", "items": {"type": "integer"}}
    delimited_ids["explode"] = False
    header_ids = openapi_schema["paths"]["/items_by_header"]["get"]["parameters"][0]
    header_ids["schema"] = {"type": "array", "items": {"type": "integer"}}
    # FastAPI never generates path item parameters
    item_path = openapi_schema["paths"]["/items/{item_id}"]
    item_path["parameters"] = item_path["get"].pop("parameters")
    # numbers are not declared with pydantic models, because they would be parsed as Python floats
    openapi_schema["components"]["schemas"]["Numbers"] = NUMBERS_SCHEMA
    numbers_ref = {"$ref": "#/components/schemas/Numbers"}
    openapi_schema["paths"]["/numbers"]["get"]["responses"]["200"]["content"] = {
        "application/json": {"schema": {"type": "array", "items": numbers_ref}},
    }
    decimal_parameter = openapi_schema["paths"]["/decimal_parameters"]["get"]["parameters"][0]
    decimal_parameter["schema"] = {"type": "number"}
    numbers_body = {"required": True, "content": {"application/json": {"schema": numbers_ref}}}
    openapi_schema["paths"]["/numbers"]["post"]["requestBody"] = numbers_body
    openapi_schema["paths"]["/echo_numbers"]["post"]["requestBody"] = numbers_body
    openapi_schema["paths"]["/echo_numbers"]["post"]["responses"]["200"]["content"] = {
        "application/json": {"schema": numbers_ref},
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.include_router(router)
app.openapi = custom_openapi
