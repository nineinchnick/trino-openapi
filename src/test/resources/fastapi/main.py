import logging
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Callable, Literal

from fastapi import APIRouter, Cookie, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field
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
# compress all but the smallest responses, so most tests also read compressed responses
app.add_middleware(GZipMiddleware, minimum_size=100)
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
    priority: int | None = None
    # fields sent in the last PATCH request, to check which columns an UPDATE sends
    patched_fields: list[str] = []


class NoteBody(BaseModel):
    text: str


class NotePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str | None = None
    priority: int | None = None


class Label(BaseModel):
    id: int
    name: str
    tags: list[str] = []
    value: str | int | None = None


class LabelBody(BaseModel):
    name: str
    tags: list[str] = []
    value: str | int | None = None


class Bookmark(BaseModel):
    url: str


class HasMoreResults(BaseModel):
    results: list[Item]
    has_more: bool


class TotalPagesResults(BaseModel):
    results: list[Item]
    total_pages: int


class RequestCount(BaseModel):
    requests: int
    last_query: str = None


class PageFilter(BaseModel):
    page: int = 1


class DecimalText(BaseModel):
    decimal_text: str


class DateTimeText(BaseModel):
    at_text: str
    received_at: datetime
    shifted_at: datetime


class DateText(BaseModel):
    day_text: str
    next_day: date


class Change(BaseModel):
    id: int
    updated_at: datetime
    query: str


class ChangesResults(BaseModel):
    results: list[Change]
    total_results: int


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


class UserViews(BaseModel):
    views: list[str]


class UserViewsError(BaseModel):
    code: int
    message: str


# forbidding extra fields rejects values with fields of both alternatives
class Bot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    bot_name: str


class Person(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    email: str


class Event(BaseModel):
    name: str
    actor: Bot | Person


class File(BaseModel):
    name: str
    content: str


class Entry(BaseModel):
    name: str
    size: int


class ContentsResults(BaseModel):
    results: File | list[Entry]


class Results(BaseModel):
    results: list[Item]
    total_results: int


class Station(BaseModel):
    station_id: int
    name: str
    temperature: float


class Comment(BaseModel):
    comment_id: int
    author: str
    text: str


class Review(BaseModel):
    id: int
    rating: int
    text: str


items = {
    1: Item(id="1", name="Portal Gun", price=42.0, tags=["sci-fi"], revisedAt=[date.fromisoformat("2007-10-10"), date.fromisoformat("2022-12-08")], categories=[Category(id="1", name="main")]),
    2: Item(id="2", name="Plumbus", price=32.0, validUntil=date.fromisoformat("2999-01-01"), properties={"feeble": "schleem"}),
}


notes: dict[int, Note] = {}

labels: dict[int, Label] = {}

bookmarks = [Bookmark(url="https://trino.io"), Bookmark(url="https://example.com")]

stations = {
    1: Station(station_id=1, name="Alpha", temperature=20.0),
    2: Station(station_id=2, name="Bravo", temperature=-5.0),
}

comments = {
    1: Comment(comment_id=1, author="alice", text="First"),
    2: Comment(comment_id=2, author="bob", text="Second"),
    3: Comment(comment_id=3, author="alice", text="Third"),
}

product_reviews = {
    1: {
        1: Review(id=1, rating=5, text="Great"),
        2: Review(id=2, rating=5, text="Good"),
        3: Review(id=3, rating=1, text="Bad"),
    },
}

# kept as raw JSON, because Python floats can't hold all decimal values exactly
numbers = [
    '{"id": 1, "real_value": -1.5, "decimal_value": 1234567890.12345678, '
    '"real_values": [1.5, -2.25], "decimal_values": [0.0000000001, -9999999999999999999999999999.9999999999], '
    '"decimal_map": {"a": 1710950954154}}',
]

NUMBERS_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "real_value": {"type": "number", "format": "float"},
        "decimal_value": {"type": "number"},
        "real_values": {"type": "array", "items": {"type": "number", "format": "float"}},
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


class CachedItemRequests(BaseModel):
    total: int
    revalidated: int


cached_item_requests = CachedItemRequests(total=0, revalidated=0)
CACHED_ITEMS_ETAG = '"items-v1"'


class RequestEncoding(BaseModel):
    accept_encoding: str | None


@router.get("/request_encoding")
def get_request_encoding(request: Request) -> RequestEncoding:
    return RequestEncoding(accept_encoding=request.headers.get("accept-encoding"))


@router.get("/cached_items")
def list_cached_items(request: Request, response: Response) -> list[Item]:
    cached_item_requests.total += 1
    if request.headers.get("if-none-match") == CACHED_ITEMS_ETAG:
        cached_item_requests.revalidated += 1
        return Response(status_code=304, headers={"ETag": CACHED_ITEMS_ETAG, "Cache-Control": "max-age=300"})
    response.headers["ETag"] = CACHED_ITEMS_ETAG
    response.headers["Cache-Control"] = "max-age=300"
    return list(items.values())


@router.get("/cached_item_requests")
def get_cached_item_requests() -> CachedItemRequests:
    return cached_item_requests


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


@router.get("/writable_items")
def list_writable_items(page: int = 1, per_page: int = 1, sort: str = None, direction: str = "asc") -> list[Item]:
    results = list(items.values())
    if sort is not None:
        results.sort(key=lambda item: getattr(item, sort), reverse=direction == "desc")
    offset = (page - 1) * per_page
    return results[offset:offset+per_page]


# an operation used for DELETE adds a hidden row ID column to the table
@router.delete("/writable_items/{item_id}")
def delete_writable_item(item_id: int):
    return None


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


# like servers that cap the page size below the declared default, these return one item per page,
# and the last page for out of range page numbers, so a short page must not end pagination
@router.get("/short_page_items")
def list_short_page_items(page: int = 1, size: int = 3) -> Results:
    page = min(max(page, 1), len(items))
    return Results(
        results=list(items.values())[page - 1:page],
        total_results=len(items))


@router.get("/short_page_has_more_items")
def list_short_page_has_more_items(page: int = 1, size: int = 3) -> HasMoreResults:
    page = min(max(page, 1), len(items))
    return HasMoreResults(
        results=list(items.values())[page - 1:page],
        has_more=page < len(items))


@router.get("/max_pages_items")
def list_max_pages_items(page: int = 1) -> Results:
    page = min(max(page, 1), len(items))
    return Results(
        results=list(items.values())[page - 1:page],
        total_results=len(items))


counted_items_requests = RequestCount(requests=0)


@router.get("/counted_items")
def list_counted_items(request: Request, page: int = 1, sort: str = None, direction: str = "asc", name: str = None) -> Results:
    # save requests, so tests can check how many pages were requested, and with which parameters, using /counted_items_requests
    counted_items_requests.requests += 1
    counted_items_requests.last_query = request.url.query
    # only listing items can filter them by name, like listing pull requests by state
    results = [item for item in items.values() if name is None or item.name == name]
    if sort is not None:
        # nulls are last when ascending and first when descending, unlike the Trino default of NULLS LAST
        results.sort(key=lambda item: (getattr(item, sort) is None, getattr(item, sort) or 0), reverse=direction == "desc")
    return Results(
        results=results[page - 1:page],
        total_results=len(results))


@router.get("/counted_items/{item_id}")
def get_counted_item(request: Request, item_id: int) -> Item:
    counted_items_requests.requests += 1
    counted_items_requests.last_query = request.url.query
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    return items[item_id]


# an INSERT operation makes response-only columns, like price, nullable, even though every response includes them
@router.post("/counted_items", status_code=201)
def create_counted_item(body: NoteBody) -> Item:
    return items[1]


@router.get("/max_results_items")
def list_max_results_items(request: Request, page: int = 1) -> Results:
    # like servers that fail for results past a limit, but without the error, to check that no such page is requested
    return list_counted_items(request, page)


@router.get("/counted_items_requests")
def get_counted_items_requests() -> RequestCount:
    return counted_items_requests


defaulted_items_requests = RequestCount(requests=0)
defaulted_item_states = {1: "open", 2: "closed"}


@router.get("/defaulted_items")
def list_defaulted_items(
        request: Request,
        page: int = 1,
        per_page: int = 1,
        state: Literal["open", "closed", "all"] = "open",
        sort: str = None,
        direction: str = "asc") -> Results:
    # like listing GitHub pull requests, without parameters only the first open item is returned on every page,
    # so the spec sets parameterDefaults to read all items in fewer requests
    defaulted_items_requests.requests += 1
    defaulted_items_requests.last_query = request.url.query
    results = [item for item_id, item in items.items() if state == "all" or defaulted_item_states[item_id] == state]
    if sort is not None:
        results.sort(key=lambda item: getattr(item, sort), reverse=direction == "desc")
    return Results(
        results=results[(page - 1) * per_page:page * per_page],
        total_results=len(results))


@router.get("/defaulted_items/{item_id}")
def get_defaulted_item(request: Request, item_id: int) -> Item:
    defaulted_items_requests.requests += 1
    defaulted_items_requests.last_query = request.url.query
    return items[item_id]


@router.get("/defaulted_items_requests")
def get_defaulted_items_requests() -> RequestCount:
    return defaulted_items_requests


class Fruit(BaseModel):
    name: str


class FruitResults(BaseModel):
    results: list[Fruit]
    total_results: int


fruits = [Fruit(name="cherry"), Fruit(name="Banana"), Fruit(name="apple")]
sorted_fruits_requests = RequestCount(requests=0)


@router.get("/sorted_fruits")
def list_sorted_fruits(request: Request, page: int = 1, sort: str = None, direction: str = "asc") -> FruitResults:
    # like many servers, sort text ignoring case, so apple comes before Banana, unlike in Trino
    return sort_fruits(request, page, sort, direction, key=str.lower)


@router.get("/binary_sorted_fruits")
def list_binary_sorted_fruits(request: Request, page: int = 1, sort: str = None, direction: str = "asc") -> FruitResults:
    return sort_fruits(request, page, sort, direction, key=str)


def sort_fruits(request: Request, page: int, sort: str, direction: str, key: Callable[[str], str]) -> FruitResults:
    # save requests, so tests can check how many pages were requested, and with which parameters, using /sorted_fruits_requests
    sorted_fruits_requests.requests += 1
    sorted_fruits_requests.last_query = request.url.query
    results = list(fruits)
    if sort is not None:
        results.sort(key=lambda fruit: key(getattr(fruit, sort)), reverse=direction == "desc")
    return FruitResults(
        results=results[page - 1:page],
        total_results=len(results))


@router.get("/sorted_fruits_requests")
def get_sorted_fruits_requests() -> RequestCount:
    return sorted_fruits_requests


class OwnedItem(BaseModel):
    name: str
    ownerId: str


owned_items = [OwnedItem(name="Portal Gun", ownerId="Rick"), OwnedItem(name="Plumbus", ownerId="Morty")]


@router.get("/owned_items")
def list_owned_items(name: str = None, ownerId: str = None) -> list[OwnedItem]:
    # like search endpoints, names match by a substring and owners ignore case
    return [item for item in owned_items
            if (name is None or name in item.name) and (ownerId is None or ownerId.lower() == item.ownerId.lower())]


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


@router.patch("/notes/{note_id}")
def patch_note(note_id: int, body: NotePatch) -> Note:
    if note_id not in notes:
        raise HTTPException(status_code=404, detail="Note not found")
    changes = body.model_dump(exclude_unset=True)
    notes[note_id] = notes[note_id].model_copy(update={**changes, "patched_fields": sorted(changes)})
    return notes[note_id]


@router.delete("/notes/{note_id}", status_code=204)
def delete_note(note_id: int):
    if note_id not in notes:
        raise HTTPException(status_code=404, detail="Note not found")
    del notes[note_id]


@router.get("/labels")
def list_labels() -> list[Label]:
    return list(labels.values())


@router.get("/labels/{label_id}")
def get_label(label_id: int) -> Label:
    if label_id not in labels:
        raise HTTPException(status_code=404, detail="Label not found")
    return labels[label_id]


@router.post("/labels/{label_id}", status_code=201)
def create_label(label_id: int, body: LabelBody) -> Label:
    if label_id in labels:
        raise HTTPException(status_code=409, detail="Label already exists")
    labels[label_id] = Label(id=label_id, **body.model_dump())
    return labels[label_id]


@router.put("/labels/{label_id}")
def update_label(label_id: int, body: LabelBody) -> Label:
    if label_id not in labels:
        raise HTTPException(status_code=404, detail="Label not found")
    labels[label_id] = Label(id=label_id, **body.model_dump())
    return labels[label_id]


# like Petstore's /user/createWithList, the request body is an array of resources
@router.post("/label_list")
def create_labels(body: list[Label]) -> list[Label]:
    for label in body:
        labels[label.id] = label
    return body


@router.get("/bookmarks")
def list_bookmarks() -> list[Bookmark]:
    return bookmarks


# some APIs delete resources using POST, with filters in the request body
@router.post("/bookmarks")
def delete_bookmarks(body: Bookmark) -> list[Bookmark]:
    global bookmarks
    deleted = [bookmark for bookmark in bookmarks if bookmark.url == body.url]
    bookmarks = [bookmark for bookmark in bookmarks if bookmark.url != body.url]
    return deleted


@router.get("/stations")
def list_stations() -> list[Station]:
    return list(stations.values())


@router.get("/stations/{station_id}")
def get_station(station_id: int, units: str) -> Station:
    # units are required, so this path can't be used without them, even when the station ID is known
    if station_id not in stations:
        raise HTTPException(status_code=404, detail="Station not found")
    station = stations[station_id]
    if units == "imperial":
        return station.model_copy(update={"temperature": station.temperature * 9 / 5 + 32})
    return station


@router.get("/comments")
def list_comments(author: str) -> list[Comment]:
    # listing comments requires an author, so reading all comments requires their IDs
    return [comment for comment in comments.values() if comment.author == author]


@router.get("/products/{product_id}/reviews")
def list_reviews(product_id: int, rating: int = None) -> list[Review]:
    reviews = product_reviews.get(product_id, {}).values()
    return [review for review in reviews if rating is None or review.rating == rating]


@router.get("/products/{product_id}/reviews/{review_id}")
def get_review(product_id: int, review_id: int) -> Review:
    # reviews don't include their ID under the name of this parameter, so it's only known from the path
    if review_id not in product_reviews.get(product_id, {}):
        raise HTTPException(status_code=404, detail="Review not found")
    return product_reviews[product_id][review_id]


@router.get("/comments/{comment_id}")
def get_comment(comment_id: int) -> Comment:
    if comment_id not in comments:
        raise HTTPException(status_code=404, detail="Comment not found")
    return comments[comment_id]


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


# some APIs respond with an error object and a 200 status, so the response schema is a union of both
@router.get("/user_views/{username}")
def user_views(username: str) -> UserViews | UserViewsError:
    if username == "unknown":
        return UserViewsError(code=404, message="Unknown user")
    return UserViews(views=["home", "settings"])


@router.get("/user_views_list")
def user_views_list() -> list[UserViews | UserViewsError]:
    return [UserViews(views=["home"]), UserViewsError(code=404, message="Unknown user")]


@router.get("/events")
def list_events() -> list[Event]:
    return [
        Event(name="build", actor=Bot(id="ci", bot_name="CI")),
        Event(name="review", actor=Person(id=7, email="jane@example.com")),
    ]


@router.post("/echo_event")
def echo_event(body: Event) -> Event:
    return body


def get_contents(path: str) -> File | list[Entry]:
    if path == "readme":
        return File(name="readme", content="Hello")
    return [Entry(name="a", size=1), Entry(name="b", size=2)]


# like GitHub's contents endpoint, files are single objects, and directories are arrays of entries
@router.get("/contents/{path}")
def contents(path: str) -> File | list[Entry]:
    return get_contents(path)


@router.get("/contents_results/{path}")
def contents_results(path: str) -> ContentsResults:
    return ContentsResults(results=get_contents(path))


@router.get("/decimal_parameters")
def decimal_parameters(decimal_value: str) -> DecimalText:
    # returns the exact text of the parameter, to check how it was formatted
    return DecimalText(decimal_text=decimal_value)


@router.get("/datetime_parameters")
def datetime_parameters(request: Request, at: datetime) -> DateTimeText:
    # returns the exact text of the parameter, to check how it was formatted,
    # and the parsed value with different offsets, to check how they're read
    return DateTimeText(
        at_text=request.query_params["at"],
        received_at=at,
        shifted_at=at.astimezone(timezone(-timedelta(hours=5, minutes=30))))


@router.get("/date_parameters")
def date_parameters(request: Request, day: date) -> DateText:
    return DateText(day_text=request.query_params["day"], next_day=day + timedelta(days=1))


change_times = [datetime(2024, 1, day, tzinfo=timezone.utc) for day in range(1, 6)]


def get_changes(request: Request, since: datetime = None, until: datetime = None) -> list[Change]:
    # every change includes the query string, to check which bounds were sent
    return [
        Change(id=index + 1, updated_at=updated_at, query=request.scope["query_string"].decode())
        for index, updated_at in enumerate(change_times)
        if (since is None or updated_at >= since) and (until is None or updated_at < until)
    ]


@router.get("/changes")
def list_changes(request: Request, since: datetime = None, until: datetime = None) -> list[Change]:
    return get_changes(request, since, until)


paged_changes_requests = 0


@router.get("/paged_changes")
def list_paged_changes(request: Request, page: int = 1, since: datetime = None, until: datetime = None) -> ChangesResults:
    # count requests, so tests can check how many pages were requested using /paged_changes_requests
    global paged_changes_requests
    paged_changes_requests += 1
    changes = get_changes(request, since, until)
    return ChangesResults(results=changes[page - 1:page], total_results=len(changes))


@router.get("/paged_changes_requests")
def get_paged_changes_requests() -> RequestCount:
    return RequestCount(requests=paged_changes_requests)


@router.get("/changes/{change_id}")
def get_change(request: Request, change_id: int) -> Change:
    return next(change for change in get_changes(request) if change.id == change_id)


@router.get("/exact_changes")
def list_exact_changes(request: Request, since: datetime = None, until: datetime = None) -> list[Change]:
    return get_changes(request, since, until)


@router.get("/approximate_changes")
def list_approximate_changes(request: Request, since: datetime = None) -> list[Change]:
    # like APIs that compare only dates, the time of the bound is ignored, so more changes are returned
    if since is not None:
        since = since.replace(hour=0, minute=0, second=0, microsecond=0)
    return get_changes(request, since)


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


@router.get("/retired_items")
def list_retired_items() -> list[Item]:
    # like a path that a proxy, or a server using a different prefix, doesn't know
    raise HTTPException(status_code=404, detail="Not Found")


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
    openapi_schema["paths"]["/retired_items"]["get"]["x-trino"] = {
        "errorPath": "$response.body#/detail"
    }
    openapi_schema["paths"]["/item_categories"]["get"]["x-trino"] = {
        "resultsPath": "$response.body#/results/0/categories"
    }
    # more stride splits than pages, to read pages past the last one
    openapi_schema["paths"]["/items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "per_page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
        "strideSplits": 3,
    }
    openapi_schema["paths"]["/writable_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "per_page",
        "sortParam": "sort",
        "sortValues": {"price": "price"},
        "directionParam": "direction",
        "ascendingValue": "asc",
        "descendingValue": "desc",
    }
    openapi_schema["paths"]["/clamped_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
        "strideSplits": 3,
    }
    # more stride splits than pages, to check that they're not used without a total
    openapi_schema["paths"]["/clamped_sized_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "size",
        "resultsPath": "$response.body#/results",
        "strideSplits": 3,
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
        "strideSplits": 3,
    }
    openapi_schema["paths"]["/total_pages_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalPagesPath": "$response.body#/total_pages",
    }
    openapi_schema["paths"]["/short_page_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "size",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
    }
    openapi_schema["paths"]["/short_page_has_more_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "size",
        "resultsPath": "$response.body#/results",
        "hasMorePath": "$response.body#/has_more",
        "strideSplits": 3,
    }
    openapi_schema["paths"]["/max_pages_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "maxPages": len(items),
    }
    openapi_schema["paths"]["/counted_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
        "sortParam": "sort",
        "sortValues": {"price": "price", "tax": "tax", "name": "name"},
        "directionParam": "direction",
        "ascendingValue": "asc",
        "descendingValue": "desc",
    }
    # the total number of results is not declared, so pagination stops on a page with fewer results than the page size
    openapi_schema["paths"]["/defaulted_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "limitParam": "per_page",
        "resultsPath": "$response.body#/results",
        "sortParam": "sort",
        "sortValues": {"price": "price"},
        "directionParam": "direction",
        "ascendingValue": "asc",
        "descendingValue": "desc",
        "parameterDefaults": {"state": "all", "per_page": 3},
    }
    fruits_sorting = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
        "sortParam": "sort",
        "directionParam": "direction",
        "ascendingValue": "asc",
        "descendingValue": "desc",
    }
    openapi_schema["paths"]["/sorted_fruits"]["get"]["x-trino"] = {
        **fruits_sorting,
        "sortValues": {"name": "name"},
    }
    openapi_schema["paths"]["/binary_sorted_fruits"]["get"]["x-trino"] = {
        **fruits_sorting,
        "sortValues": {"name": {"value": "name", "binaryOrder": True}},
    }
    openapi_schema["paths"]["/max_results_items"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "maxResults": 1,
    }
    openapi_schema["paths"]["/contents_results/{path}"]["get"]["x-trino"] = {
        "resultsPath": "$response.body#/results",
    }
    range_filters = {
        "updated_at": {
            "lowerParam": "since",
            "lowerInclusive": True,
            "upperParam": "until",
            "upperInclusive": False,
            "exact": True,
        },
    }
    openapi_schema["paths"]["/changes"]["get"]["x-trino"] = {
        "rangeFilters": range_filters,
    }
    openapi_schema["paths"]["/exact_changes"]["get"]["x-trino"] = {
        "rangeFilters": range_filters,
    }
    openapi_schema["paths"]["/paged_changes"]["get"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
        "rangeFilters": range_filters,
    }
    openapi_schema["paths"]["/approximate_changes"]["get"]["x-trino"] = {
        "rangeFilters": {
            "updated_at": {
                "lowerParam": "since",
                "lowerInclusive": True,
            },
        },
    }
    openapi_schema["paths"]["/paged_search"]["post"]["x-trino"] = {
        "pageParam": "page",
        "resultsPath": "$response.body#/results",
        "totalResultsPath": "$response.body#/total_results",
    }
    # these POST operations only read data, but POST operations are used for INSERT, unless marked otherwise
    for path in ["/search", "/paged_search", "/echo_numbers", "/echo_union", "/echo_map", "/echo_union_value", "/echo_gadget", "/echo_event"]:
        openapi_schema["paths"][path]["post"].setdefault("x-trino", {})["sqlOperation"] = "select"
    openapi_schema["paths"]["/bookmarks"]["post"]["x-trino"] = {"sqlOperation": "delete"}
    # pydantic describes optional values as unions with null, which are mapped to VARCHAR
    for schema in ["Note", "NotePatch"]:
        openapi_schema["components"]["schemas"][schema]["properties"]["priority"] = {"type": ["integer", "null"]}
    # a union without a null alternative, which pydantic adds to optional values
    for schema in ["Label", "LabelBody"]:
        openapi_schema["components"]["schemas"][schema]["properties"]["value"] = {"oneOf": [{"type": "string"}, {"type": "integer"}]}
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
