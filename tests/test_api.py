"""HTTP API: writes, queries, paging, cancellation, error handling."""

import pytest
from fastapi.testclient import TestClient

from wqe.api import create_app
from wqe.config import Config


@pytest.fixture
def client(tmp_path):
    app = create_app(Config(db_path=str(tmp_path / "api.db"), page_size=128))
    with TestClient(app) as c:
        yield c


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_write_and_query_flow(client):
    r = client.post(
        "/partitions/p/rows",
        json={"rows": [[1, 10], [1, 20], [None, 5], [2, None]]},
    )
    assert r.status_code == 200
    assert r.json()["written"] == 4

    body = {
        "partition_key": "p",
        "functions": [
            {"kind": "row_number", "alias": "rn"},
            {"kind": "rank", "alias": "rk"},
            {"kind": "lag", "alias": "lg", "offset": 1, "default": "NA"},
            {"kind": "lead", "alias": "ld", "offset": 1, "default": "NA"},
            {"kind": "sum", "alias": "s", "window": {"frame": "partition"}},
            {"kind": "count", "alias": "c", "window": {"frame": "partition"}},
            {"kind": "avg", "alias": "a", "window": {"frame": "partition"}},
            {"kind": "min", "alias": "mn", "window": {"frame": "partition"}},
            {"kind": "max", "alias": "mx", "window": {"frame": "partition"}},
            {"kind": "sum", "alias": "rs",
             "window": {"frame": "rows_preceding", "n": 1}},
        ],
    }
    r = client.post("/queries", json=body)
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["status"] == "committed"
    assert data["row_count"] == 4
    rows = data["rows"]
    # ordered NULLS LAST: 1,1,2,None
    assert [row["sort_key"] for row in rows] == [1, 1, 2, None]
    assert [row["outputs"]["rk"] for row in rows] == [1, 1, 3, 4]
    # lag default on first row; lead sees NULL value on second row
    assert rows[0]["outputs"]["lg"] == "NA"
    assert rows[0]["outputs"]["ld"] == 20
    # ordered keys: 1,1,2,None(values 10,20,NULL,5)
    # 2nd row (value 20) leads to 3rd row (value NULL); 3rd leads to 5
    assert rows[1]["outputs"]["ld"] is None
    assert rows[2]["outputs"]["ld"] == 5
    head = rows[0]["outputs"]
    # non-NULL values 10, 20, 5
    assert head["s"] == 35 and head["c"] == 3
    assert head["a"] == 35 / 3
    assert head["mn"] == 5 and head["mx"] == 20
    qid = data["query_id"]

    # Status fetch with paging.
    r = client.get(f"/queries/{qid}?offset=1&limit=2")
    paged = r.json()
    assert paged["status"] == "committed"
    assert len(paged["rows"]) == 2
    assert paged["offset"] == 1 and paged["limit"] == 2
    assert paged["row_count"] == 4


def test_nulls_first_parameter(client):
    client.post("/partitions/p/rows", json={"rows": [[None, 1], [1, 2]]})
    r = client.post(
        "/queries",
        json={
            "partition_key": "p",
            "null_order": "nulls_first",
            "functions": [{"kind": "row_number", "alias": "rn"}],
        },
    )
    assert [row["sort_key"] for row in r.json()["rows"]] == [None, 1]


def test_unknown_query_404(client):
    assert client.get("/queries/nope").status_code == 404
    assert client.post("/queries/nope/cancel").status_code == 404


def test_bad_requests(client):
    # invalid value type (objects not supported)
    r = client.post("/partitions/p/rows", json={"rows": [[1, {"x": 1}]]})
    assert r.status_code == 400
    # aggregate without a window
    r = client.post(
        "/queries",
        json={
            "partition_key": "p",
            "functions": [{"kind": "sum", "alias": "s"}],
        },
    )
    assert r.status_code == 400
    # duplicate aliases
    r = client.post(
        "/queries",
        json={
            "partition_key": "p",
            "functions": [
                {"kind": "row_number", "alias": "x"},
                {"kind": "dense_rank", "alias": "x"},
            ],
        },
    )
    assert r.status_code == 400
    # invalid pagination
    r = client.post("/partitions/p/rows", json={"rows": [[1, 1]]})
    r = client.post(
        "/queries",
        json={"partition_key": "p",
              "functions": [{"kind": "row_number", "alias": "rn"}]},
    )
    qid = r.json()["query_id"]
    assert client.get(f"/queries/{qid}?limit=0").status_code == 400


def test_timeout_returns_cancelled(client):
    # Write enough rows that a near-zero deadline cannot finish.
    rows = [[i, i] for i in range(20_000)]
    for s in range(0, len(rows), 5000):
        client.post("/partitions/p/rows", json={"rows": rows[s:s + 5000]})
    r = client.post(
        "/queries",
        json={
            "partition_key": "p",
            "timeout_s": 0.0001,
            "functions": [
                {"kind": "row_number", "alias": "rn"},
                {"kind": "max", "alias": "m", "window": {"frame": "partition"}},
            ],
        },
    )
    assert r.status_code == 201
    assert r.json()["status"] == "cancelled"
