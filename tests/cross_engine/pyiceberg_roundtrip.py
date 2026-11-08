"""The PyIceberg half of the cross-engine round trip.

`write DIR` builds tables the way PyIceberg writes them, into DIR, and records
for each the metadata location and the rows it holds. Bergman then maintains
them (`cargo test --test cross_engine -- --ignored`, which reads DIR from
`BERGMAN_CROSS_ENGINE_DIR`) and records the metadata it committed. `verify DIR`
reads each maintained table back with PyIceberg and checks it holds exactly the
rows PyIceberg wrote — through PyIceberg's own reader, which shares no code
with the one Bergman's suite otherwise reads its own output with.

Run through `just cross-engine`, which pins the PyIceberg version.
"""

import json
import sys
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "region", StringType(), required=True),
)
ARROW = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
    ]
)

SPECS = {
    "unpartitioned": PartitionSpec(),
    "by_region": PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="region")
    ),
    "by_bucket": PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="id_bucket")
    ),
}


def rows(table):
    data = table.scan().to_arrow().select(["id", "name", "region"]).to_pylist()
    return sorted((r["id"], r["name"], r["region"]) for r in data)


def write(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("pyiceberg", uri=f"sqlite:///{out}/catalog.db", warehouse=f"file://{out}")
    catalog.create_namespace("db")
    manifest = {}
    for name, spec in SPECS.items():
        table = catalog.create_table(f"db.{name}", schema=SCHEMA, partition_spec=spec)
        # Many small appends — the shape compaction exists for — then a
        # delete, so the table's history holds a rewrite PyIceberg performed.
        for i in range(6):
            ids = list(range(10 * i, 10 * i + 10))
            table.append(
                pa.table(
                    {
                        "id": pa.array(ids, pa.int32()),
                        "name": [f"n{x}" for x in ids],
                        "region": ["eu" if x % 2 else "us" for x in ids],
                    },
                    schema=ARROW,
                )
            )
        table.delete("id < 5")
        table = catalog.load_table(f"db.{name}")
        manifest[name] = {
            "metadata": table.metadata_location,
            "rows": rows(table),
            "data_files": len(list(table.scan().plan_files())),
        }
    (out / "written.json").write_text(json.dumps(manifest, indent=2))
    print(f"wrote {len(manifest)} tables into {out}")


def verify(out: Path) -> None:
    written = json.loads((out / "written.json").read_text())
    maintained = json.loads((out / "maintained.json").read_text())
    failures = []
    for name, before in written.items():
        after = maintained.get(name)
        if after is None:
            failures.append(f"{name}: Bergman recorded no maintained metadata")
            continue
        table = StaticTable.from_metadata(after["metadata"])
        got = rows(table)
        expected = [tuple(r) for r in before["rows"]]
        if got != expected:
            failures.append(f"{name}: PyIceberg reads {len(got)} rows, wrote {len(expected)}")
        files = len(list(table.scan().plan_files()))
        if files >= before["data_files"]:
            failures.append(f"{name}: {files} data files, was {before['data_files']} — nothing compacted")
        print(f"{name}: {len(got)} rows, {before['data_files']} -> {files} files, "
              f"format v{table.metadata.format_version}")
    v3 = out / "v3.json"
    if v3.exists():
        record = json.loads(v3.read_text())
        table = StaticTable.from_metadata(record["metadata"])
        data = table.scan().to_arrow().select(["id", "name"]).to_pylist()
        got = sorted([r["id"], r["name"]] for r in data)
        expected = sorted(record["rows"])
        if got != expected:
            failures.append(f"v3: PyIceberg reads {got}, Bergman left {expected}")
        print(f"v3: {len(got)} rows, format v{table.metadata.format_version}, "
              f"next-row-id {table.metadata.next_row_id}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        sys.exit(1)
    print("every table PyIceberg wrote reads back identically after Bergman maintained it")


if __name__ == "__main__":
    command, directory = sys.argv[1], Path(sys.argv[2])
    {"write": write, "verify": verify}[command](directory)
