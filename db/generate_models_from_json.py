from __future__ import annotations

import json
import keyword
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any, Tuple, Set


BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "db" / "models"

SCHEMA_JSON_PATH = BASE_DIR / "db" / "db_schema.json"


def pascal_case(name: str) -> str:
    parts = re.split(r"[^0-9a-zA-Z]+", name)
    parts = [p for p in parts if p]
    if not parts:
        return "UnnamedTable"

    cls = "".join(p[:1].upper() + p[1:] for p in parts)
    if cls[0].isdigit():
        cls = f"T{cls}"
    return cls


def safe_attr_name(col_name: str) -> str:

    name = re.sub(r"\W+", "_", col_name)
    name = name.strip("_")

    if not name:
        name = "col"

    if name[0].isdigit():
        name = f"col_{name}"

    if keyword.iskeyword(name):
        name = f"{name}_"

    return name


def sqlalchemy_type(data_type: str, max_length: Any) -> Tuple[str, Set[str]]:

    dt = data_type.lower()
    imports: Set[str] = set()

    def string_type() -> Tuple[str, Set[str]]:
        imports_local: Set[str] = set()
        if max_length in (None, -1):
            imports_local.add("Text")
            return "Text", imports_local
        imports_local.add("String")
        return f"String({max_length})", imports_local

    if dt in {"int", "integer"}:
        imports.add("Integer")
        return "Integer", imports
    if dt == "bigint":
        imports.add("BigInteger")
        return "BigInteger", imports
    if dt in {"smallint", "tinyint"}:
        imports.add("SmallInteger")
        return "SmallInteger", imports
    if dt in {"float", "real"}:
        imports.add("Float")
        return "Float", imports
    if dt in {"decimal", "numeric", "money", "smallmoney"}:
        imports.add("Numeric")
        return "Numeric", imports
    if dt in {"bit"}:
        imports.add("Boolean")
        return "Boolean", imports

    if dt in {"varchar", "nvarchar", "nchar", "char"}:
        return string_type()
    if dt in {"text", "ntext"}:
        imports.add("Text")
        return "Text", imports

    if dt in {"datetime", "datetime2", "smalldatetime"}:
        imports.add("DateTime")
        return "DateTime", imports
    if dt in {"date"}:
        imports.add("Date")
        return "Date", imports
    if dt in {"time"}:
        imports.add("Time")
        return "Time", imports

    if dt in {"binary", "varbinary", "image"}:
        imports.add("LargeBinary")
        return "LargeBinary", imports

    if dt in {"uniqueidentifier"}:
        imports.add("String")
        return "String(36)", imports

    imports.add("String")
    return "String", imports


def python_type_hint(sa_type: str, nullable: bool) -> str:
    base = "str"
    if sa_type.startswith(("Integer", "BigInteger", "SmallInteger", "Numeric")):
        base = "int"
    elif sa_type.startswith(("Float", "Numeric")):
        base = "float"
    elif sa_type.startswith(("Boolean",)):
        base = "bool"
    elif sa_type.startswith(("DateTime", "Date", "Time")):
        base = "datetime"
    elif sa_type.startswith(("LargeBinary",)):
        base = "bytes"

    if nullable:
        return f"Optional[{base}]"
    return base


def load_schema() -> List[Dict[str, Any]]:
    data = json.loads(SCHEMA_JSON_PATH.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                return value
        raise ValueError("JSON format not recognized")
    return data


def group_by_schema_table(rows: List[Dict[str, Any]]):
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        schema = r["TABLE_SCHEMA"]
        table = r["TABLE_NAME"]
        grouped[schema][table].append(r)
    return grouped


HEADER_TEMPLATE = '''\
from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, {sa_imports}
from .base import Base

'''


def generate_models_for_schema(schema: str, tables: Dict[str, List[Dict[str, Any]]]):

    lines: List[str] = []
    used_types: Set[str] = set()

    for table_name, cols in sorted(tables.items()):
        class_name = pascal_case(f"{schema}_{table_name}")
        lines.append(f"class {class_name}(Base):")
        lines.append(f'    __tablename__ = "{table_name}"')
        lines.append(f'    __table_args__ = {{"schema": "{schema}"}}')
        lines.append("")

        for col in cols:
            col_name = col["COLUMN_NAME"]
            data_type = col["DATA_TYPE"]
            nullable = (col["IS_NULLABLE"] == "YES")
            max_len = col["CHARACTER_MAXIMUM_LENGTH"]

            sa_type_str, imports = sqlalchemy_type(data_type, max_len)
            used_types.update(imports)

            attr_name = safe_attr_name(col_name)
            py_hint = python_type_hint(sa_type_str, nullable)

            if attr_name == col_name:
                col_def = f"{attr_name}: {py_hint} = Column({sa_type_str}, nullable={str(nullable)})"
            else:
                col_def = f'{attr_name}: {py_hint} = Column("{col_name}", {sa_type_str}, nullable={str(nullable)})'

            lines.append(f"    {col_def}")

        lines.append("")

    sa_imports = ", ".join(sorted(used_types)) if used_types else ""
    content = HEADER_TEMPLATE.format(sa_imports=sa_imports or "Column") + "\n".join(lines)
    content = content.rstrip() + "\n"

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MODELS_DIR / f"{schema}.py"
    out_path.write_text(content, encoding="utf-8")
    print(f"[+] wrote {out_path.relative_to(BASE_DIR)}")


def main():
    rows = load_schema()
    grouped = group_by_schema_table(rows)

    base_path = MODELS_DIR / "base.py"
    if not base_path.exists():
        base_content = '''\
from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass
'''
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        base_path.write_text(base_content, encoding="utf-8")
        print(f"[+] wrote {base_path.relative_to(BASE_DIR)}")

    for schema, tables in sorted(grouped.items()):
        generate_models_for_schema(schema, tables)

    init_path = MODELS_DIR / "__init__.py"
    if not init_path.exists():
        init_path.write_text(
            'from __future__ import annotations\n\nfrom .base import Base\n',
            encoding="utf-8",
        )
        print(f"[+] wrote {init_path.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()
