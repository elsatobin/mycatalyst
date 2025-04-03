## ❗ this library is beyond highly experimental, dont use it please

# catalyst

a lightweight, sql-like query builder designed to accelerate python developers in handling database interactions. catalyst aims to simplify database operations, providing an intuitive and efficient approach to building and managing database queries in python.

## supported databases
currently, catalyst supports the following databases:
- ✅ postgres

support for additional databases is maybe planned in future updates.

## development

### requirements
- [uv](https://docs.astral.sh/uv) - a modern python package manager and build tool.

### environment setup
prepare your development environment with the following commands:

```bash
uv venv .venv --python 3.10
source .venv/bin/activate
uv sync
```

this creates and activates a virtual environment, then synchronizes all required dependencies.

### building catalyst
create distributable packages by running:

```bash
uv build
```

this generates build artifacts ready for distribution in **./dist**.

## roadmap
make this shit prod ready