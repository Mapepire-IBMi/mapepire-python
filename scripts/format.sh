#!/bin/bash
echo "Running isort >>>>>>>>>>>>>>>>>>>>"
isort .

echo "Running ruff  >>>>>>>>>>>>>>>>>>>>"
ruff format . --exclude tests/

echo "Running black >>>>>>>>>>>>>>>>>>>>"
black . --exclude tests/