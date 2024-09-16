#!/bin/bash
echo "Running isort >>>>>>>>>>>>>>>>>>>>"
isort .

echo "Running ruff  >>>>>>>>>>>>>>>>>>>>"
ruff .  --fix  --exclude tests/

echo "Running black >>>>>>>>>>>>>>>>>>>>"
black . --exclude tests/