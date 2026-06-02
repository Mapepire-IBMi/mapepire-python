.PHONY : run-checks
run-checks :
	isort --check .
	black --check .
	ruff check .
	mypy .
	CUDA_VISIBLE_DEVICES='' pytest -v --color=yes --doctest-modules tests/ mapepire_python/

.PHONY : build
build :
	rm -rf *.egg-info/
	python -m build
