install:
    pip install -r requirements.txt

lint:
    flake8 .

format:
    black .

test:
    python -m unittest discover

