# Rebuild the ./lock file using a python image
# to avoid accidentally dragging in any mac os dependencies.

poetry.lock: pyproject.toml
	$(eval CID := $(shell docker run -dit --rm python:3.9))
	docker cp pyproject.toml $(CID):pyproject.toml
	docker exec $(CID) pip install poetry
	docker exec $(CID) poetry lock
	docker cp $(CID):poetry.lock poetry.lock
	docker stop $(CID)