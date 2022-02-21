# Linked Data Repository Shared

Code shared between the various `linked-data-repository` applications.

# GCP PubSub Client: Running Tests

At time of writing the tests PubSub client tests are running against topics and subscriptions on GCP itself so we're **not** running them on commit, a developer needs to explicitly call them.

These tests **will** clean up after themselves regardless of test results (provided you don't bomb out early - please don't do that).

To use:

* Install the gcp cli https://cloud.google.com/sdk/docs/install
* Authenticate your machine with `gcloud auth login` and your **gsscogs.uk** address
* use `gcloud auth application-default login` and your **gsscogs.uk** address - this will tell gcloud to use your developer credentials for applications locally (in place of holdings lots of different service account credentials on your machine).
* Install dependencies via poetry `poetry install`
* Run tests via `poetry run pytest ./gcp_tests/* -v` (the -v is for verbose, you'll want that).

