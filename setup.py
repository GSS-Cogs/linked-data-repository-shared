import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ldrshared",
    author="Michael Adams",
    author_email="michael.adams@gsscogs.uk",
    description="Common python clients and helpers used by the linked data repository applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/GSS-Cogs/linked-data-repository-shared",
    packages=setuptools.find_packages(),
    install_requires=["google-cloud-pubsub", "tinydb"],
)
