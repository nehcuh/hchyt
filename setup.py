import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hchyt",
    version="0.0.2",
    author="Hu Chen",
    author_email="curiousbull@163.com",
    description="This is my quant box",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nehcuh/hchyt",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)