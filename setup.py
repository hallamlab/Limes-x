import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("./src/limes_x/version.txt") as f:
    version = f.read()

setuptools.setup(
    name="limes-x",
    version=version,
    author="Tony Liu",
    author_email="contacttonyliu@gmail.com",
    description="declarative workflow automation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hallamlab/limes-x",
    project_urls={
        "Bug Tracker": "https://github.com/hallamlab/limes-x/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    # packages=pks,
    package_data={
        "":["version.txt"],
        # "package-name": ["*.txt"],
        # "test_package": ["res/*.txt"],
    },
    entry_points={
        'console_scripts': [
            'lx = limes_x:main',
        ]
    },
    python_requires=">=3.10",
    install_requires=[
        "snakemake",
    ]
)