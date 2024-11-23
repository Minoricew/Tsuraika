from setuptools import setup, find_packages

NAME = "tsuraika"

VERSION = "1.0.0"

DESCRIPTION = "The Next Generation of Fast Reverse Proxy"

AUTHOR = "CocoTeirina"

AUTHOR_EMAIL = "cocoteirina@proton.me"

URL = "https://github.com/CocoTeirina/Tsuraika"

PACKAGES = find_packages()

ENTRY_POINTS = {
    "console_scripts": [
        "tsuraika = tsuraika.cli:app",
    ],
}

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    install_requires=[
        "msgpack>=1.0.5",
        "typer[all]>=0.9.0",
    ],
    extras_require={
        "dev": [
            "black>=23.3.0",
            "isort>=5.12.0",
            "pylint>=2.17.0",
            "pytest>=7.3.1",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0"
        ]
    },
    url=URL,
    packages=PACKAGES,
    entry_points=ENTRY_POINTS
)
