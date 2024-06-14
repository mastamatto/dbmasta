from setuptools import setup, find_packages

setup(
    name="dbConnect",
    version="1.0",
    packages=find_packages(),
    install_requires = [
        "sqlalchemy>=2.0.27",
        "python-dotenv",
        "pymysql",
        "asyncmy"
    ]
)