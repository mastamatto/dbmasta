from setuptools import setup, find_packages

def getlong():
    with open('README.md') as f:
        data = f.read()
    return data

setup(
    name='dbmasta',
    version='0.1.1',
    author='Matt Owen',
    author_email='matt@ca-automation.app',
    description='A simple MariaDB client based on SQLAlchemy core',
    long_description=getlong(),
    long_description_content_type='text/markdown',
    url='https://github.com/mastamatto/dbConnect',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=2.0.27',
        'asyncmy',
        'pymysql'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)
