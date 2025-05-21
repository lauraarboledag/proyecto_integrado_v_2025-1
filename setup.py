from setuptools import setup, find_packages 

setup(
    name= "entregables_2025",
    author= "Laura Arboleda",
    version= "0.0.1",
    author_email= "",
    description= "", 
    py_modules= ["proyecto"],
    install_requires = [
        "pandas==2.2.3",
        "openpyxl",
        "requests==2.32.3",
        "beautifulsoup4==4.13.3",
        "joblib",
        "scikit-learn"
    ]
)