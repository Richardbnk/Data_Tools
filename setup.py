import setuptools

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='Data_Tools',
    version='1.0.0',
    author="Richard Raphael Banak",
    description="Biblioteca de códigos para processamento de dados",
    url="https://github.com/Richardbnk/Data_Tools",
    packages=['data_tools'],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=[required],
)


