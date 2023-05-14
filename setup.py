from setuptools import setup


setup(
    name='cldfbench_nmdb-data',
    py_modules=['cldfbench_nmdb-data'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'nmdb-data=cldfbench_nmdb-data:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
