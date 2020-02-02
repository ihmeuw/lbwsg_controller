import os

from setuptools import setup, find_packages


if __name__ == "__main__":

    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    install_requirements = [
        'click',
        'loguru',
    ]

    extras_require = {
        'tables-old': [
            'tables==3.4.0',
            'pandas<0.25',
            'get_draws',
            'db_queries',
        ],
        'tables-new': [
            'tables==3.5.2',
            'pandas>=0.25',
            'get_draws',
            'db_queries',
        ]
    }

    setup(
        name='lbwsg',
        package_dir={'': 'src'},
        packages=find_packages(where='src'),
        include_package_data=True,

        install_requires=install_requirements,
        extras_require=extras_require,

        zip_safe=False,

        entry_points='''
            [console_scripts]
            make_lbwsg_pickle=lbwsg.cli:get_draws
        '''
    )
