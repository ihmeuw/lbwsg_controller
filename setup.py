import os

from setuptools import setup, find_packages


if __name__ == "__main__":

    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    install_requirements = [
        'click',
        'loguru',
        'pandas',
        'tqdm',
        'drmaa',
        'db_queries',
        'pandas<0.25',
        'tables==3.4.2',
        'vivarium',
    ]


    setup(
        name='lbwsg_controller',
        package_dir={'': 'src'},
        packages=find_packages(where='src'),
        include_package_data=True,

        install_requires=install_requirements,

        zip_safe=False,

        entry_points='''
            [console_scripts]
            make_lbwsg_pickles=lbwsg_controller.cli:make_lbwsg_pickles
            make_lbwsg_hdf_files=lbwsg_controller.cli.make_lbwsg_hdf_files
        '''
    )
