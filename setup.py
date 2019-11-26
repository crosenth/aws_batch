import setuptools

setuptools.setup(author='Chris Rosenthal',
                 author_email='crosenth@gmail.com',
                 classifiers=[
                     'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
                     'Development Status :: 4 - Beta',
                     'Environment :: Console',
                     'Operating System :: OS Independent',
                     'Intended Audience :: End Users/Desktop',
                     'License :: OSI Approved :: '
                     'GNU General Public License v3 (GPLv3)',
                     'Programming Language :: Python :: 3 :: Only'],
                 python_requires='>=3.3',
                 description='AWS Batch helper',
                 entry_points={
                     'console_scripts': {'aws_batch=aws_batch:main'}},
                 install_requires=['awscli'],
                 keywords=['aws', 'batch', 's3'],
                 license='GPLv3',
                 name='aws_batch',
                 packages=setuptools.find_packages(),
                 version=0.2,
                 url='https://github.com/crosenth/aws_batch'
                 )
