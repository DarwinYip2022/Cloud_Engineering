from setuptools import setup, find_packages
setup(
    name='project_pipeline',
    version='0.1',
    author='Komono Zhou',
    author_email='ziyizhou2024@northwestern.edu',
    description='A simple Random Forest classifier using scikit-learn',
    long_description='A simple clouds classifier using sklearn',
    long_description_content_type='text/markdown',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
    install_requires=[
        'numpy',
        'scikit-learn',
        'pandas',
        'requests',
    ],
)
