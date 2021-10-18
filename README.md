# Introduction 
This project is to based on the old Excel/VBA/R tool and re-platform the 
Premium Allocation Tool into a more reliable service and easier to integrate with other applications.

* bcp Utility needs to be installed in the hosting server
* 

# Getting Started
python -m pip install --upgrade pip
pip3 install poetry==1.1.2
poetry install

# Build and Test
cd frentend
yarn build
cd ..
poetry build --format=whee

