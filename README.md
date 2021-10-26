# Introduction 
This project is to based on the old Excel/VBA/R tool and re-platform the 
Premium Allocation Tool into a more reliable service and easier to integrate with other applications.

* bcp Utility needs to be installed in the hosting server
* 

# Getting Started
--In root folder "premium-allocation-tool"
python -m pip install --upgrade pip
pip install poetry==1.1.2
poetry install

--In subfolder "pat_front"
yarn install

# Build and Test
cd pat_front
yarn build
cd ..
poetry build --format=whee

