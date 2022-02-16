rm poetry.lock
py -m pip install poetry==1.1.9
py -m poetry cache clear pypi --all
# Uncomment to remove the virtual env if it already exists
# py -m poetry env remove premium-allocation-tool-kxJ-Wl85-py3.10
py -m poetry install
echo "Add python.exe in the following env variable as the Python Interpreter in Visual Studio Code"
py -m poetry env list --full-path