cd # Introduction 
This project is to based on the old Excel/VBA/R tool and re-platform the 
Premium Allocation Tool into a more reliable service and easier to integrate with other applications.

* bcp Utility needs to be installed in the hosting server
  <br><br>


# Getting Started
-- First time, 
* Create a "pat.log" file: 
  ```
  C:\_Working\PAT_20201019\log\pat.log
  ```
* Include in root folder, two files: 
  ```
  logging.cfg           -and-
  .env
  ```

* Download [GCUI React](https://guycarp.visualstudio.com/GC%20Design%20System/_packaging?_a=feed&feed=gcui%40Local) library and place the .tgz file in `/pat_front`

-- In root folder "premium-allocation-tool":
```
python -m pip install --upgrade pip         -or-
py -m pip install --upgrade pip (CMD, Git Bash)

pip install poetry==1.1.2                   -or-
py -m pip install poetry==1.1.2 (CMD, Git Bash)

poetry install                              -or-
py -m poetry install (CMD, GIt Bash)
```
* Optionally, reset to clean state by running the script: `clean_install.sh`

-- In subfolder "pat_front":
```
yarn install
```

# Build and Test
```
cd pat_front
yarn build
cd ..
poetry build --format=wheel
```

# Run App
Backend:
```
poetry run py api.py                -or-
py -m poetry run py api.py (CMD, Git Bash)
```
Frontend only:
```
cd pat_front
yarn start
```

