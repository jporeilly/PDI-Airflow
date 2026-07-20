# deploy-target

The landing zone for DAGs you generate during the workshop. The Studio
(and `pdi2dag migrate --dags-folder …/deploy-target`) writes generated
DAGs here, and Airflow — which mounts the parent `workshop/dags` folder
recursively — picks them up.

Generated `*.py` files in this folder are **git-ignored** (see the repo
`.gitignore`), so your deploys never show up as repo changes. Only this
README and `.gitkeep` are tracked, keeping the folder present on a fresh
clone.
