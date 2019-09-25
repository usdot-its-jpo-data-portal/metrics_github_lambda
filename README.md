## Setup Instructions

Prerequisites:
- CD to root of repo
- Python 3.6

Deployment:
- Create a virtualenv at the root of the repo with this command:
`virtualenv -p python3 ./.venv_metrics_github_lambda`

- Activate that venv with this command:
`source ./.venv_metrics_github_lambda/bin/activate`

- Install all pip dependencies to the package folder with this command:
`pip install -r package/requirements.txt -t ./package`

- CD into package folder: `cd package`

- Zip package folder using this command:
`zip -r ../lambdapackage.zip ./*`

- Upload package to lambda using AWS CLI or web console