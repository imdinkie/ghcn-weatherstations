Venv:

python3 -m venv .venv
source .venv/bin/activate

Requirements:

pip install -r requirements.txt

Start uvicorn with .env file:

uvicorn app.main:app --reload --env-file .env