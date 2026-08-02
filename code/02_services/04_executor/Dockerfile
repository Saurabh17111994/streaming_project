FROM python:3.11-slim
# R-133: floating 3.11 patch tag (tracks security/bugfix patches). The exact
# patch pin was stale (April 2024) and missed subsequent fixes. For a fully
# reproducible build, pin to the current 3.11 patch and refresh it regularly.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py"]
