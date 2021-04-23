FROM python:3.9

WORKDIR /app
VOLUME /data

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "exchanger.py", "-c", "/data/exchanger.ini"]