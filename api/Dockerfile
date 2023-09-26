FROM python:3.10.4

WORKDIR /app 

ENV FLASK_APP=biomarkerkb
ENV FLASK_ENV=production 

COPY ./requirements.txt .
RUN pip install -r requirements.txt 

# copy wheel distribution and install it 
COPY ./dist/biomarkerkb-1.0-py3-none-any.whl .
RUN pip install biomarkerkb-1.0-py3-none-any.whl

COPY . .

ENTRYPOINT ["gunicorn", "-b", ":80", "biomarkerkb:create_app()"]