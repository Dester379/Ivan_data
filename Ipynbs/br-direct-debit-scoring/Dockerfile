FROM nexus.mm.moneyman.ru/mm/python3:0.0.3
LABEL maintainer="kirill@shtrykov.com"
COPY . /opt/early-scoring
WORKDIR /opt/early-scoring
RUN pip3 install -r requirements.txt
ENTRYPOINT ["/usr/bin/python3", "api.py"]
EXPOSE 8080
