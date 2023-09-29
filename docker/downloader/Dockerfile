FROM python

# USER CREATE
#
RUN apt-get update -y && apt-get install git
RUN apt-get install wget -y
# install systemd
#RUN wget -O "/data/bild.png" "https://d1q6f0aelx0por.cloudfront.net/product-logos/library-python-logo.png"

WORKDIR /data/
RUN --mount=type=secret,id=token,required token="$(cat /run/secrets/token)" \
    && git clone "https://${token}@github.com/52North/OGC_DP23.git" 

CMD "sleep" "1d"