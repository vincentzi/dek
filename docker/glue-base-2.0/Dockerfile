FROM python:3.7.12-bullseye

## Install utils
RUN apt-get update && apt-get install -y zip git

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip && ./aws/install

## Install Open JDK 8
RUN apt-get update \
  && apt-get install -y software-properties-common \
  && apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' \
  && apt-get update \
  && apt-get install -y openjdk-8-jdk

## Create environment variables
ENV M2_HOME=/opt/apache-maven-3.6.0
ENV SPARK_HOME=/opt/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
ENV PATH="${PATH}:${M2_HOME}/bin"


## Add Maven and Spark binary to /opt
RUN curl -SsL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz \
    | tar -C /opt --warning=no-unknown-keyword -xzf -
RUN curl -SsL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-2.0/spark-2.4.3-bin-hadoop2.8.tgz \
    | tar -C /opt --warning=no-unknown-keyword -xzf -

# Install Glue dependencies
RUN git clone https://github.com/awslabs/aws-glue-libs.git /opt/aws-glue-libs-2.0 \
    && cd /opt/aws-glue-libs-2.0 \
    && git checkout glue-2.0 \
    && cd /opt/aws-glue-libs-2.0/bin \
    && bash -c "source glue-setup.sh"

## Add default Python packages
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

## Update Python path
ENV GLUE_HOME=/opt/aws-glue-libs-2.0
ENV PYTHONPATH=$GLUE_HOME:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$SPARK_HOME/python


#############################################
############### Customization ###############
#############################################


## Install extra Python packages
RUN pip install 'boto3-stubs[essentials,glue]'

## Install Zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.2/zsh-in-docker.sh)" -- \
    -p git \
    -p https://github.com/zsh-users/zsh-autosuggestions \
    -p https://github.com/zsh-users/zsh-completions \
    -p https://github.com/zsh-users/zsh-syntax-highlighting


EXPOSE 4040

CMD ["zsh"]