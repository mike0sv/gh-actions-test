import io
import time
import traceback
from urllib.parse import urlparse

import boto3
import docker
import pytest
from botocore.exceptions import EndpointConnectionError
from docker.models.containers import Container
from testcontainers.core.container import DockerContainer

KEY = 'file.bin'

ACCESS_KEY = 'accessKey1'
SECRET_KEY = 'verySecretKey1'
PORT = 8008
BUCKET = 'testbucket'


@pytest.fixture
def run_s3_docker():
    c = DockerContainer('scality/s3server:mem-latest'). \
        with_name('tmp_s3_2').with_bind_ports(8000, PORT)
    with c as c:
        time.sleep(10)
        yield c.get_container_host_ip()


@pytest.fixture
def docker_host():
    client = docker.from_env()
    uri = urlparse(client.api.base_url)
    return uri.hostname


@pytest.fixture
def run_s3_docker2():
    # docker run -d --name s3server -p 8003:8000 scality/s3server:mem-latest
    print('running container')
    client: docker.DockerClient = docker.from_env()
    print(client.api.base_url)
    try:
        client.containers.list()
    except:
        traceback.print_exc()
        raise Exception('no docker')
    container: Container = client.containers.run('scality/s3server:mem-latest', name='tmp_s3', auto_remove=True,
                                                 detach=True, ports={8000: PORT})

    for _ in range(5):
        time.sleep(5)
        if any('tmp_s3' == c.name for c in client.containers.list()):
            break
        print(f'trying {_}')
    else:
        raise Exception('cant run container')

    print('Logs', container.logs().decode('utf8'))
    yield
    container.stop()


def test_upload_and_download_file(run_s3_docker, docker_host):
    print('running test')
    try:
        endpoint = f'http://{run_s3_docker}:{PORT}'
        print('ENDPOINT', endpoint)
        s3 = boto3.client('s3',
                          endpoint_url=endpoint,
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

        s3.create_bucket(Bucket=BUCKET)
        s3.upload_fileobj(io.BytesIO(b'kek'), BUCKET, KEY)
        buffer = io.BytesIO()
        s3.download_fileobj(BUCKET, KEY, buffer)
        assert b'kek' == buffer.getvalue()
    except EndpointConnectionError as e:
        pytest.fail(str(e))
