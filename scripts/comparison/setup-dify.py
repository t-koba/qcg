"""Prepare an owned, disposable, digest-pinned Dify comparison deployment.

Requires the fixed Docker/Compose tools and an already-running disposable
rootless daemon. This never changes an existing Dify deployment or account.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parent
lock = json.loads((ROOT / "runtime-lock.json").read_text())
parser = argparse.ArgumentParser()
parser.add_argument("--docker", default="docker")
parser.add_argument("--compose", required=True)
parser.add_argument("--directory")
args = parser.parse_args()

def run(command, **options):
    return subprocess.run(command, check=True, text=True, **options)

def output(command):
    return subprocess.check_output(command, text=True).strip()

info = output([args.docker, "info", "--format", "{{.ServerVersion}} {{json .SecurityOptions}}"])
if not info.startswith(lock["docker"]["version"]+" "):
    raise SystemExit("Docker version differs from runtime-lock.json")
if "name=rootless" not in info:
    raise SystemExit("Use a disposable rootless Docker daemon for this local comparison")
if output([args.compose, "version", "--short"]).removeprefix("v") != lock["compose"]["version"]:
    raise SystemExit("Compose version differs from runtime-lock.json")
directory = Path(args.directory) if args.directory else Path(tempfile.mkdtemp(prefix="qcg-dify-fixture-"))
if args.directory:
    directory.mkdir(parents=True, exist_ok=False)
project = "qcgcmp-"+uuid.uuid4().hex[:12]
source = directory / "source"
run(["git", "init", str(source)], stdout=subprocess.DEVNULL)
run(["git", "-C", str(source), "remote", "add", "origin", "https://github.com/langgenius/dify.git"])
run(["git", "-C", str(source), "fetch", "--depth", "1", "origin", lock["dify"]["revision"]], stdout=subprocess.DEVNULL)
run(["git", "-C", str(source), "checkout", "--detach", "FETCH_HEAD"], stdout=subprocess.DEVNULL)
if output(["git", "-C", str(source), "rev-parse", "HEAD"]) != lock["dify"]["revision"]:
    raise SystemExit("Dify source revision differs")
docker_dir = source / "docker"
(docker_dir / ".env").write_text((docker_dir / ".env.example").read_text())
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
images = {image.split("@",1)[0].split("/")[-1]: image for image in lock["dify"]["images"]}
for image in images.values():
    run([args.docker, "pull", image], stdout=subprocess.DEVNULL)
# Exactly the enabled workload services, excluding unrelated vector/agent stacks.
(docker_dir / "comparison.nginx.conf").write_text('server { listen 5001; server_name localhost; location / { proxy_pass http://api:5001; proxy_http_version 1.1; proxy_set_header Host $http_host; proxy_set_header Connection ""; proxy_buffering off; proxy_read_timeout 120s; } }\n')
with (docker_dir / ".env").open("a") as environment_file:
    environment_file.write(f'\nCONSOLE_API_URL=http://127.0.0.1:{port}\nCONSOLE_WEB_URL=http://127.0.0.1:{port}\nSERVICE_API_URL=http://127.0.0.1:{port}\nSERVER_WORKER_AMOUNT=1\nCELERY_WORKER_AMOUNT=1\nVECTOR_STORE=qdrant\nDISABLE_TELEMETRY=true\nENTERPRISE_TELEMETRY_ENABLED=false\n')
depends = """    depends_on: !override
      init_permissions:
        condition: service_completed_successfully
      db_postgres:
        condition: service_healthy
      redis:
        condition: service_started
"""
text = 'services:\n'
for service, image in [("api","dify-api"),("worker","dify-api"),("db_postgres","postgres"),("redis","redis"),("sandbox","dify-sandbox"),("ssrf_proxy","squid"),("plugin_daemon","dify-plugin-daemon"),("init_permissions","busybox")]:
    text += f'  {service}:\n    image: {images[image]}\n'
    if service in ("api","worker"):
        text += '    environment:\n      DISABLE_TELEMETRY: "true"\n      ENTERPRISE_TELEMETRY_ENABLED: "false"\n'+depends
    if service in ("api","plugin_daemon"):
        text += '    ports: !reset []\n'
text += f'''  comparison_gateway:
    image: {images['nginx']}
    restart: on-failure
    depends_on:
      api:
        condition: service_healthy
    ports:
      - "127.0.0.1:{port}:5001"
    networks:
      - default
      - comparison_ingress
    volumes:
      - ./comparison.nginx.conf:/etc/nginx/conf.d/default.conf:ro
networks:
  default:
    internal: true
  comparison_ingress:
    driver: bridge
'''
override = docker_dir / "comparison.override.yaml"
override.write_text(text)
compose = [args.compose,"--project-directory",str(docker_dir),"-f",str(docker_dir/"docker-compose.yaml"),"-f",str(override),"-p",project]
services = ["api","worker","db_postgres","redis","sandbox","ssrf_proxy","plugin_daemon","init_permissions","comparison_gateway"]
run(compose+["up","-d",*services])
if output([args.docker,"network","inspect",project+"_default","--format","{{.Internal}}"]).lower() != "true":
    raise SystemExit("Dify application network is not internal")
config = {"directory": str(directory), "project": project, "url": f"http://127.0.0.1:{port}", "api_container": project+"-api-1", "compose_command": compose}
(directory/"comparison-config.json").write_text(json.dumps(config,indent=2)+"\n")
env = {"QCG_DIFY_URL": config["url"],"QCG_DIFY_PROJECT":project,"QCG_DIFY_API_CONTAINER":config["api_container"],"QCG_COMPARISON_DOCKER":args.docker,"QCG_DIFY_ACCOUNT_FILE":str(directory/"account.json")}
(directory/"comparison.env").write_text("\n".join(f"export {key}={shlex.quote(value)}" for key,value in env.items())+"\n")
print(f"Fixture configuration: {directory/'comparison-config.json'}")
print(f"source {shlex.quote(str(directory/'comparison.env'))}")
print("Cleanup only this owned fixture with: "+shlex.join(compose+["down","--volumes"]))
