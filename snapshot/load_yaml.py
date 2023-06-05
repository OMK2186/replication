import yaml


loc="/Users/ketan/Documents/Projects/replication/snapshot/userpaylaterbalance_closing.glue.yaml"
with open(loc, "r") as stream:
    try:
        configsyaml.safe_load(stream))
    except yaml.YAMLError as exc:
        print(exc)