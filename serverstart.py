from create_instance import CloudAPI

if __name__ == "__main__":
    kvserver = CloudAPI()
    kvserver.main("kvserver-script.sh", "kv-server")
