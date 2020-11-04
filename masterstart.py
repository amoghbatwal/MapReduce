from create_instance import CloudAPI

if __name__ == "__main__":
    master = CloudAPI()
    master.main("master-script.sh", "master-node")
