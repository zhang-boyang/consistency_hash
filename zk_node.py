import node
import zk_client

HASH_BASH = 4096
VIRTUAL_NUM = 10

def main():
    print("run")
    n = node.Node(HASH_BASH, VIRTUAL_NUM, "192.168.56.102:2181", 5.0, "/zookeeper", "node")
    n.run()

if __name__ == "__main__":
    main()