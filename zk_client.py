from kazoo.client import KazooClient
import time
import os

def get_path_and_no(node_path):
    tmp_list = node_path.split("-")
    if len(tmp_list) < 2 :
        return "", "0"
    else:
        return node_path, tmp_list[1]

class ZkNode:
    def __init__(self, host, time_out, root_path, file_prefix):
        self.zk = KazooClient(hosts = host, timeout=time_out)
        self.root_path_ = root_path
        self.node_no_ = "0"
        self.file_prefix_ = file_prefix + "-"
        self.node_no_list_ = []

    def start_(self):
            self.zk.start()

    def get_node_(self):
        if not os.path.exists("node_info.txt"):
            with open("node_info.txt", "w+") as fo:
                pass
        with open("node_info.txt", "r+") as fi:
            node_path = fi.readline()
            return get_path_and_no(node_path)

    def create_node_(self):
        node_path, self.node_no_ = self.get_node_();
        if self.node_no_ == "0":
            zk_path = os.path.join(self.root_path_, self.file_prefix_)
            node_path = self.zk.create(zk_path, b"True", ephemeral=True, sequence=True)
            node_path, self.node_no_ = get_path_and_no(node_path)
            with open("node_info.txt", "w+") as fo:
                fo.write(node_path)
        else:
            node_path = self.zk.create(node_path, b"True", ephemeral=True, sequence=False)
        self.node_no_list_.append(self.node_no_)

    def node_to_node_list(self, node_list):
        self.node_no_list_.clear()
        for node_name in node_list:
            n_n, node_no = get_path_and_no(node_name)
            if node_no != "0":
                self.node_no_list_.append(node_no)
        print (self.node_no_list_)

    def node_change_callback(self, event):
        print ("event type: %s"%(event.type))
        print ("state :%s"%(event.state))
        if event.type == "CHILD" and event.state == "CONNECTED":
            node_list = self.zk.get_children(self.root_path_, watch=self.node_change_callback)
            self.node_to_node_list(node_list)

    def get_children_(self):
        node_list = self.zk.get_children(self.root_path_, watch=self.node_change_callback)
        self.node_to_node_list(node_list)

    def run(self):
        self.start_()
        self.create_node_()
        self.get_children_()
        try:
            while True:
                time.sleep(5)
        except:
            self.zk.stop()

def main():
    print ("run")
    node = ZkNode("192.168.56.102:2181", 5.0, "/zookeeper", "node")
    node.run()

if __name__ == "__main__":
    main()