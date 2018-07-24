import hashlib
import bisect
import random
import math
import zk_client
import threading

class HashItem:
    def __init__(self, no, node_item):
        self.no_ = no
        self.item_ = node_item
    # <
    def __lt__(self, intx):
        return self.no_ < intx
    # >
    def __gt__(self, intx):
        return self.no_ > intx
    def __str__(self):
        return str(self.no_) + "+" + self.item_.id_

class VirtualNode:
    def __init__(self, id, parent_id, hash_base):
        self.id_ = id
        self.parent_id_ = parent_id
        self.hash_base_ = hash_base
    def get_hash_num(self):
        md5 = hashlib.md5()
        md5.update((self.id_ + self.parent_id_).encode('utf-8'))
        self.num_ = int(md5.hexdigest()[0:8], base = 16) % self.hash_base_
        return self.num_

class HashNode:
    def __init__(self, id, hash_base, num_virtual):
        self.id_ = id
        self.hash_base_ = hash_base
        self.num_virtual_ = num_virtual
        self.virutal_node_ = [VirtualNode(str(i), self.id_, self.hash_base_) \
            for i in range(self.num_virtual_)]
        self.rang_num_set = set()
        self.hit_set = set()

class Node(zk_client.ZkNode):
    def __init__(self, hash_base, num_virtual, host, time_out, root_path, file_prefix):
        zk_client.ZkNode.__init__(self, host, time_out, root_path, file_prefix)
        self.id_ = self.node_no_
        self.hash_ring_set_ = set()
        self.hash_ring_ = []
        self.num_v_ = num_virtual
        self.hash_base_ = hash_base

    def re_initialize_node(self):
        self.hash_ring_set_ = set()
        self.hash_ring_ = []
        node_vec = [HashNode(self.node_no_list_[i], self.hash_base_, self.num_v_) \
            for i in range(len(self.node_no_list_))]
        for node_item in node_vec:
            for v_node in node_item.virutal_node_:
                h = v_node.get_hash_num()
                if h in self.hash_ring_set_:
                    continue
                else:
                    node_item.rang_num_set.add(h)
                    self.hash_ring_.append(HashItem(h, node_item))
                    self.hash_ring_set_.add(h)
        self.hash_ring_.sort()
        for item in self.hash_ring_:
            print(item, end="  ", flush = True)
        print("\n")
        
    def node_change_callback(self, event):
        #print ("event type: %s"%(event.type))
        #print ("state :%s"%(event.state))
        if event.type == "CHILD" and event.state == "CONNECTED":
            node_list = self.zk.get_children(self.root_path_, watch=self.node_change_callback)
            self.node_to_node_list(node_list)
            self.re_initialize_node()

    def get_children_(self):
        node_list = self.zk.get_children(self.root_path_, watch=self.node_change_callback)
        self.node_to_node_list(node_list)
        self.re_initialize_node()

    def run_(self):
        t = threading.Thread(target=self.run, args=(self,))
        t.start()
        t.join()
    