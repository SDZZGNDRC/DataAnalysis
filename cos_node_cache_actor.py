import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import pandas as pd
import numpy as np
import os
import time
import requests
from qcloud_cos import CosConfig, CosServiceError
from qcloud_cos import CosS3Client
from qcloud_cos.cos_threadpool import SimpleThreadPool

@ray.remote
class NodeCacheActor:
    """
    自用的COS类, 包含一些常用的操作。
    这个 Actor 在 Ray 集群中作为单例服务，统一管理 COS 客户端和凭证。
    """
    def __init__(self,
                 region='ap-nanjing',
                 bucket_name='crypto-spider-1313293485',
                 local_cache_dir='node_local_cache'):
        # 初始化节点本地的状态
        self.node_id = ray.get_runtime_context().node_id
        self.local_cache_dir = local_cache_dir
        os.makedirs(self.local_cache_dir, exist_ok=True)
        
        # 打印信息，以便我们知道 Actor 在哪个节点上启动
        print(f"NodeCacheActor initialized on Node: {self.node_id}, PID: {os.getpid()}")
        print(f"Cache directory: {os.path.abspath(self.local_cache_dir)}")
        
        # COS 客户端初始化逻辑
        self.region = region
        self.bucket_name = bucket_name
        self.delimiter = ''
        self.client = None
        self.expired_time = 0
        self._refresh_client()

    def _refresh_client(self):
        """获取或刷新COS客户端和临时密钥"""
        print(f"[{self.node_id}] Refreshing COS client credentials...")
        cam = requests.get("http://metadata.tencentyun.com/latest/meta-data/cam/security-credentials/ray-cluster").json()
        self.expired_time = cam['ExpiredTime']
        self.client = CosS3Client(CosConfig(Region=self.region, SecretId=cam['TmpSecretId'], SecretKey=cam['TmpSecretKey'], Token=cam['Token']))
        print(f"[{self.node_id}] COS client credentials refreshed.")

    def _ensure_client_valid(self):
        """确保COS客户端的临时密钥有效"""
        if self.expired_time - time.time() < 1 * 60 * 60:
            self._refresh_client()

    def listCurrentDir(self, prefix):
        """列出当前目录子节点，返回所有子节点信息。

        Args:
            prefix (string): 目录前缀，类似于文件系统中的路径。

        Returns:
            list: 包含当前目录下所有文件和子目录信息的列表。
        """
        file_infos = []
        sub_dirs = []
        marker = ""
        count = 1
        while True:
            response = self.client.list_objects(self.bucket_name, prefix, self.delimiter, marker)
            # 调试输出
            # json_object = json.dumps(response, indent=4)
            # print(count, " =======================================")
            # print(json_object)
            count += 1

            if "CommonPrefixes" in response:
                common_prefixes = response.get("CommonPrefixes")
                sub_dirs.extend(common_prefixes)

            if "Contents" in response:
                contents = response.get("Contents")
                file_infos.extend(contents)

            if "NextMarker" in response.keys():
                marker = response["NextMarker"]
            else:
                break

        sorted(file_infos, key=lambda file_info: file_info["Key"])
        for file in file_infos:
            print(file)
        return file_infos

    def downLoadFiles(self, file_infos):
        """下载文件到本地目录，如果本地目录已经有同名文件则会被覆盖；
        如果目录结构不存在，则会创建和对象存储一样的目录结构

        Args:
            file_infos (list)
        """
        localDir = "./download/"
    
        # 创建线程池
        pool = SimpleThreadPool()
        # 创建线程池时若不指定线程数则默认为5。线程数可通过参数指定，例如指定线程数为10：
        # pool = SimpleThreadPool(num_threads=10)
        for file in file_infos:
            # 文件下载 获取文件到本地
            file_cos_key = file["Key"]
            localName = localDir + file_cos_key
    
            # 如果本地目录结构不存在，递归创建
            if not os.path.exists(os.path.dirname(localName)):
                os.makedirs(os.path.dirname(localName))
    
            # skip dir, no need to download it
            if str(localName).endswith("/"):
                continue
    
            # 实际下载文件
            # 使用线程池方式
            pool.add_task(self.client.download_file, self.bucket_name, file_cos_key, localName)
    
            # 简单下载方式
            # response = client.get_object(
            #     Bucket=test_bucket,
            #     Key=file_cos_key,
            # )
            # response['Body'].get_stream_to_file(localName)
    
        pool.wait_completion()
        return None

    def downLoadDirFromCos(self, prefix):
        global file_infos
    
        try:
            file_infos = self.listCurrentDir(prefix)
    
        except CosServiceError as e:
            print(e.get_origin_msg())
            print(e.get_digest_msg())
            print(e.get_status_code())
            print(e.get_error_code())
            print(e.get_error_msg())
            print(e.get_resource_location())
            print(e.get_trace_id())
            print(e.get_request_id())
    
        self.downLoadFiles(file_infos)
        return None

    def get(self, cos_key):
        """
        核心方法：获取或下载文件，并返回其在节点上的本地路径。
        """
        local_filename = cos_key.replace("/", "_") # 创建一个安全的文件名
        local_path = os.path.join(self.local_cache_dir, local_filename)

        # 3. 检查缓存
        if os.path.exists(local_path):
            print(f"[{self.node_id}] Cache HIT for {cos_key}. Path: {local_path}")
            return local_path
        
        # 4. 缓存未命中，执行下载
        print(f"[{self.node_id}] Cache MISS for {cos_key}. Downloading...")
        self._ensure_client_valid()
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=cos_key,
            )
            response['Body'].get_stream_to_file(local_path)
            print(f"[{self.node_id}] Download complete. Saved to {local_path}")
            return local_path
        except Exception as e:
            print(f"[{self.node_id}] FAILED to download {cos_key}: {e}")
            # 如果下载失败，最好清理掉可能产生的空文件或不完整文件
            if os.path.exists(local_path):
                os.remove(local_path)
            raise e

# --- 示例：如何在一个 Ray 应用中使用这个节点缓存 Actor ---

@ray.remote
def data_processing_task(file_key): # 不再需要传递 cache_actors_map
    """
    一个数据处理任务，它会智能地找到其本地节点的缓存 Actor。
    """
    # 1. 获取当前任务运行在哪一个节点上
    # 使用新的推荐 API
    current_node_id_str = ray.get_runtime_context().get_node_id() 
    
    # 2. 根据节点ID，动态获取对应名字的 Actor 句柄
    actor_name = f"cache_actor_{current_node_id_str}"
    try:
        # ray.get_actor() 会在集群中寻找这个命名的 Actor
        local_cache_actor = ray.get_actor(actor_name)
    except ValueError:
        # 如果因为某种原因 Actor 不存在，可以提供更清晰的错误信息
        return f"Failed to find cache actor for node {current_node_id_str}. Actor name '{actor_name}' not found."

    print(f"Task for {file_key} on Node {current_node_id_str} is requesting file...")
    
    try:
        # 3. 调用本地 Actor 的方法
        local_path_ref = local_cache_actor.get.remote(file_key)
        local_file_path = ray.get(local_path_ref)
        
        print(f"Task on {current_node_id_str} is now processing file: {local_file_path}")
        time.sleep(2) 
        
        return f"Successfully processed {file_key} on node {current_node_id_str}"
    except Exception as e:
        return f"Failed to process {file_key} on node {current_node_id_str}: {e}"
    
if __name__ == '__main__':
    ray.init(address='ray://localhost:10001') # 假设连接到您的集群

    # --- 关键步骤：为每个节点创建一个专属于它的 Actor ---
    
    print("Discovering nodes and creating a cache actor for each...")
    nodes = ray.nodes()
    # 创建一个字典，用于存储 NodeID -> ActorHandle 的映射
    cache_actors = {}
    for node in nodes:
        if node["Alive"]:
            node_id_str = node["NodeID"]
            actor_name = f"cache_actor_{node_id_str}" # 为每个 actor 创建一个唯一的名字
            # 使用调度策略来替代自定义资源
            print(f"Creating actor '{actor_name}' and pinning it to node {node_id_str}...")
            actor_handle = NodeCacheActor.options(
                name=actor_name, # 命名 Actor
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_id_str,
                    soft=False
                )
            ).remote()
            cache_actors[node_id_str] = actor_handle
    
    print("All node cache actors have been created:")
    print(cache_actors)

    # 假设我们要处理的文件列表 (包含重复文件，以测试缓存)
    files_to_process = [
        "raw_data/600001.SH.csv",
        "raw_data/600002.SH.csv",
        "raw_data/600001.SH.csv", # 重复
        "raw_data/600003.SH.csv",
        "raw_data/600004.SH.csv",
        "raw_data/600003.SH.csv", # 重复
        "raw_data/600001.SH.csv", # 重复
    ]

    task_refs = []
    for file_key in files_to_process:
        task_refs.append(data_processing_task.remote(file_key))

    results = ray.get(task_refs)

    print("\n--- Final Results ---")
    for res in results:
        print(res)

    ray.shutdown()