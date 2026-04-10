"""
Client WebHDFS — interface REST pour Apache Hadoop HDFS.
Doc : https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
"""
import logging
import requests

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER     = "root"


class WebHDFSClient:

    def __init__(self, base_url=WEBHDFS_BASE_URL, user=WEBHDFS_USER):
        self.base_url = base_url.rstrip("/")
        self.user = user

    def _url(self, path, op, **params):
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        for k, v in params.items():
            url += f"&{k}={v}"
        return url

    def mkdirs(self, hdfs_path):
        resp = requests.put(self._url(hdfs_path, "MKDIRS"), timeout=30)
        resp.raise_for_status()
        success = resp.json().get("boolean", False)
        logger.info("HDFS mkdirs %s → %s", hdfs_path, success)
        return success

    def upload(self, hdfs_path, local_file_path):
        # Étape 1 — initiation sur le NameNode (retourne 307)
        init_resp = requests.put(
            self._url(hdfs_path, "CREATE", overwrite="true"),
            allow_redirects=False,
            timeout=30,
        )
        datanode_url = init_resp.headers.get("Location")
        if not datanode_url:
            raise RuntimeError("WebHDFS : header Location absent dans la réponse 307")

        # Étape 2 — envoi des données vers le DataNode
        with open(local_file_path, "rb") as f:
            up_resp = requests.put(
                datanode_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600,
            )
        if up_resp.status_code not in (200, 201):
            up_resp.raise_for_status()

        logger.info("HDFS upload OK : %s (HTTP %d)", hdfs_path, up_resp.status_code)
        return hdfs_path

    def open(self, hdfs_path):
        resp = requests.get(
            self._url(hdfs_path, "OPEN"),
            allow_redirects=True,
            timeout=600,
        )
        resp.raise_for_status()
        logger.info("HDFS open OK : %s (%d octets)", hdfs_path, len(resp.content))
        return resp.content

    def exists(self, hdfs_path):
        try:
            resp = requests.get(self._url(hdfs_path, "GETFILESTATUS"), timeout=30)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def list_status(self, hdfs_path):
        resp = requests.get(self._url(hdfs_path, "LISTSTATUS"), timeout=30)
        resp.raise_for_status()
        return resp.json().get("FileStatuses", {}).get("FileStatus", [])
