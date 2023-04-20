
use super::INum;
use async_trait::async_trait;
use core::fmt::Debug;
use datenlord::common::error::{Context, DatenLordError, DatenLordResult};
use etcd_client::TxnCmp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::path::PathBuf;
use std::time::SystemTime;

/// In order to derive Serialize and Deserialize,
/// Replace the `SFlag` with `u32` , `u32` can later be converted to `SFlag`
#[derive(Serialize, Deserialize, Debug)]
pub struct RawFileAttr{
        /// Inode number
        pub ino: INum,
        /// Size in bytes
        pub size: u64,
        /// Size in blocks
        pub blocks: u64,
        /// Time of last access
        pub atime: SystemTime,
        /// Time of last modification
        pub mtime: SystemTime,
        /// Time of last change
        pub ctime: SystemTime,
        /// Time of creation (macOS only)
        pub crtime: SystemTime,
        /// Kind of file (directory, file, pipe, etc)
        pub kind: u32,
        /// Permissions
        pub perm: u16,
        /// Number of hard links
        pub nlink: u32,
        /// User id
        pub uid: u32,
        /// Group id
        pub gid: u32,
        /// Rdev
        pub rdev: u32,
        /// Flags (macOS only, see chflags(2))
        pub flags: u32,
}

/// In order to derive Serialize and Deserialize,
/// Replace the `Arc<FileAttr>` with `RawFileAttr` in `DirEntry`
#[derive(Serialize, Deserialize, Debug)]
pub struct RawDirEntry{
    pub name: String,
    pub file_attr : RawFileAttr,
}

/// In order to derive Serialize and Deserialize,
/// Replace the 'BTreeMap<String, DirEntry>' with 'HashMap<String, RawDirEntry>'
#[derive(Serialize, Deserialize, Debug)]
pub enum RawS3NodeData {
    Directory(HashMap<String,RawDirEntry>),
    /// File data is ignored ,because `Arc<GlobalCache>` is not serializable
    SymLink(PathBuf),
}


/// In order to derive Serialize and Deserialize,
/// Ignore the `s3_backend` and `meta` in `S3Node` , because they are not serializable
/// And replace the atomic types with normal types
#[derive(Serialize, Deserialize, Debug)]
pub struct RawS3Node {
    /// Parent node i-number
    parent: u64,
    /// S3Node name
    name: String,
    /// Full path
    full_path: String,
    /// S3Node attribute
    attr: RawFileAttr,
    /// S3Node data
    data: RawS3NodeData,
    /// S3Node open counter
    open_count: i64,
    /// S3Node lookup counter
    lookup_count: i64,
    /// If S3Node has been marked as deferred deletion
    deferred_deletion: bool,
}

/// The ValueType is used to provide support for metadata.
#[derive(Serialize, Deserialize, Debug)]
pub enum ValueType {
    Node(RawS3Node),
    DirEntry(RawDirEntry),
    INum(INum),
    Attr(RawFileAttr),
}

/// The KeyType is used to locate the value in the distributed K/V storage.
/// Every key is prefixed with a string to indicate the type of the value.
/// If you want to add a new type of value, you need to add a new variant to the enum.
/// And you need to add a new match arm to the get_key function , make sure the key is unique.
#[allow(dead_code)]
pub enum KeyType {
    INum2Node(INum),
    INum2DirEntry(INum),
    Path2INum(String),
    INum2Attr(INum),
}

impl KeyType {
    /// Get the key in bytes.
    fn get_key(&self) -> Vec<u8> {
        match self {
            KeyType::INum2Node(i) => format!("I{}", i).into_bytes(),
            KeyType::INum2DirEntry(i) => format!("D{}", i).into_bytes(),
            KeyType::Path2INum(p) => format!("P{}", p).into_bytes(),
            KeyType::INum2Attr(i) => format!("A{}", i).into_bytes(),
        }
    }
}

/// The Txn is used to provide support for metadata.
#[async_trait]
pub trait Txn {
    /// Get the value by the key. 
    /// Notice : do not get the same key twice in one transaction.
    async fn get(&mut self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
    /// Set the value by the key.
    async fn set(&mut self, key: &KeyType, value: &ValueType) -> DatenLordResult<()>;
    /// Delete the value by the key.
    async fn delete(&mut self, key: &KeyType) -> DatenLordResult<()>;
    /// Commit the transaction.
    /// Only when commit is called, the write operations will be executed.
    async fn commit(&mut self) -> DatenLordResult<()>;
}

/// To support different K/V storage engines, we need to a trait to abstract the K/V storage engine.
#[async_trait]
pub trait KVEngine {
    /// Create a new transaction.
    async fn new_txn(&self) -> Box<dyn Txn + Send>;
}

/// The `etcd`'s transaction impl.
/// The txn won't do anything until commit is called.
/// Write operations are buffered until commit is called.
struct EtcdTxn {
    /// The etcd client.
    client: etcd_client::Client,
    /// The key is the key in bytes, the value is the version of the key.
    version_map: HashMap<Vec<u8>, usize>,
    /// Store the write operations in the buffer.
    buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl EtcdTxn {
    /// Create a new etcd transaction.
    async fn new(client: etcd_client::Client) -> DatenLordResult<Self> {
        Ok(EtcdTxn {
            client,
            version_map: HashMap::new(),
            buffer: HashMap::new(),
        })
    }
}

#[async_trait]
impl Txn for EtcdTxn {
    async fn get(&mut self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        // first check if the key is in buffer (write op)
        let key = key.get_key();
        if let Some(_) = self.buffer.get(&key) {
                panic!("get the key after write in the same transaction")
        }
        if let Some(_) = self.version_map.get(&key) {
            panic!("get the key twice in the same transaction");
        }
         // Fetch the value from `etcd`
        let req = etcd_client::EtcdGetRequest::new(key.clone());
        let mut resp = self
            .client
            .kv()
            .get(req)
            .await
            .with_context(|| format!("failed to get GetResponse from etcd, key={key:?}"))?;
        let kvs = resp.take_kvs();
        if kvs.len() > 1 {
            // panic here because we don't expect to have multiple values for one key
            panic!("multiple values for one key");
        }
        match kvs.get(0) {
            Some(kv) => {
                let value = kv.value();
                // update the version_map
                self.version_map.insert(key.clone(), kv.version());
                Ok(Some(serde_json::from_slice(value)?))
            }
            None => {
                // update the version_map
                self.version_map.insert(key.clone(), 0);
                Ok(None)
            }
        }
    }

    async fn set(&mut self, key: &KeyType, value: &ValueType) -> DatenLordResult<()> {
        let key = key.get_key();
        let value = serde_json::to_vec(value)?;
        self.buffer.insert(key, Some(value));
        Ok(())
    }

    async fn delete(&mut self, key: &KeyType) -> DatenLordResult<()> {
        let key = key.get_key();
        self.buffer.insert(key, None);
        Ok(())
    }

    async fn commit(&mut self) -> DatenLordResult<()> {
        let mut req: etcd_client::EtcdTxnRequest = etcd_client::EtcdTxnRequest::new();
        // add the version check
        for (key, value) in self.version_map.iter() {
            let key = key.clone();
            let version = *value;
            req = req.when_version(
                etcd_client::KeyRange::key(key.clone()),
                TxnCmp::Equal,
                version,
            );
        }
        // add the write operations
        for (key, value) in self.buffer.iter() {
            let key = key.clone();
            if let Some(value) = value {
                // TODO(xiaguan) : maybe too many clone?
                let put_req = etcd_client::EtcdPutRequest::new(key.clone(), value.clone());
                req = req.and_then(put_req);
            } else {
                let delete_req =
                    etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::key(key.clone()));
                req = req.and_then(delete_req);
            }
        }
        let resp = self
            .client
            .kv()
            .txn(req)
            .await
            .with_context(|| format!("failed to get TxnResponse from etcd"))?;
        if resp.is_success() {
            Ok(())
        } else {
            // TODO(xiaguan) : Add new error type for txn failed.
            Err(DatenLordError::Unimplemented {
                context: vec!["txn failed".to_string()],
            })
        }
    }
}

#[derive(Clone)]
struct EtcdKVEngine {
    client: etcd_client::Client,
}

impl EtcdKVEngine {
    #[allow(dead_code)]
    async fn new(etcd_address_vec : Vec<String> ) -> DatenLordResult<Self> {
        let client = etcd_client::Client::connect(etcd_client::ClientConfig::new(
            etcd_address_vec.clone(),
            None,
            64,
            true,
        ))
        .await
        .with_context(|| format!("failed to build etcd client to addresses={etcd_address_vec:?}"))?;
        Ok(EtcdKVEngine { client })
    }
}

#[async_trait]
impl KVEngine for EtcdKVEngine {
    async fn new_txn(&self) -> Box<dyn Txn + Send> {
        Box::new(EtcdTxn::new(self.client.clone()).await.unwrap())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test]
    async fn test_connect_local() {
        let client = EtcdKVEngine::new(vec![ETCD_ADDRESS.to_string()]).await.unwrap();
        let mut txn = client.new_txn().await;
        let key = KeyType::Path2INum(String::from("/"));
        let value = ValueType::INum(12);
        txn.set(&key, &value).await.unwrap();
        txn.commit().await.unwrap();
        let mut txn = client.new_txn().await;
        let value = txn.get(&key).await.unwrap();
        if let Some(ValueType::INum(num)) = value {
            assert_eq!(num, 12);
        } else {
            panic!("wrong value type {value:?}");
        }
    }

    #[tokio::test]
    async fn test_easy_commit_fail() {
        // Generate three transactions
        // The first one will set two keys and commit
        // And the second one read two keys 
        // And the third one will set two keys and commit
        // What we expect is that the second one will fail
        // Between it's read ,the thrird one will set the same key
        let client = EtcdKVEngine::new(vec![ETCD_ADDRESS.to_string()]).await.unwrap();
        let mut first_txn = client.new_txn().await;
        let key1 = KeyType::Path2INum(String::from("/"));
        let value1 = ValueType::INum(12);
        let key2 = KeyType::Path2INum(String::from("/a"));
        let value2 = ValueType::INum(13);
        first_txn.set(&key1, &value1).await.unwrap();
        first_txn.set(&key2, &value2).await.unwrap();
        first_txn.commit().await.unwrap();
        // use two thread to do the second and third txn
        // and use channel to control the order
        let (first_step_tx, first_step_rx) = tokio::sync::oneshot::channel::<()>();
        let (second_step_tx, second_step_rx) = tokio::sync::oneshot::channel::<()>();
        let second_handle = tokio::spawn(async move {
            let client = EtcdKVEngine::new(vec![ETCD_ADDRESS.to_string()]).await.unwrap();
            let mut second_txn = client.new_txn().await;
            let key1 = KeyType::Path2INum(String::from("/"));
            let value1 = second_txn.get(&key1).await.unwrap();
            assert!(value1.is_some());
            if let Some(ValueType::INum(num)) = value1 {
                assert_eq!(num, 12);
            } else {
                panic!("wrong value type");
            }
            // let the third txn start
            first_step_tx.send(()).unwrap();
            // wait for the third txn to set the key
            second_step_rx.await.unwrap();
            let key2 = KeyType::Path2INum(String::from("/a"));
            let value2 = second_txn.get(&key2).await.unwrap();
            assert!(value2.is_some());
            if let Some(ValueType::INum(num)) = value2 {
                assert_eq!(num, 13);
            } else {
                panic!("wrong value type");
            }
            let res = second_txn.commit().await;
            // expect the commit fail
            assert!(res.is_err());
        });
        let third_handle = tokio::spawn(async move {
            let client = EtcdKVEngine::new(vec![ETCD_ADDRESS.to_string()]).await.unwrap();
            let mut third_txn = client.new_txn().await;
            // wait for the second read first key and send the signal
            first_step_rx.await.unwrap();
            let key1 = KeyType::Path2INum(String::from("/"));
            let value1 = ValueType::INum(14);
            third_txn.set(&key1, &value1).await.unwrap();
            third_txn.commit().await.unwrap();
                        // send the signal to the second txn
                        second_step_tx.send(()).unwrap();
        });
        second_handle.await.unwrap();
        third_handle.await.unwrap();
    }
}
