use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    Attribute, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    path::Path,
};
use std::fmt::{Debug, Display};
use std::sync::Arc;

/// Wrapper around an ObjectStore that sets storage class on all PUT operations
pub struct StorageClassObjectStore {
    inner: Arc<dyn ObjectStore>,
    storage_class: String,
}

impl StorageClassObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, storage_class: String) -> Self {
        Self {
            inner,
            storage_class,
        }
    }

    fn add_storage_class_to_put(&self, mut opts: PutOptions) -> PutOptions {
        let mut attrs = opts.attributes.clone();
        attrs.insert(
            Attribute::StorageClass,
            self.storage_class.clone().into(),
        );
        opts.attributes = attrs;
        opts
    }

    fn add_storage_class_to_multipart(&self, mut opts: PutMultipartOptions) -> PutMultipartOptions {
        let mut attrs = opts.attributes.clone();
        attrs.insert(
            Attribute::StorageClass,
            self.storage_class.clone().into(),
        );
        opts.attributes = attrs;
        opts
    }
}

impl Debug for StorageClassObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageClassObjectStore({:?}, class={})",
            self.inner, self.storage_class
        )
    }
}

impl Display for StorageClassObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageClassObjectStore({}, class={})",
            self.inner, self.storage_class
        )
    }
}

#[async_trait]
impl ObjectStore for StorageClassObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let opts = self.add_storage_class_to_put(PutOptions::default());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let opts = self.add_storage_class_to_put(opts);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let opts = self.add_storage_class_to_multipart(PutMultipartOptions::default());
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let opts = self.add_storage_class_to_multipart(opts);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}
