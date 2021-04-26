use crate::{
    block::{self, BlockId, BlockName, BlockVersion, BLOCK_SIZE},
    crypto::{
        aead::{AeadInPlace, NewAead},
        AuthTag, Cipher, Nonce, NonceSequence, SecretKey,
    },
    db,
    error::Result,
    index::{self, BlockKind, ChildTag},
};
use std::{
    convert::TryInto,
    io::SeekFrom,
    mem,
    ops::{Deref, DerefMut},
};
use zeroize::Zeroize;

pub struct Blob {
    context: Context,
    nonce_sequence: NonceSequence,
    current_block: OpenBlock,
    len: u64,
    len_dirty: bool,
}

// TODO: figure out how to implement `flush` on `Drop`.

impl Blob {
    /// Opens an existing blob.
    ///
    /// - `directory_name` is the name of the head block of the directory containing the blob.
    ///   `None` if the blob is the root blob.
    /// - `directory_seq` is the sequence number of the blob within its directory.
    pub async fn open(
        pool: db::Pool,
        secret_key: SecretKey,
        directory_name: Option<BlockName>,
        directory_seq: u32,
    ) -> Result<Self> {
        let context = Context {
            pool,
            secret_key,
            directory_name,
            directory_seq,
        };

        // NOTE: no need to commit this transaction because we are only reading here.
        let mut tx = context.pool.begin().await?;
        let (id, buffer, auth_tag) = context.load_block(&mut tx, None, 0).await?;

        let mut content = Cursor::new(buffer);

        let nonce_sequence = NonceSequence::with_prefix(content.read_array());
        let nonce = nonce_sequence.get(0);

        context.decrypt_block(&id, &mut content, &auth_tag, &nonce)?;

        let len = content.read_u64();

        let current_block = OpenBlock {
            head_name: id.name,
            number: 0,
            id,
            content,
            dirty: false,
        };

        Ok(Self {
            context,
            nonce_sequence,
            current_block,
            len,
            len_dirty: false,
        })
    }

    /// Creates a new blob.
    ///
    /// See [`Self::open`] for explanation of `directory_name` and `directory_seq`.
    pub fn create(
        pool: db::Pool,
        secret_key: SecretKey,
        directory_name: Option<BlockName>,
        directory_seq: u32,
    ) -> Self {
        let context = Context {
            pool,
            secret_key,
            directory_name,
            directory_seq,
        };

        let nonce_sequence = NonceSequence::random();
        let mut content = Cursor::new(Buffer::new());

        content.write(&nonce_sequence.prefix()[..]);
        content.write_u64(0); // blob length

        let id = BlockId::random();
        let current_block = OpenBlock {
            head_name: id.name,
            number: 0,
            id,
            content,
            dirty: true,
        };

        Self {
            context,
            nonce_sequence,
            current_block,
            len: 0,
            len_dirty: false,
        }
    }

    /// Length of this blob in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Reads data from this blob into `buffer`, advancing the internal cursor. Returns the
    /// number of bytes actually read which might be less than `buffer.len()` if the portion of the
    /// blob past the internal cursor is smaller than `buffer.len()`.
    pub async fn read(&mut self, mut buffer: &mut [u8]) -> Result<usize> {
        let mut total_len = 0;

        loop {
            let remaining = (self.len() - self.seek_position())
                .try_into()
                .unwrap_or(usize::MAX);
            let len = buffer.len().min(remaining);
            let len = self.current_block.content.read(&mut buffer[..len]);

            buffer = &mut buffer[len..];
            total_len += len;

            if buffer.is_empty() {
                break;
            }

            let number = self.current_block.next_number();
            if number >= self.block_count() {
                break;
            }

            // NOTE: unlike in `write` we create a separate transaction for each iteration. This is
            // because if we created a single transaction for the whole `read` call, then a failed
            // read could rollback the changes made in a previous iteration which would then be
            // lost. This is fine because there is going to be at most one dirty block within
            // a single `read` invocation anyway.
            let mut tx = self.context.pool.begin().await?;

            let (id, content) = self
                .context
                .read_block(
                    &mut tx,
                    Some(&self.current_block.head_name),
                    number,
                    &self.nonce_sequence,
                )
                .await?;

            self.replace_current_block(&mut tx, number, id, content)
                .await?;

            tx.commit().await?;
        }

        Ok(total_len)
    }

    /// Writes `buffer` into this blob, advancing the blob's internal cursor.
    pub async fn write(&mut self, mut buffer: &[u8]) -> Result<()> {
        // Wrap the whole `write` in a transaction to make it atomic.
        let mut tx = self.context.pool.begin().await?;

        loop {
            let len = self.current_block.content.write(buffer);

            if len > 0 {
                self.current_block.dirty = true;
            }

            buffer = &buffer[len..];

            if self.seek_position() > self.len {
                self.len = self.seek_position();
                self.len_dirty = true;
            }

            if buffer.is_empty() {
                break;
            }

            let number = self.current_block.next_number();
            let (id, content) = if number < self.block_count() {
                self.context
                    .read_block(
                        &mut tx,
                        Some(&self.current_block.head_name),
                        number,
                        &self.nonce_sequence,
                    )
                    .await?
            } else {
                (BlockId::random(), Buffer::new())
            };

            self.replace_current_block(&mut tx, number, id, content)
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Seek to an offset in the blob.
    ///
    /// It is allowed to specify offset that is outside of the range of the blob but such offset
    /// will be clamped to be within the range.
    ///
    /// Returns the new seek position from the start of the blob.
    pub async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let offset = match pos {
            SeekFrom::Start(n) => n.min(self.len),
            SeekFrom::End(n) => {
                if n >= 0 {
                    self.len
                } else {
                    self.len.saturating_sub((-n) as u64)
                }
            }
            SeekFrom::Current(n) => {
                if n >= 0 {
                    self.seek_position().saturating_add(n as u64).min(self.len)
                } else {
                    self.seek_position().saturating_sub((-n) as u64)
                }
            }
        };

        let actual_offset = offset + self.header_size() as u64;
        let block_number = (actual_offset / BLOCK_SIZE as u64) as u32;
        let block_offset = (actual_offset % BLOCK_SIZE as u64) as usize;

        if block_number != self.current_block.number {
            let mut tx = self.context.pool.begin().await?;
            let (id, content) = self
                .context
                .read_block(
                    &mut tx,
                    Some(&self.current_block.head_name),
                    block_number,
                    &self.nonce_sequence,
                )
                .await?;
            self.replace_current_block(&mut tx, block_number, id, content)
                .await?;
            tx.commit().await?;
        }

        self.current_block.content.pos = block_offset;

        Ok(offset)
    }

    /// Flushes this blob, ensuring that all intermediately buffered contents gets written to the
    /// store.
    pub async fn flush(&mut self) -> Result<()> {
        let mut tx = self.context.pool.begin().await?;
        self.flush_in(&mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn flush_in(&mut self, tx: &mut db::Transaction) -> Result<()> {
        if !self.current_block.dirty {
            return Ok(());
        }

        self.current_block.id.version = BlockVersion::random();

        self.write_len(tx).await?;
        self.context
            .write_block(
                tx,
                Some(&self.current_block.head_name),
                self.current_block.number,
                &self.nonce_sequence,
                &self.current_block.id,
                self.current_block.content.buffer.clone(),
            )
            .await?;
        self.current_block.dirty = false;

        Ok(())
    }

    async fn replace_current_block(
        &mut self,
        tx: &mut db::Transaction,
        number: u32,
        id: BlockId,
        content: Buffer,
    ) -> Result<()> {
        self.flush_in(tx).await?;

        let mut content = Cursor::new(content);

        if number == 0 {
            // If head block, skip over the header.
            content.pos = self.header_size();
        }

        self.current_block = OpenBlock {
            head_name: self.current_block.head_name,
            number,
            id,
            content,
            dirty: false,
        };

        Ok(())
    }

    // Write the current blob length into the blob header in the head block.
    async fn write_len(&mut self, tx: &mut db::Transaction) -> Result<()> {
        if !self.len_dirty {
            return Ok(());
        }

        if self.current_block.number == 0 {
            let old_pos = self.current_block.content.pos;
            self.current_block.content.pos = self.nonce_sequence.prefix().len();
            self.current_block.content.write_u64(self.len);
            self.current_block.content.pos = old_pos;
            self.current_block.dirty = true;
        } else {
            let (mut id, buffer) = self
                .context
                .read_block(tx, None, 0, &self.nonce_sequence)
                .await?;

            let mut cursor = Cursor::new(buffer);
            cursor.pos = self.nonce_sequence.prefix().len();
            cursor.write_u64(self.len);
            id.version = BlockVersion::random();

            self.context
                .write_block(tx, None, 0, &self.nonce_sequence, &id, cursor.buffer)
                .await?;
        }

        self.len_dirty = false;

        Ok(())
    }

    // Total number of blocks in this blob including the possibly partially filled final block.
    fn block_count(&self) -> u32 {
        // https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c
        (1 + (self.len + self.header_size() as u64 - 1) / BLOCK_SIZE as u64)
            .try_into()
            .unwrap_or(u32::MAX)
    }

    // Returns the current seek position from the start of the blob.
    fn seek_position(&self) -> u64 {
        self.current_block.number as u64 * BLOCK_SIZE as u64 + self.current_block.content.pos as u64
            - self.header_size() as u64
    }

    fn header_size(&self) -> usize {
        self.nonce_sequence.prefix().len() + mem::size_of_val(&self.len)
    }
}

struct Context {
    pool: db::Pool,
    secret_key: SecretKey,
    directory_name: Option<BlockName>,
    directory_seq: u32,
}

impl Context {
    async fn read_block(
        &self,
        tx: &mut db::Transaction,
        head_name: Option<&BlockName>,
        number: u32,
        nonce_sequence: &NonceSequence,
    ) -> Result<(BlockId, Buffer)> {
        let (id, mut buffer, auth_tag) = self.load_block(tx, head_name, number).await?;

        let nonce = nonce_sequence.get(number);
        let offset = if number == 0 {
            nonce_sequence.prefix().len()
        } else {
            0
        };

        self.decrypt_block(&id, &mut buffer[offset..], &auth_tag, &nonce)?;

        Ok((id, buffer))
    }

    async fn load_block(
        &self,
        tx: &mut db::Transaction,
        head_name: Option<&BlockName>,
        number: u32,
    ) -> Result<(BlockId, Buffer, AuthTag)> {
        let id = if let Some(child_tag) = self.child_tag(head_name, number) {
            index::get(tx, &child_tag).await?
        } else {
            index::get_root(tx).await?
        };

        let mut content = Buffer::new();
        let auth_tag = block::read(tx, &id, &mut content).await?;

        Ok((id, content, auth_tag))
    }

    fn decrypt_block(
        &self,
        id: &BlockId,
        buffer: &mut [u8],
        auth_tag: &AuthTag,
        nonce: &Nonce,
    ) -> Result<()> {
        let aad = id.to_array(); // "additional associated data"
        let cipher = Cipher::new(self.secret_key.as_array());
        cipher.decrypt_in_place_detached(&nonce, &aad, buffer, &auth_tag)?;

        Ok(())
    }

    async fn write_block(
        &self,
        tx: &mut db::Transaction,
        head_name: Option<&BlockName>,
        number: u32,
        nonce_sequence: &NonceSequence,
        id: &BlockId,
        mut buffer: Buffer,
    ) -> Result<()> {
        let nonce = nonce_sequence.get(number);
        let aad = id.to_array(); // "additional associated data"

        let offset = if number == 0 {
            nonce_sequence.prefix().len()
        } else {
            0
        };

        let cipher = Cipher::new(self.secret_key.as_array());
        let auth_tag = cipher.encrypt_in_place_detached(&nonce, &aad, &mut buffer[offset..])?;

        block::write(tx, id, &buffer, &auth_tag).await?;

        if let Some(child_tag) = self.child_tag(head_name, number) {
            index::insert(tx, id, &child_tag).await?;
        } else {
            index::insert_root(tx, id).await?;
        }

        Ok(())
    }

    fn child_tag(&self, head_name: Option<&BlockName>, number: u32) -> Option<ChildTag> {
        match (number, &self.directory_name) {
            (0, None) => None, // root
            (0, Some(directory_name)) => Some(ChildTag::new(
                &self.secret_key,
                directory_name,
                self.directory_seq,
                BlockKind::Head,
            )),
            (_, _) => Some(ChildTag::new(
                &self.secret_key,
                head_name.expect("head name is required for trunk blocks"),
                number,
                BlockKind::Trunk,
            )),
        }
    }
}

// Data for a block that's been loaded into memory and decrypted.
struct OpenBlock {
    // Name of the head block of the blob. If this `OpenBlock` represents the head block, this is
    // the same as `id.name`.
    head_name: BlockName,
    // Number of this blob within the blob. Head block's number is 0, then next one is 1, and so
    // on...
    number: u32,
    // Id of the block.
    id: BlockId,
    // Decrypted content of the block wrapped in `Cursor` to track the current seek position.
    content: Cursor,
    // Was this block modified since the last time it was loaded from/saved to the store?
    dirty: bool,
}

impl OpenBlock {
    fn next_number(&self) -> u32 {
        // TODO: return error instead of panic
        self.number
            .checked_add(1)
            .expect("block count limit exceeded")
    }
}

// Buffer for keeping loaded block content and also for in-place encryption and decryption.
#[derive(Clone)]
struct Buffer(Box<[u8]>);

impl Buffer {
    fn new() -> Self {
        Self(vec![0; BLOCK_SIZE].into_boxed_slice())
    }
}

// Scramble the buffer on drop to prevent leaving decrypted data in memory past the buffer
// lifetime.
impl Drop for Buffer {
    fn drop(&mut self) {
        self.0.zeroize()
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Wrapper for `Buffer` with an internal position which advances when data is read from or
// written to the buffer.
struct Cursor {
    buffer: Buffer,
    pos: usize,
}

impl Cursor {
    fn new(buffer: Buffer) -> Self {
        Self { buffer, pos: 0 }
    }

    // Reads data from the buffer into `dst` and advances the internal position. Returns the
    // number of bytes actual read.
    fn read(&mut self, dst: &mut [u8]) -> usize {
        let n = (self.buffer.len() - self.pos).min(dst.len());
        dst[..n].copy_from_slice(&self.buffer[self.pos..self.pos + n]);
        self.pos += n;
        n
    }

    // Read data from the buffer into a fixed-length array.
    //
    // # Panics
    //
    // Panics if the remaining length is less than `N`.
    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let array = self.buffer[self.pos..self.pos + N].try_into().unwrap();
        self.pos += N;
        array
    }

    // Read data from the buffer into a `u64`.
    //
    // # Panics
    //
    // Panics if the remaining length is less than `size_of::<u64>()`
    fn read_u64(&mut self) -> u64 {
        u64::from_le_bytes(self.read_array())
    }

    // Writes data from `dst` into the buffer and advances the internal position. Returns the
    // number of bytes actually written.
    fn write(&mut self, src: &[u8]) -> usize {
        let n = (self.buffer.len() - self.pos).min(src.len());
        self.buffer[self.pos..self.pos + n].copy_from_slice(&src[..n]);
        self.pos += n;
        n
    }

    // Write a `u64` into the buffer.
    //
    // # Panics
    //
    // Panics if the remaining length is less than `size_of::<u64>()`
    fn write_u64(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        assert!(self.buffer.len() - self.pos >= bytes.len());
        self.write(&bytes[..]);
    }
}

impl Deref for Cursor {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[self.pos..]
    }
}

impl DerefMut for Cursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[self.pos..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rand::{distributions::Standard, prelude::*};
    use std::future::Future;

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_blob() {
        let pool = init_db().await;
        let secret_key = SecretKey::random();

        let mut blob = Blob::create(pool.clone(), secret_key.clone(), None, 0);
        blob.flush().await.unwrap();

        // Re-open the blob and read its contents.
        let mut blob = Blob::open(pool.clone(), secret_key.clone(), None, 0)
            .await
            .unwrap();

        let mut buffer = [0; 1];
        assert_eq!(blob.read(&mut buffer[..]).await.unwrap(), 0);
    }

    // Arguments for the `write_and_read` test.
    #[derive(Debug)]
    struct WriteAndReadArgs {
        blob_len: usize,
        write_len: usize,
        read_len: usize,
        rng_seed: u64,
    }

    impl WriteAndReadArgs {
        fn strategy() -> impl Strategy<Value = Self> {
            (1..3 * BLOCK_SIZE)
                .prop_flat_map(|blob_len| {
                    (
                        Just(blob_len),
                        1..=blob_len,
                        1..=blob_len + 1,
                        any::<u64>().no_shrink(),
                    )
                })
                .prop_map(|(blob_len, write_len, read_len, rng_seed)| Self {
                    blob_len,
                    write_len,
                    read_len,
                    rng_seed,
                })
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn write_and_read(args in WriteAndReadArgs::strategy()) {
            run(write_and_read_case(
                args.blob_len,
                args.write_len,
                args.read_len,
                args.rng_seed,
            ))
        }

        #[test]
        fn len(content_len in 0..3 * BLOCK_SIZE, rng_seed in any::<u64>().no_shrink()) {
            run(len_case(content_len, rng_seed))
        }
    }

    async fn write_and_read_case(
        blob_len: usize,
        write_len: usize,
        read_len: usize,
        rng_seed: u64,
    ) {
        let mut rng = StdRng::seed_from_u64(rng_seed);
        let secret_key = SecretKey::generate(&mut rng);
        let pool = init_db().await;

        // Create the blob and write to it in chunks of `write_len` bytes.
        let mut blob = Blob::create(pool.clone(), secret_key.clone(), None, 0);

        let orig_content: Vec<u8> = (&mut rng).sample_iter(Standard).take(blob_len).collect();

        for chunk in orig_content.chunks(write_len) {
            blob.write(chunk).await.unwrap();
        }

        blob.flush().await.unwrap();

        // Re-open the blob and read from it in chunks of `read_len` bytes
        let mut blob = Blob::open(pool.clone(), secret_key.clone(), None, 0)
            .await
            .unwrap();

        let mut read_content = vec![0; 0];
        let mut read_buffer = vec![0; read_len];

        loop {
            let len = blob.read(&mut read_buffer[..]).await.unwrap();

            if len == 0 {
                break; // done
            }

            read_content.extend(&read_buffer[..len]);
        }

        assert_eq!(orig_content.len(), read_content.len());
        assert_eq!(
            orig_content
                .iter()
                .zip(&read_content)
                .position(|(orig, read)| orig != read),
            None
        );
    }

    async fn len_case(content_len: usize, rng_seed: u64) {
        let mut rng = StdRng::seed_from_u64(rng_seed);
        let secret_key = SecretKey::generate(&mut rng);
        let pool = init_db().await;

        let content: Vec<u8> = rng.sample_iter(Standard).take(content_len).collect();

        let mut blob = Blob::create(pool.clone(), secret_key.clone(), None, 0);
        blob.write(&content[..]).await.unwrap();
        assert_eq!(blob.len(), content_len as u64);

        blob.flush().await.unwrap();
        assert_eq!(blob.len(), content_len as u64);

        let blob = Blob::open(pool.clone(), secret_key.clone(), None, 0)
            .await
            .unwrap();
        assert_eq!(blob.len(), content_len as u64);
    }

    // proptest currently doesn't work with the `#[tokio::test]` macro - we need to create the
    // runtime manually.
    fn run<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(future)
    }

    /*

    #[tokio::test(flavor = "multi_thread")]
    async fn seek() {
        let pool = init_db().await;
        let secret_key = SecretKey::random();

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(Standard)
            .take(5 * BLOCK_SIZE / 2)
            .collect();

        let mut blob = Blob::create(pool.clone(), secret_key.clone(), None, 0);
        blob.write(&content[..]).await.unwrap();
        blob.flush().await.unwrap();

        let mut buffer = vec![0; 1024];
        let len = blob.read(&mut buffer[..]).await.unwrap();
        assert_eq!(len, 0);

        // Seek from the start
        for &offset in &[
            0,
            1,
            2,
            3,
            100,
            1014,
            BLOCK_SIZE as u64,
            2 * BLOCK_SIZE as u64,
            content.len() as u64 - 2,
            content.len() as u64 - 1,
            content.len() as u64,
        ] {
            assert_eq!(blob.seek(SeekFrom::Start(offset)).await.unwrap(), offset);
            let len = blob.read(&mut buffer[..]).await.unwrap();
            assert_eq!(len, buffer.len().min(content.len() - offset as usize));
            assert_eq!(
                buffer[..len],
                content[offset as usize..offset as usize + len]
            );
        }

        // Seek past the end
        assert_eq!(
            blob.seek(SeekFrom::Start(content.len() as u64 + 1))
                .await
                .unwrap(),
            content.len() as u64
        );

        // Seek from the end
        assert_eq!(
            blob.seek(SeekFrom::End(0)).await.unwrap(),
            content.len() as u64
        );
        assert_eq!(
            blob.seek(SeekFrom::End(-1)).await.unwrap(),
            content.len() as u64 - 1
        );
        assert_eq!(
            blob.seek(SeekFrom::End(-(content.len() as i64)))
                .await
                .unwrap(),
            0
        );

        // Seek past the start
        assert_eq!(
            blob.seek(SeekFrom::End(-(content.len() as i64) - 1))
                .await
                .unwrap(),
            0
        );

        // Seek past the end
        assert_eq!(
            blob.seek(SeekFrom::End(1)).await.unwrap(),
            content.len() as u64
        );

        // Rewind and seek from the current position
        blob.seek(SeekFrom::Start(0)).await.unwrap();

        assert_eq!(blob.seek(SeekFrom::Current(0)).await.unwrap(), 0);
        assert_eq!(blob.seek(SeekFrom::Current(1)).await.unwrap(), 1);
        assert_eq!(blob.seek(SeekFrom::Current(1)).await.unwrap(), 2);
        assert_eq!(blob.seek(SeekFrom::Current(-1)).await.unwrap(), 1);

        // Seek past the start
        assert_eq!(blob.seek(SeekFrom::Current(-2)).await.unwrap(), 0);

        // Seek past the end
        assert_eq!(
            blob.seek(SeekFrom::Current(content.len() as i64 + 1))
                .await
                .unwrap(),
            content.len() as u64
        );
    }
    */

    async fn init_db() -> db::Pool {
        let pool = db::Pool::connect(":memory:").await.unwrap();
        index::init(&pool).await.unwrap();
        block::init(&pool).await.unwrap();
        pool
    }
}
