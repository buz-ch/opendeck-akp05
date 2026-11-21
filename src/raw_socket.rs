//! Per-device Unix socket that accepts raw 800x128 touch-strip frames.
//!
//! OpenDeck re-renders every dial image on a 200x100 canvas, so a full-strip
//! picture cannot arrive through `setImage`. With `strip_raw = true` each
//! device with a full-width transport instead listens on
//! `$XDG_RUNTIME_DIR/opendeck-akp05/<device-id>.sock`.
//!
//! Wire format: `type: u8`, `len: u32 (big endian)`, `payload: [u8; len]`.
//! Every message is answered with exactly one reply byte.

use std::{
    fs, io,
    io::Cursor,
    os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use image::{
    DynamicImage, ImageDecoder, Rgb, RgbImage,
    codecs::{jpeg::JpegDecoder, png::PngDecoder},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixListener,
    task::JoinSet,
    time::{sleep, timeout},
};

use crate::{
    device::StripSink,
    managed_device::{ManagedDevice, RawError},
    mappings::FullStrip,
};

pub const STRIP_WIDTH: u32 = 800;
pub const STRIP_HEIGHT: u32 = 128;
/// Largest accepted `frame` payload.
pub const MAX_PAYLOAD: usize = 512 * 1024;
/// Once a message has started, the rest must arrive within this time.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(5);

const TYPE_FRAME: u8 = 0x01;
const TYPE_CLEAR: u8 = 0x02;

/// Why a message was not carried out. The discriminant is the reply byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    Malformed = 0x01,
    Unsupported = 0x02,
    WrongSize = 0x03,
    TooLarge = 0x04,
    WriteError = 0x05,
    UnknownType = 0x06,
}

impl Reject {
    pub fn code(self) -> u8 {
        self as u8
    }

    fn describe(self) -> &'static str {
        match self {
            Reject::Malformed => "malformed message or undecodable image",
            Reject::Unsupported => "unsupported encoding or colour model",
            Reject::WrongSize => "frame is not 800x128",
            Reject::TooLarge => "payload too large",
            Reject::WriteError => "device write failed or device asleep",
            Reject::UnknownType => "unknown message type",
        }
    }

    fn bit(self) -> u8 {
        1 << (self as u8 - 1)
    }
}

/// What the socket drives. Implemented by `ManagedDevice`.
pub trait FrameSink: Send + Sync {
    /// Shows `image`. `stale` reports, once the device is ours, whether a
    /// newer frame has arrived; stale frames are dropped without a write.
    fn frame(
        &self,
        image: RgbImage,
        stale: &(dyn Fn() -> bool + Sync),
    ) -> impl Future<Output = Result<(), Reject>> + Send;

    /// Leaves raw mode.
    fn clear(&self) -> impl Future<Output = Result<(), Reject>> + Send;
}

impl<D: StripSink> FrameSink for ManagedDevice<D> {
    async fn frame(
        &self,
        image: RgbImage,
        stale: &(dyn Fn() -> bool + Sync),
    ) -> Result<(), Reject> {
        self.raw_frame(image, stale).await.map_err(map_raw_error)
    }

    async fn clear(&self) -> Result<(), Reject> {
        self.raw_clear().await.map_err(map_raw_error)
    }
}

fn map_raw_error(err: RawError) -> Reject {
    match err {
        RawError::NoStrip | RawError::NotAwake => Reject::WriteError,
        RawError::Write(err) => {
            log::error!("Raw strip write failed: {err}");
            Reject::WriteError
        }
    }
}

/// State shared by all connections to one device socket.
#[derive(Default)]
struct Shared {
    /// Ticket of the newest frame that has been read.
    latest: AtomicU64,
    /// Reasons already logged at warn level.
    logged: AtomicU8,
}

impl Shared {
    fn note(&self, device: &str, reason: Reject) {
        if self.logged.fetch_or(reason.bit(), Ordering::SeqCst) & reason.bit() == 0 {
            log::warn!(
                "Raw strip on {device}: rejected message: {}",
                reason.describe()
            );
        } else {
            log::debug!(
                "Raw strip on {device}: rejected message: {}",
                reason.describe()
            );
        }
    }
}

/// Decodes a raw frame: 8-bit RGB/RGBA PNG or RGB JPEG, exactly 800x128.
/// Alpha is composited over black. The image is never scaled.
pub fn decode_frame(bytes: &[u8]) -> Result<RgbImage, Reject> {
    match image::guess_format(bytes) {
        Ok(image::ImageFormat::Png) => decode_png(bytes),
        Ok(image::ImageFormat::Jpeg) => decode_jpeg(bytes),
        _ => Err(Reject::Unsupported),
    }
}

fn decode_png(bytes: &[u8]) -> Result<RgbImage, Reject> {
    // Read the header ourselves: the decoder would expand palette and
    // low-bit-depth images, which this mode does not accept.
    if bytes.len() < 33 || &bytes[12..16] != b"IHDR" {
        return Err(Reject::Malformed);
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let height = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
    let (bit_depth, color_type) = (bytes[24], bytes[25]);
    if bit_depth != 8 || !matches!(color_type, 2 | 6) {
        return Err(Reject::Unsupported);
    }
    if (width, height) != (STRIP_WIDTH, STRIP_HEIGHT) {
        return Err(Reject::WrongSize);
    }
    let decoder = PngDecoder::new(Cursor::new(bytes)).map_err(|_| Reject::Malformed)?;
    let image = DynamicImage::from_decoder(decoder).map_err(|_| Reject::Malformed)?;
    Ok(match image {
        DynamicImage::ImageRgb8(rgb) => rgb,
        DynamicImage::ImageRgba8(rgba) => RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
            let [r, g, b, a] = rgba.get_pixel(x, y).0;
            let over_black = |c: u8| ((u16::from(c) * u16::from(a) + 127) / 255) as u8;
            Rgb([over_black(r), over_black(g), over_black(b)])
        }),
        _ => return Err(Reject::Unsupported),
    })
}

fn decode_jpeg(bytes: &[u8]) -> Result<RgbImage, Reject> {
    let decoder = JpegDecoder::new(Cursor::new(bytes)).map_err(|_| Reject::Malformed)?;
    if decoder.color_type() != image::ColorType::Rgb8 {
        return Err(Reject::Unsupported);
    }
    if decoder.dimensions() != (STRIP_WIDTH, STRIP_HEIGHT) {
        return Err(Reject::WrongSize);
    }
    DynamicImage::from_decoder(decoder)
        .map(DynamicImage::into_rgb8)
        .map_err(|_| Reject::Malformed)
}

/// Reads exactly `len` bytes within the stall timeout.
async fn read_exact_within<R: AsyncRead + Unpin>(
    reader: &mut R,
    len: usize,
    stall: Duration,
) -> Option<Vec<u8>> {
    let mut buf = vec![0; len];
    timeout(stall, reader.read_exact(&mut buf))
        .await
        .ok()?
        .ok()?;
    Some(buf)
}

async fn reply<W: AsyncWrite + Unpin>(writer: &mut W, result: Result<(), Reject>) -> bool {
    let code = result.map_or_else(Reject::code, |()| 0x00);
    writer.write_all(&[code]).await.is_ok() && writer.flush().await.is_ok()
}

/// Serves one client until it disconnects, stalls or misbehaves.
async fn handle_connection<IO, S>(io: IO, device: &str, sink: &S, shared: &Shared, stall: Duration)
where
    IO: AsyncRead + AsyncWrite + Unpin,
    S: FrameSink,
{
    let (mut reader, mut writer) = tokio::io::split(io);
    loop {
        // An idle connection may wait as long as it likes for its next message.
        let mut kind = [0u8; 1];
        if reader.read_exact(&mut kind).await.is_err() {
            return;
        }
        // From the first byte on, the whole message must arrive promptly.
        let Some(len) = read_exact_within(&mut reader, 4, stall).await else {
            return;
        };
        let len = u32::from_be_bytes(len.try_into().unwrap()) as usize;

        if len > MAX_PAYLOAD {
            shared.note(device, Reject::TooLarge);
            reply(&mut writer, Err(Reject::TooLarge)).await;
            return;
        }
        let Some(payload) = read_exact_within(&mut reader, len, stall).await else {
            return;
        };

        let result = match kind[0] {
            TYPE_FRAME => match decode_frame(&payload) {
                Ok(image) => {
                    let ticket = shared.latest.fetch_add(1, Ordering::SeqCst) + 1;
                    let stale = || shared.latest.load(Ordering::SeqCst) != ticket;
                    log::debug!("Raw strip on {device}: frame {ticket}, {len} bytes");
                    sink.frame(image, &stale).await
                }
                Err(reason) => Err(reason),
            },
            TYPE_CLEAR if payload.is_empty() => sink.clear().await,
            TYPE_CLEAR => Err(Reject::Malformed),
            _ => Err(Reject::UnknownType),
        };
        if let Err(reason) = result {
            shared.note(device, reason);
        }
        if !reply(&mut writer, result).await {
            return;
        }
    }
}

/// Removes the socket file when the device task ends.
struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn sanitize(device_id: &str) -> String {
    let mut name: String = device_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() || name.starts_with('.') {
        name.insert(0, '_');
    }
    name
}

/// `<runtime_dir>/opendeck-akp05/<device-id>.sock`
pub fn socket_path(runtime_dir: &Path, device_id: &str) -> PathBuf {
    runtime_dir
        .join("opendeck-akp05")
        .join(format!("{}.sock", sanitize(device_id)))
}

/// Binds `path`, creating its directory (mode 0700), replacing a stale socket
/// file and restricting the socket to its owner (mode 0600).
fn bind_socket(path: &Path) -> io::Result<UnixListener> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"))?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;

    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => fs::remove_file(path)?,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a socket", path.display()),
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Listens for raw frames for as long as the returned future is polled.
/// Dropping it closes every connection and removes the socket.
pub async fn serve<S: FrameSink + 'static>(
    device_id: &str,
    transport: FullStrip,
    sink: Arc<S>,
) -> io::Result<()> {
    let runtime_dir = dirs::runtime_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no runtime directory"))?;
    let path = socket_path(&runtime_dir, device_id);
    let listener = bind_socket(&path)?;
    let _guard = SocketGuard(path.clone());
    log::info!(
        "Raw strip socket for {device_id} at {} (transport {transport:?})",
        path.display()
    );

    let shared = Arc::new(Shared::default());
    let device: Arc<str> = Arc::from(device_id);
    let mut clients = JoinSet::new();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (sink, shared, device) = (sink.clone(), shared.clone(), device.clone());
                clients.spawn(async move {
                    handle_connection(stream, &device, &*sink, &shared, STALL_TIMEOUT).await;
                });
            }
            Err(err) => {
                log::warn!("Raw strip accept failed for {device_id}: {err}");
                sleep(Duration::from_millis(100)).await;
            }
        }
        while clients.try_join_next().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Mutex,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use image::{
        ExtendedColorType, ImageEncoder, Rgba, RgbaImage,
        codecs::{jpeg::JpegEncoder, png::PngEncoder},
    };
    use tokio::io::duplex;

    use super::*;

    fn png_rgb(width: u32, height: u32) -> Vec<u8> {
        let img = RgbImage::from_pixel(width, height, Rgb([10, 20, 30]));
        let mut out = Vec::new();
        PngEncoder::new(&mut out)
            .write_image(img.as_raw(), width, height, ExtendedColorType::Rgb8)
            .unwrap();
        out
    }

    fn png_rgba(alpha: u8) -> Vec<u8> {
        let img = RgbaImage::from_pixel(800, 128, Rgba([200, 100, 50, alpha]));
        let mut out = Vec::new();
        PngEncoder::new(&mut out)
            .write_image(img.as_raw(), 800, 128, ExtendedColorType::Rgba8)
            .unwrap();
        out
    }

    fn jpeg(width: u32, height: u32, gray: bool) -> Vec<u8> {
        let mut out = Vec::new();
        let encoder = JpegEncoder::new_with_quality(&mut out, 90);
        if gray {
            let pixels = vec![128u8; (width * height) as usize];
            encoder
                .write_image(&pixels, width, height, ExtendedColorType::L8)
                .unwrap();
        } else {
            let pixels = vec![128u8; (width * height * 3) as usize];
            encoder
                .write_image(&pixels, width, height, ExtendedColorType::Rgb8)
                .unwrap();
        }
        out
    }

    #[test]
    fn accepts_rgb_png_rgba_png_and_rgb_jpeg() {
        let rgb = decode_frame(&png_rgb(800, 128)).unwrap();
        assert_eq!(rgb.dimensions(), (800, 128));
        assert_eq!(rgb.get_pixel(5, 5), &Rgb([10, 20, 30]));

        let opaque = decode_frame(&png_rgba(255)).unwrap();
        assert_eq!(opaque.get_pixel(0, 0), &Rgb([200, 100, 50]));
        let half = decode_frame(&png_rgba(128)).unwrap();
        assert_eq!(half.get_pixel(0, 0), &Rgb([100, 50, 25]));
        let clear = decode_frame(&png_rgba(0)).unwrap();
        assert_eq!(clear.get_pixel(0, 0), &Rgb([0, 0, 0]));

        let jpg = decode_frame(&jpeg(800, 128, false)).unwrap();
        assert_eq!(jpg.dimensions(), (800, 128));
    }

    #[test]
    fn rejects_wrong_sizes_without_scaling() {
        assert_eq!(decode_frame(&png_rgb(799, 128)), Err(Reject::WrongSize));
        assert_eq!(decode_frame(&png_rgb(800, 129)), Err(Reject::WrongSize));
        assert_eq!(decode_frame(&png_rgb(200, 100)), Err(Reject::WrongSize));
        assert_eq!(decode_frame(&jpeg(799, 128, false)), Err(Reject::WrongSize));
        assert_eq!(decode_frame(&jpeg(800, 129, false)), Err(Reject::WrongSize));
    }

    #[test]
    fn rejects_unsupported_encodings_and_colour_models() {
        assert_eq!(decode_frame(b"not an image"), Err(Reject::Unsupported));
        assert_eq!(decode_frame(b""), Err(Reject::Unsupported));
        assert_eq!(
            decode_frame(&jpeg(800, 128, true)),
            Err(Reject::Unsupported)
        );

        // 16-bit RGB PNG.
        let mut sixteen = Vec::new();
        PngEncoder::new(&mut sixteen)
            .write_image(
                &vec![0u8; 800 * 128 * 6],
                800,
                128,
                ExtendedColorType::Rgb16,
            )
            .unwrap();
        assert_eq!(decode_frame(&sixteen), Err(Reject::Unsupported));

        // Grayscale and palette PNG, by patching the IHDR colour type.
        for color_type in [0u8, 3, 4] {
            let mut png = png_rgb(800, 128);
            png[25] = color_type;
            assert_eq!(decode_frame(&png), Err(Reject::Unsupported));
        }
    }

    #[test]
    fn rejects_corrupt_images() {
        let mut png = png_rgb(800, 128);
        png.truncate(png.len() / 2);
        assert_eq!(decode_frame(&png), Err(Reject::Malformed));
        let mut short = png_rgb(800, 128);
        short.truncate(20);
        assert_eq!(decode_frame(&short), Err(Reject::Malformed));
        let mut jpg = jpeg(800, 128, false);
        jpg.truncate(30);
        assert_eq!(decode_frame(&jpg), Err(Reject::Malformed));
    }

    /// Records what the socket asked for.
    #[derive(Default)]
    struct MockSink {
        frames: Mutex<Vec<RgbImage>>,
        clears: AtomicUsize,
        fail: Mutex<Option<Reject>>,
    }

    impl FrameSink for MockSink {
        async fn frame(
            &self,
            image: RgbImage,
            stale: &(dyn Fn() -> bool + Sync),
        ) -> Result<(), Reject> {
            if let Some(reason) = *self.fail.lock().unwrap() {
                return Err(reason);
            }
            if !stale() {
                self.frames.lock().unwrap().push(image);
            }
            Ok(())
        }

        async fn clear(&self) -> Result<(), Reject> {
            self.clears.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn message(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![kind];
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Runs the connection handler against an in-memory client.
    fn connect(
        sink: Arc<MockSink>,
        stall: Duration,
    ) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<()>) {
        let (client, server) = duplex(2 * 1024 * 1024);
        let handle = tokio::spawn(async move {
            let shared = Shared::default();
            handle_connection(server, "test", &*sink, &shared, stall).await;
        });
        (client, handle)
    }

    async fn roundtrip(client: &mut tokio::io::DuplexStream, msg: &[u8]) -> u8 {
        client.write_all(msg).await.unwrap();
        let mut code = [0u8; 1];
        client.read_exact(&mut code).await.unwrap();
        code[0]
    }

    #[tokio::test]
    async fn frames_and_clears_get_ok_replies() {
        let sink = Arc::new(MockSink::default());
        let (mut client, _task) = connect(sink.clone(), STALL_TIMEOUT);

        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(800, 128))).await,
            0x00
        );
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(800, 128))).await,
            0x00
        );
        assert_eq!(roundtrip(&mut client, &message(0x02, &[])).await, 0x00);
        assert_eq!(sink.frames.lock().unwrap().len(), 2);
        assert_eq!(sink.clears.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reply_codes_match_the_rejection() {
        let sink = Arc::new(MockSink::default());
        let (mut client, _task) = connect(sink.clone(), STALL_TIMEOUT);

        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(799, 128))).await,
            0x03
        );
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(800, 129))).await,
            0x03
        );
        assert_eq!(roundtrip(&mut client, &message(0x01, b"junk")).await, 0x02);
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &jpeg(800, 128, true))).await,
            0x02
        );
        let mut truncated = png_rgb(800, 128);
        truncated.truncate(100);
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &truncated)).await,
            0x01
        );
        assert_eq!(roundtrip(&mut client, &message(0x02, b"x")).await, 0x01);
        assert_eq!(roundtrip(&mut client, &message(0x7f, b"xyz")).await, 0x06);
        // The connection survives all of that.
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(800, 128))).await,
            0x00
        );
        assert_eq!(sink.frames.lock().unwrap().len(), 1);

        *sink.fail.lock().unwrap() = Some(Reject::WriteError);
        assert_eq!(
            roundtrip(&mut client, &message(0x01, &png_rgb(800, 128))).await,
            0x05
        );
    }

    #[tokio::test]
    async fn oversized_payload_is_refused_and_the_connection_closed() {
        let sink = Arc::new(MockSink::default());
        let (mut client, task) = connect(sink.clone(), STALL_TIMEOUT);

        // Only the header is sent; the payload is never read.
        let mut header = vec![0x01];
        header.extend_from_slice(&((MAX_PAYLOAD + 1) as u32).to_be_bytes());
        assert_eq!(roundtrip(&mut client, &header).await, 0x04);
        task.await.unwrap();
        assert!(sink.frames.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stalled_message_closes_the_connection() {
        let sink = Arc::new(MockSink::default());
        let (mut client, task) = connect(sink, Duration::from_millis(100));

        client.write_all(&[0x01, 0, 0]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("handler must give up on a stalled client")
            .unwrap();
        let mut buf = [0u8; 1];
        assert_eq!(client.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn idle_connections_are_not_timed_out() {
        let sink = Arc::new(MockSink::default());
        let (mut client, task) = connect(sink, Duration::from_millis(50));

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!task.is_finished());
        assert_eq!(roundtrip(&mut client, &message(0x02, &[])).await, 0x00);
    }

    #[test]
    fn rejection_reasons_log_once() {
        let shared = Shared::default();
        shared.note("test", Reject::WrongSize);
        let first = shared.logged.load(Ordering::SeqCst);
        shared.note("test", Reject::WrongSize);
        assert_eq!(shared.logged.load(Ordering::SeqCst), first);
        shared.note("test", Reject::Malformed);
        assert_ne!(shared.logged.load(Ordering::SeqCst), first);
    }

    #[test]
    fn socket_names_are_sanitised() {
        let dir = Path::new("/run/user/1000");
        assert_eq!(
            socket_path(dir, "n4-ABC123"),
            Path::new("/run/user/1000/opendeck-akp05/n4-ABC123.sock")
        );
        assert_eq!(
            socket_path(dir, "../etc/x"),
            Path::new("/run/user/1000/opendeck-akp05/_.._etc_x.sock")
        );
        assert_eq!(
            socket_path(dir, ""),
            Path::new("/run/user/1000/opendeck-akp05/_.sock")
        );
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("akp05-raw-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn socket_is_private_and_stale_files_are_replaced() {
        let runtime = scratch_dir("bind");
        let path = socket_path(&runtime, "n4-test");

        let listener = bind_socket(&path).unwrap();
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(path.parent().unwrap()), 0o700);
        assert_eq!(mode(&path), 0o600);

        // A crashed plugin leaves the socket file behind.
        drop(listener);
        assert!(path.exists());
        let listener = bind_socket(&path).unwrap();

        // The replacement really accepts connections.
        let accept = tokio::spawn(async move { listener.accept().await.map(|_| ()) });
        tokio::net::UnixStream::connect(&path).await.unwrap();
        accept.await.unwrap().unwrap();

        // Something that is not a socket is never deleted.
        let other = socket_path(&runtime, "other");
        fs::write(&other, b"data").unwrap();
        assert!(bind_socket(&other).is_err());
        assert_eq!(fs::read(&other).unwrap(), b"data");

        let guard = SocketGuard(path.clone());
        drop(guard);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(&runtime);
    }
}
