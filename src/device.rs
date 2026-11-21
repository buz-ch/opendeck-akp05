use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use data_url::DataUrl;
use image::{DynamicImage, GenericImageView, RgbImage, load_from_memory_with_format};
use mirajazz::{
    device::Device,
    error::MirajazzError,
    images::convert_image_with_format,
    state::DeviceStateUpdate,
    types::{ImageFormat, ImageMirroring, ImageMode, ImageRotation},
};
use openaction::{device_plugin, global_events::SetImageEvent};
use tokio::time::{interval, timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    DEVICES, config, led_config,
    managed_device::ManagedDevice,
    mappings::{
        COL_COUNT, CandidateDevice, DEVICE_TYPE, ENCODER_COUNT, FullStrip, KEY_COUNT, Kind,
        ROW_COUNT,
    },
};

const FULL_STRIP_FORMAT: ImageFormat = ImageFormat {
    mode: ImageMode::JPEG,
    size: (800, 128),
    rotation: ImageRotation::Rot180,
    mirror: ImageMirroring::None,
};

/// Initializes a device and listens for events
pub async fn device_task(candidate: CandidateDevice, token: CancellationToken) {
    log::info!("Running device task for {:?}", candidate);

    let initialize = async {
        let device = connect(&candidate).await?;
        let cfg = config::load();
        let firmware = device.firmware_version.as_deref();
        let full_strip = candidate.kind.full_strip(firmware, cfg.bgpic);

        if let Some(full_strip) = full_strip {
            log::info!(
                "Using full-width touch strip via {full_strip:?} on {:04x}:{:04x}, firmware {}",
                device.vid,
                device.pid,
                firmware
                    .map(|version| version.trim_end_matches('\0'))
                    .unwrap_or("unknown"),
            );
        }

        let strip = full_strip.map(StripCanvas::new);
        if cfg.strip_raw && full_strip.is_none() {
            log::warn!(
                "strip_raw is enabled but {} has no full-width strip transport; raw mode is unavailable",
                candidate.id
            );
        }
        let raw_strip = full_strip.filter(|_| cfg.strip_raw);
        device.set_brightness(50).await?;
        clear_display(&device, strip.as_ref()).await?;

        log::info!("Applying config: {:?}", cfg);
        if let Some(led_config::LedMode::Static { colors }) = cfg.leds.mode {
            device.set_led_brightness(cfg.leds.brightness).await?;
            device.set_led_colors(&colors).await?;
        }

        if let Some(enabled) = cfg.vibration {
            if candidate.kind.supports_vibration() {
                log::info!("Setting vibration: {}", enabled);
                device.set_vibration(enabled).await?;
            } else {
                log::warn!(
                    "Ignoring vibration config: {} does not support it",
                    candidate.kind.human_name()
                );
            }
        }

        Ok((device, strip, raw_strip))
    };
    let device: Result<_, MirajazzError> = tokio::select! {
        biased;
        _ = token.cancelled() => return,
        result = initialize => result,
    };
    let (device, strip, raw_strip) = match device {
        Ok(device) => device,
        Err(err) => {
            log::error!("Device {} initialization failed: {err}", candidate.id);
            return;
        }
    };

    let device = Arc::new(ManagedDevice::new(device, token.clone(), strip));
    tokio::select! {
        biased;
        _ = token.cancelled() => {},
        _ = async {
            // Publish before registering: OpenDeck may immediately send images.
            DEVICES.write().await.insert(candidate.id.clone(), device.clone());
            if let Err(err) = device_plugin::register_device(
                candidate.id.clone(), candidate.kind.human_name(),
                ROW_COUNT as u8, COL_COUNT as u8, ENCODER_COUNT as u8, DEVICE_TYPE,
            ).await {
                log::error!("Device {} registration failed: {err}", candidate.id);
                return;
            }
            tokio::select! {
                _ = device_events_task(&candidate, &device) => {},
                _ = keepalive_task(&candidate, &device) => {},
                _ = raw_socket_task(&candidate, &device, raw_strip) => {},
            }
        } => {},
    };

    token.cancel();
    log::info!("Shutting down device {:?}", candidate);

    if timeout(Duration::from_secs(2), device.shutdown())
        .await
        .is_err()
    {
        log::warn!("Device {} shutdown timed out", candidate.id);
    }

    log::info!("Device task finished for {:?}", candidate);
}

/// Serves the raw strip socket for as long as the device runs. Never returns:
/// a failure to serve leaves the device running without raw mode.
#[cfg(unix)]
async fn raw_socket_task(
    candidate: &CandidateDevice,
    device: &Arc<ManagedDevice>,
    raw_strip: Option<FullStrip>,
) {
    if let Some(transport) = raw_strip
        && let Err(err) = crate::raw_socket::serve(&candidate.id, transport, device.clone()).await
    {
        log::warn!("Raw strip socket for {} unavailable: {err}", candidate.id);
    }
    std::future::pending::<()>().await;
}

#[cfg(not(unix))]
async fn raw_socket_task(
    candidate: &CandidateDevice,
    _device: &Arc<ManagedDevice>,
    raw_strip: Option<FullStrip>,
) {
    if raw_strip.is_some() {
        log::warn!(
            "strip_raw is only supported on Unix; raw mode unavailable for {}",
            candidate.id
        );
    }
    std::future::pending::<()>().await;
}

pub async fn connect(candidate: &CandidateDevice) -> Result<Device, MirajazzError> {
    let result = Device::connect(
        &candidate.dev,
        candidate.kind.protocol_version(),
        KEY_COUNT,
        ENCODER_COUNT,
    )
    .await;

    match result {
        Ok(device) => {
            Ok(device
                .with_supports_both_encoder_states(candidate.kind.supports_both_encoder_states()))
        }
        Err(e) => {
            log::error!("Error while connecting to device: {e}");

            Err(e)
        }
    }
}

/// Handles events from device to OpenDeck
async fn device_events_task(
    candidate: &CandidateDevice,
    device: &ManagedDevice,
) -> Result<(), MirajazzError> {
    log::info!("Connecting to {} for incoming events", candidate.id);

    let reader = device.get_reader();

    log::info!("Connected to {} for incoming events", candidate.id);

    log::info!("Reader is ready for {}", candidate.id);

    loop {
        log::debug!("Reading updates...");

        let updates = match reader.read(None).await {
            Ok(updates) => updates,
            Err(e) => {
                if !device.handle_error(&candidate.id, e) {
                    break;
                }

                continue;
            }
        };

        for update in updates {
            log::debug!("New update: {:#?}", update);

            let id = candidate.id.clone();

            match update {
                DeviceStateUpdate::ButtonDown(key) => {
                    device_plugin::key_down(id, key).await.unwrap()
                }
                DeviceStateUpdate::ButtonUp(key) => device_plugin::key_up(id, key).await.unwrap(),
                DeviceStateUpdate::EncoderDown(encoder) => {
                    device_plugin::encoder_down(id, encoder).await.unwrap();
                }
                DeviceStateUpdate::EncoderUp(encoder) => {
                    device_plugin::encoder_up(id, encoder).await.unwrap();
                }
                DeviceStateUpdate::EncoderTwist(encoder, val) => {
                    device_plugin::encoder_change(id, encoder, val as i16)
                        .await
                        .unwrap();
                }
            }
        }
    }

    Ok(())
}

/// Sends periodic keepalives to the device to maintain connection
async fn keepalive_task(
    candidate: &CandidateDevice,
    device: &ManagedDevice,
) -> Result<(), MirajazzError> {
    let mut interval = interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        log::debug!("Sending keepalive to {}", candidate.id);

        let Err(e) = device
            .with_awake_device(async |device| device.keep_alive().await)
            .await
        else {
            continue;
        };
        if !device.handle_error(&candidate.id, e) {
            break;
        }
    }

    Ok(())
}

fn map_position(mut position: u8, is_encoder: bool) -> Result<u8, MirajazzError> {
    if is_encoder {
        position += 10;
    }
    let position = match position {
        0 => 10,
        1 => 11,
        2 => 12,
        3 => 13,
        4 => 14,
        5 => 5,
        6 => 6,
        7 => 7,
        8 => 8,
        9 => 9,
        10 => 0,
        11 => 1,
        12 => 2,
        13 => 3,
        _ => {
            log::error!("Invalid key position");
            return Err(MirajazzError::BadData);
        }
    };
    Ok(position)
}

/// Handles different combinations of "set image" event, including clearing the specific buttons and whole device
pub async fn handle_set_image(
    device: &Device,
    evt: SetImageEvent,
    strip: Option<&StripCanvas>,
) -> Result<(), MirajazzError> {
    let is_encoder = evt.controller.as_deref() == Some("Encoder");
    let kind = Kind::from_vid_pid(device.vid, device.pid).unwrap();
    let encoder_strip = strip.filter(|_| is_encoder);
    match (evt.position, evt.image) {
        (Some(position), Some(image)) => {
            log::debug!("Setting image for button {}", position);
            let dial = position;
            let position = map_position(position, is_encoder)?;

            // OpenDeck sends image as a data url, so parse it using a library
            let url = DataUrl::process(image.as_str()).unwrap(); // Isn't expected to fail, so unwrap it is
            let (body, _fragment) = url.decode_to_vec().unwrap(); // Same here

            // Allow only image/jpeg mime for now
            if url.mime_type().subtype != "jpeg" {
                log::error!("Incorrect mime type: {}", url.mime_type());

                return Ok(()); // Not a fatal error, enough to just log it
            }

            let image = load_from_memory_with_format(body.as_slice(), image::ImageFormat::Jpeg)?;

            if let Some(strip) = encoder_strip {
                return strip.set_dial(device, dial, Some(image)).await;
            }

            let format = if is_encoder {
                kind.touch_image_format()
            } else {
                kind.image_format()
            };
            // The panel conversion stretches into `format.size`, which distorts
            // dial images (OpenDeck renders 200x100, the N4 strip is 176x112):
            // pre-fit encoder images preserving aspect ratio, so that stretch
            // becomes a no-op.
            let image = if is_encoder {
                fit_into(image, format.size)
            } else {
                image
            };

            device.set_button_image(position, format, image).await?;
            device.flush().await?;
        }
        (Some(position), None) => {
            if let Some(strip) = encoder_strip {
                return strip.set_dial(device, position, None).await;
            }
            let position = map_position(position, is_encoder)?;
            device.clear_button_image(position).await?;
            device.flush().await?;
        }
        (None, None) => {
            clear_display(device, strip).await?;
        }
        _ => {}
    }

    Ok(())
}

/// Where the finished 800x128 strip picture is written.
pub trait StripSink: Send + Sync {
    fn write_strip(
        &self,
        mode: FullStrip,
        image: DynamicImage,
    ) -> impl Future<Output = Result<(), MirajazzError>> + Send;
}

impl StripSink for Device {
    async fn write_strip(&self, mode: FullStrip, image: DynamicImage) -> Result<(), MirajazzError> {
        match mode {
            FullStrip::ImageSlot(index) => {
                self.set_button_image(index, FULL_STRIP_FORMAT, image)
                    .await?;
                self.flush().await
            }
            FullStrip::Bgpic => send_strip_bgpic(self, image).await,
        }
    }
}

/// One continuous strip canvas per connection, with a quarter for each dial.
///
/// `pixels` is always the composed view. While raw mode is active, dial
/// updates still change `pixels` but are not written, so leaving raw mode can
/// restore the composed view.
pub struct StripCanvas {
    mode: FullStrip,
    pixels: Mutex<RgbImage>,
    raw: AtomicBool,
    raw_announced: AtomicBool,
}

impl StripCanvas {
    pub fn new(mode: FullStrip) -> Self {
        let (width, height) = FULL_STRIP_FORMAT.size;
        Self {
            mode,
            pixels: Mutex::new(RgbImage::new(width as u32, height as u32)),
            raw: AtomicBool::new(false),
            raw_announced: AtomicBool::new(false),
        }
    }

    /// The transport the strip is written through.
    pub fn mode(&self) -> FullStrip {
        self.mode
    }

    /// Whether the strip currently shows a raw frame.
    pub fn is_raw(&self) -> bool {
        self.raw.load(Ordering::SeqCst)
    }

    fn update(&self, dial: u8, image: Option<DynamicImage>) -> Result<DynamicImage, MirajazzError> {
        let dial = usize::from(dial);
        if dial >= ENCODER_COUNT {
            log::error!("Invalid dial position {dial}");
            return Err(MirajazzError::BadData);
        }

        let (strip_width, height) = FULL_STRIP_FORMAT.size;
        let width = strip_width / ENCODER_COUNT;
        let tile = match image {
            Some(image) => fit_into(image, (width, height)).into_rgb8(),
            None => RgbImage::new(width as u32, height as u32),
        };
        let mut pixels = self.pixels.lock().unwrap();
        image::imageops::replace(&mut *pixels, &tile, (dial * width) as i64, 0);
        Ok(DynamicImage::ImageRgb8(pixels.clone()))
    }

    pub async fn set_dial<S: StripSink>(
        &self,
        sink: &S,
        dial: u8,
        image: Option<DynamicImage>,
    ) -> Result<(), MirajazzError> {
        let image = self.update(dial, image)?;
        if self.is_raw() {
            return Ok(());
        }
        sink.write_strip(self.mode, image).await
    }

    /// Shows `frame` 1:1 and enters raw mode once the write has succeeded.
    pub async fn set_raw<S: StripSink>(
        &self,
        sink: &S,
        frame: RgbImage,
    ) -> Result<(), MirajazzError> {
        sink.write_strip(self.mode, DynamicImage::ImageRgb8(frame))
            .await?;
        self.raw.store(true, Ordering::SeqCst);
        if !self.raw_announced.swap(true, Ordering::SeqCst) {
            let transport = match self.mode {
                FullStrip::ImageSlot(_) => "image slot",
                FullStrip::Bgpic => "BGPIC",
            };
            log::info!("Raw 800x128 strip active via {transport}");
        }
        Ok(())
    }

    /// Blanks the composed view and leaves raw mode. Returns the blank strip
    /// when the transport needs it written explicitly (BGPIC).
    fn blank(&self) -> Option<DynamicImage> {
        let mut pixels = self.pixels.lock().unwrap();
        pixels.fill(0);
        self.raw.store(false, Ordering::SeqCst);
        (self.mode == FullStrip::Bgpic).then(|| DynamicImage::ImageRgb8(pixels.clone()))
    }

    /// Leaves raw mode and shows the composed view again. Does nothing when
    /// raw mode is not active.
    pub async fn clear_raw<S: StripSink>(&self, sink: &S) -> Result<(), MirajazzError> {
        if !self.is_raw() {
            return Ok(());
        }
        let composed = DynamicImage::ImageRgb8(self.pixels.lock().unwrap().clone());
        sink.write_strip(self.mode, composed).await?;
        self.raw.store(false, Ordering::SeqCst);
        Ok(())
    }
}

async fn clear_display(device: &Device, strip: Option<&StripCanvas>) -> Result<(), MirajazzError> {
    let background = strip.and_then(StripCanvas::blank);
    device.clear_all_button_images().await?;
    if let Some(background) = background {
        return send_strip_bgpic(device, background).await;
    }
    device.flush().await
}

async fn send_strip_bgpic(device: &Device, image: DynamicImage) -> Result<(), MirajazzError> {
    let jpeg = convert_image_with_format(FULL_STRIP_FORMAT, image).await?;
    let len = u32::try_from(jpeg.len()).map_err(|_| MirajazzError::BadData)?;
    let (width, height) = FULL_STRIP_FORMAT.size;
    let mut report = b"\0CRT\0\0BGPIC".to_vec();
    report.extend_from_slice(&len.to_be_bytes());
    // BGPIC encodes x, y, width and height as big-endian u16s, then 0 and the layer.
    for value in [0, 0, width as u16, height as u16] {
        report.extend_from_slice(&value.to_be_bytes());
    }
    report.extend_from_slice(&[0, 0]);
    device.write_extended_data(&mut report).await?;

    // All supported devices use 1024-byte HID payloads after the report ID.
    for chunk in jpeg.chunks(1024) {
        report[1..1 + chunk.len()].copy_from_slice(chunk);
        report[1 + chunk.len()..].fill(0);
        device.write_data(&report).await?;
    }

    device
        .write_extended_data(&mut b"\0CRT\0\0STP".to_vec())
        .await
}

/// Fit `image` inside `size` preserving aspect ratio, with black padding.
/// `set_button_image` stretches into the panel format, which distorts images
/// whose aspect differs (e.g. OpenDeck's 200x100 dial image on the 176x112
/// touch strip); pre-fitting here keeps shapes intact at the cost of small
/// letterbox bars.
fn fit_into(image: DynamicImage, size: (usize, usize)) -> DynamicImage {
    let (ws, hs) = (size.0 as u32, size.1 as u32);
    let (w, h) = image.dimensions();
    if w == ws && h == hs {
        return image;
    }
    let scale = (ws as f32 / w as f32).min(hs as f32 / h as f32);
    let nw = ((w as f32 * scale).round() as u32).clamp(1, ws);
    let nh = ((h as f32 * scale).round() as u32).clamp(1, hs);
    let fitted = image
        .resize_exact(nw, nh, image::imageops::FilterType::Nearest)
        .into_rgb8();
    let mut canvas = RgbImage::new(ws, hs);
    image::imageops::replace(
        &mut canvas,
        &fitted,
        i64::from((ws - nw) / 2),
        i64::from((hs - nh) / 2),
    );
    DynamicImage::ImageRgb8(canvas)
}

#[cfg(test)]
pub(crate) mod test_sink {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use image::{DynamicImage, RgbImage};
    use mirajazz::error::MirajazzError;

    use super::{FullStrip, StripSink};

    /// Records every strip write instead of talking to a device.
    #[derive(Default)]
    pub(crate) struct RecordingSink {
        pub writes: Mutex<Vec<RgbImage>>,
        pub fail: AtomicBool,
    }

    impl StripSink for RecordingSink {
        async fn write_strip(
            &self,
            _mode: FullStrip,
            image: DynamicImage,
        ) -> Result<(), MirajazzError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(MirajazzError::BadData);
            }
            self.writes.lock().unwrap().push(image.into_rgb8());
            Ok(())
        }
    }

    impl RecordingSink {
        pub(crate) fn count(&self) -> usize {
            self.writes.lock().unwrap().len()
        }

        pub(crate) fn last(&self) -> RgbImage {
            self.writes.lock().unwrap().last().cloned().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_sink::RecordingSink;
    use super::*;
    use image::Rgb;

    fn solid(width: u32, height: u32, color: [u8; 3]) -> RgbImage {
        RgbImage::from_pixel(width, height, Rgb(color))
    }

    #[tokio::test]
    async fn dial_updates_during_raw_mode_update_canvas_without_writing() {
        let sink = RecordingSink::default();
        let strip = StripCanvas::new(FullStrip::ImageSlot(3));
        let red = DynamicImage::ImageRgb8(solid(200, 100, [255, 0, 0]));
        let blue = DynamicImage::ImageRgb8(solid(200, 100, [0, 0, 255]));

        strip.set_dial(&sink, 0, Some(red)).await.unwrap();
        strip
            .set_raw(&sink, solid(800, 128, [0, 255, 0]))
            .await
            .unwrap();
        assert!(strip.is_raw());
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.last(), solid(800, 128, [0, 255, 0]));

        strip.set_dial(&sink, 1, Some(blue)).await.unwrap();
        assert_eq!(sink.count(), 2, "dial updates must not touch a raw strip");

        strip.clear_raw(&sink).await.unwrap();
        assert!(!strip.is_raw());
        assert_eq!(sink.count(), 3);
        let composed = sink.last();
        assert_eq!(composed.get_pixel(100, 64), &Rgb([255, 0, 0]));
        assert_eq!(composed.get_pixel(300, 64), &Rgb([0, 0, 255]));
        assert_eq!(composed.get_pixel(300, 5), &Rgb([0, 0, 0]));
    }

    #[tokio::test]
    async fn clear_raw_without_dial_images_blanks_the_strip() {
        let sink = RecordingSink::default();
        let strip = StripCanvas::new(FullStrip::Bgpic);
        strip
            .set_raw(&sink, solid(800, 128, [9, 9, 9]))
            .await
            .unwrap();
        strip.clear_raw(&sink).await.unwrap();
        assert_eq!(sink.last(), solid(800, 128, [0, 0, 0]));
    }

    #[tokio::test]
    async fn clear_raw_when_inactive_writes_nothing() {
        let sink = RecordingSink::default();
        let strip = StripCanvas::new(FullStrip::ImageSlot(3));
        strip.clear_raw(&sink).await.unwrap();
        assert_eq!(sink.count(), 0);
        assert!(!strip.is_raw());
    }

    #[tokio::test]
    async fn failed_write_does_not_start_raw_mode_but_keeps_it_once_started() {
        let sink = RecordingSink::default();
        let strip = StripCanvas::new(FullStrip::ImageSlot(3));
        sink.fail.store(true, Ordering::SeqCst);
        assert!(
            strip
                .set_raw(&sink, solid(800, 128, [1, 2, 3]))
                .await
                .is_err()
        );
        assert!(!strip.is_raw());

        sink.fail.store(false, Ordering::SeqCst);
        strip
            .set_raw(&sink, solid(800, 128, [1, 2, 3]))
            .await
            .unwrap();
        sink.fail.store(true, Ordering::SeqCst);
        assert!(
            strip
                .set_raw(&sink, solid(800, 128, [4, 5, 6]))
                .await
                .is_err()
        );
        assert!(strip.is_raw());
    }

    #[test]
    fn blanking_the_device_ends_raw_mode() {
        let strip = StripCanvas::new(FullStrip::ImageSlot(3));
        strip.raw.store(true, Ordering::SeqCst);
        assert!(strip.blank().is_none());
        assert!(!strip.is_raw());

        let strip = StripCanvas::new(FullStrip::Bgpic);
        strip.raw.store(true, Ordering::SeqCst);
        assert!(strip.blank().is_some());
        assert!(!strip.is_raw());
    }

    #[test]
    fn full_strip_preserves_the_whole_dial_image() {
        let background = Rgb([0, 0, 255]);
        let edge = Rgb([255, 0, 0]);
        let image = RgbImage::from_fn(200, 100, |x, y| {
            if x == 0 || x == 199 || y == 0 || y == 99 {
                edge
            } else {
                background
            }
        });
        let strip = StripCanvas::new(FullStrip::ImageSlot(3));
        let frame = strip
            .update(1, Some(DynamicImage::ImageRgb8(image.clone())))
            .unwrap()
            .into_rgb8();

        let rendered = frame.view(200, 14, 200, 100).to_image();
        assert_eq!(rendered, image);
        for (x, y) in [(200, 13), (399, 114), (199, 64), (400, 64)] {
            assert_eq!(frame.get_pixel(x, y), &Rgb([0, 0, 0]));
        }
    }
}
