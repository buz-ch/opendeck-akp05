use std::sync::Arc;

use image::RgbImage;
use mirajazz::{device::Device, error::MirajazzError, state::DeviceStateReader};
use openaction::global_events::SetImageEvent;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::device::{StripCanvas, StripSink, handle_set_image};

#[derive(Clone, Copy, PartialEq, Eq)]
enum PowerState {
    Awake,
    Sleeping,
}

/// Serializes power transitions with commands that could wake the device.
pub struct ManagedDevice<D = Device> {
    device: D,
    power: Mutex<PowerState>,
    cancel: CancellationToken,
    strip: Option<StripCanvas>,
}

impl<D> ManagedDevice<D> {
    pub fn new(device: D, cancel: CancellationToken, strip: Option<StripCanvas>) -> Self {
        Self {
            device,
            power: Mutex::new(PowerState::Awake),
            cancel,
            strip,
        }
    }

    pub async fn with_awake_device(
        &self,
        operation: impl for<'a> AsyncFnOnce(&'a D) -> Result<(), MirajazzError>,
    ) -> Result<(), MirajazzError> {
        self.run_if_awake(operation).await.map(|_| ())
    }

    /// Like `with_awake_device`, but reports whether the operation ran:
    /// `Ok(false)` means the device was asleep or cancelled.
    pub async fn run_if_awake(
        &self,
        operation: impl for<'a> AsyncFnOnce(&'a D) -> Result<(), MirajazzError>,
    ) -> Result<bool, MirajazzError> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Ok(false),
            result = async {
                let power = self.power.lock().await;
                if *power == PowerState::Awake {
                    operation(&self.device).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            } => result,
        }
    }

    async fn set_power(
        &self,
        power: PowerState,
        operation: impl for<'a> AsyncFnOnce(&'a D) -> Result<(), MirajazzError>,
    ) -> Result<(), MirajazzError> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Ok(()),
            result = async {
                let mut current = self.power.lock().await;
                operation(&self.device).await?;
                *current = power;
                Ok(())
            } => result,
        }
    }

    /// Reports an operation error, cancelling this device on fatal errors.
    pub fn handle_error(&self, id: &str, err: MirajazzError) -> bool {
        log::error!("Device {id} error: {err}");
        if matches!(err, MirajazzError::ImageError(_) | MirajazzError::BadData) {
            return true;
        }
        self.cancel.cancel();
        false
    }
}

/// Why a raw strip operation did not complete.
#[derive(Debug)]
pub enum RawError {
    /// This device has no full-width strip transport.
    NoStrip,
    /// The device is asleep or shutting down.
    NotAwake,
    Write(MirajazzError),
}

impl<D: StripSink> ManagedDevice<D> {
    /// Shows a raw frame. `stale` is checked once the device lock is held; a
    /// stale frame is dropped silently because a newer one is waiting.
    pub async fn raw_frame(
        &self,
        image: RgbImage,
        stale: &(dyn Fn() -> bool + Sync),
    ) -> Result<(), RawError> {
        let strip = self.strip.as_ref().ok_or(RawError::NoStrip)?;
        let ran = self
            .run_if_awake(async |device| {
                if stale() {
                    return Ok(());
                }
                strip.set_raw(device, image).await
            })
            .await
            .map_err(RawError::Write)?;
        ran.then_some(()).ok_or(RawError::NotAwake)
    }

    /// Leaves raw mode, returning the strip to the composed view.
    pub async fn raw_clear(&self) -> Result<(), RawError> {
        let strip = self.strip.as_ref().ok_or(RawError::NoStrip)?;
        let ran = self
            .run_if_awake(async |device| strip.clear_raw(device).await)
            .await
            .map_err(RawError::Write)?;
        ran.then_some(()).ok_or(RawError::NotAwake)
    }
}

impl ManagedDevice {
    pub async fn set_image(&self, event: SetImageEvent) -> Result<(), MirajazzError> {
        self.with_awake_device(async |device| {
            handle_set_image(device, event, self.strip.as_ref()).await
        })
        .await
    }

    pub async fn set_brightness(&self, brightness: u8) -> Result<(), MirajazzError> {
        // LIG 0 leaves these panels lit. HAN sleeps them; nonzero LIG wakes them.
        if brightness == 0 {
            self.set_power(PowerState::Sleeping, async |device| device.sleep().await)
                .await
        } else {
            self.set_power(PowerState::Awake, async |device| {
                device.set_brightness(brightness).await
            })
            .await
        }
    }

    pub fn get_reader(&self) -> Arc<DeviceStateReader> {
        self.device.get_reader(crate::inputs::process_input)
    }

    pub async fn shutdown(&self) -> Result<(), MirajazzError> {
        let _power = self.power.lock().await;
        self.device.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::test_sink::RecordingSink;
    use crate::mappings::FullStrip;
    use futures_lite::future::poll_once;
    use std::pin::pin;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn cancellation_releases_inflight_and_queued_commands() {
        let cancel = CancellationToken::new();
        let device = ManagedDevice::new((), cancel.clone(), None);
        let mut inflight = pin!(device.with_awake_device(async |_| {
            std::future::pending::<Result<(), MirajazzError>>().await
        }));
        assert!(poll_once(inflight.as_mut()).await.is_none());
        let mut queued = pin!(device.with_awake_device(async |_| {
            panic!("queued I/O must not start after cancellation");
        }));
        assert!(poll_once(queued.as_mut()).await.is_none());

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            inflight.await.unwrap();
            queued.await.unwrap();
        })
        .await
        .expect("dead HID I/O must not block event dispatch or shutdown");
        assert!(device.power.try_lock().is_ok());
    }

    #[tokio::test]
    async fn sleep_waits_for_inflight_command_and_blocks_queued_commands() {
        let device = ManagedDevice::new(Mutex::new(Vec::new()), CancellationToken::new(), None);
        let (finish, finished) = oneshot::channel();
        let mut command = pin!(device.with_awake_device(async |events| {
            events.lock().await.push("keepalive started");
            finished.await.unwrap();
            events.lock().await.push("keepalive finished");
            Ok(())
        }));
        assert!(poll_once(command.as_mut()).await.is_none());

        let mut sleep = pin!(device.set_power(PowerState::Sleeping, async |events| {
            events.lock().await.push("sleep");
            Ok(())
        }));
        assert!(poll_once(sleep.as_mut()).await.is_none());
        let mut queued = pin!(device.with_awake_device(async |events| {
            events.lock().await.push("unexpected command");
            Ok(())
        }));
        assert!(poll_once(queued.as_mut()).await.is_none());

        finish.send(()).unwrap();
        command.await.unwrap();
        sleep.await.unwrap();
        queued.await.unwrap();
        assert_eq!(
            *device.device.lock().await,
            ["keepalive started", "keepalive finished", "sleep"]
        );
    }

    fn raw_device(sleeping: bool) -> ManagedDevice<RecordingSink> {
        let device = ManagedDevice::new(
            RecordingSink::default(),
            CancellationToken::new(),
            Some(StripCanvas::new(FullStrip::ImageSlot(3))),
        );
        if sleeping {
            *device.power.try_lock().unwrap() = PowerState::Sleeping;
        }
        device
    }

    fn frame() -> RgbImage {
        RgbImage::from_pixel(800, 128, image::Rgb([1, 2, 3]))
    }

    #[tokio::test]
    async fn raw_frame_is_written_and_clear_restores_the_composed_view() {
        let device = raw_device(false);
        device.raw_frame(frame(), &|| false).await.unwrap();
        assert_eq!(device.device.count(), 1);
        assert_eq!(device.device.last(), frame());

        device.raw_clear().await.unwrap();
        assert_eq!(device.device.count(), 2);
        device.raw_clear().await.unwrap();
        assert_eq!(device.device.count(), 2, "second clear changes nothing");
    }

    #[tokio::test]
    async fn stale_raw_frames_are_dropped_without_a_write() {
        let device = raw_device(false);
        device.raw_frame(frame(), &|| true).await.unwrap();
        assert_eq!(device.device.count(), 0);
        assert!(!device.strip.as_ref().unwrap().is_raw());
    }

    #[tokio::test]
    async fn raw_frames_are_refused_while_asleep() {
        let device = raw_device(true);
        assert!(matches!(
            device.raw_frame(frame(), &|| false).await,
            Err(RawError::NotAwake)
        ));
        assert!(matches!(device.raw_clear().await, Err(RawError::NotAwake)));
        assert_eq!(device.device.count(), 0);
    }

    #[tokio::test]
    async fn raw_frames_need_a_full_strip() {
        let device = ManagedDevice::new(RecordingSink::default(), CancellationToken::new(), None);
        assert!(matches!(
            device.raw_frame(frame(), &|| false).await,
            Err(RawError::NoStrip)
        ));
    }

    #[tokio::test]
    async fn raw_write_errors_are_reported_and_keep_the_device_usable() {
        let device = raw_device(false);
        device
            .device
            .fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            device.raw_frame(frame(), &|| false).await,
            Err(RawError::Write(_))
        ));
        device
            .device
            .fail
            .store(false, std::sync::atomic::Ordering::SeqCst);
        device.raw_frame(frame(), &|| false).await.unwrap();
        assert_eq!(device.device.count(), 1);
    }
}
