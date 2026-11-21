# Raw strip mode (`raw-image` branch)

The AKP05 plugin normally builds the touch strip from four 200x100 dial images, because OpenDeck renders every dial image on a fixed 200x100 canvas. AKP05 centres each image in its quarter of the 800x128 strip, which leaves 14-pixel black bands above and below. A caller can therefore never use the strip's full height, and OpenDeck cannot deliver a full-width picture at all.

This branch adds an opt-in raw strip mode that goes around OpenDeck entirely. With `strip_raw = true` in the plugin's `config.toml`, each connected deck with a full-width strip transport gets a Unix socket under `$XDG_RUNTIME_DIR/opendeck-akp05/`. The transport is either the firmware-gated image slot or BGPIC. A client sends a whole 800x128 PNG or JPEG as one length-prefixed message and gets a one-byte reply saying whether it was shown or why it was rejected. The plugin shows the frame 1:1 across the strip, with no composition, no letterboxing and no scaling. Frames of any other size, unsupported colour models and payloads over 512 KiB are rejected without touching the display. A `clear` message ends raw mode.

While raw mode is active, OpenDeck's dial updates keep arriving and are stored, but they do not change the strip. When raw mode ends, the stored dial images come back. Raw mode is not persisted: reconnecting the deck, or resuming from suspend, ends it, and the sender has to resend. Each frame is one strip write under the same device lock as every other command, so the strip never shows a half-updated frame. If frames arrive faster than the device accepts them, the newest wins.

The change is small and mostly self-contained. `StripCanvas` gained a raw flag and a sink trait for writing the strip. A new `raw_socket.rs` module holds the protocol and server. `device_task` starts the socket alongside the existing tasks, so reconnects and shutdown clean it up automatically. The unit and socket tests need no device. The checks that need a real deck are listed in the spec (`akp05-raw-strip-spec.md`, section 12).

## Notes

- The socket is Unix-only. On other platforms `strip_raw` logs a warning and does nothing.
- If the strip's outermost pixel row looks cut off, look straight at it first. The cover glass sits above the panel, so from an angle (parallax) the edge row can appear missing even though it is displayed.
