# Examples

## Zero-download demo

The real datasets are multi-GB and not included in this repo. This example
fabricates one small synthetic IQ capture and runs the normal ingest pipeline
so you can see the viewer work without downloading anything.

```bash
pip install -r requirements.txt
python examples/make_sample.py     # writes examples/sample_data/ + iq.duckdb
python serve.py                    # open http://127.0.0.1:8090
```

In the viewer, open the **Source** dropdown in the header and choose
**"IQ capture: demo_signal"**. You'll see a spectrogram with three obvious
features:

- a steady horizontal line: a continuous carrier at +400 kHz,
- stepping horizontal segments: a tone hopping across the band every 50 ms,
- evenly spaced vertical bands: periodic wideband bursts (10 ms on / 40 ms off).

Scroll to zoom in time, `Ctrl`+scroll to zoom in frequency, and drag to pan.
The colour scale stays locked as you zoom.

![Synthetic demo spectrogram](../docs/demo_spectrogram.png)

`make_sample.py` generates only numpy noise and tones, so it represents no real
device or signal. To work with real captures instead, point `ingest/iq_ingest.py`
at a folder of SigMF / TDMS files (see [../docs/IQ_DATASETS.md](../docs/IQ_DATASETS.md)).
