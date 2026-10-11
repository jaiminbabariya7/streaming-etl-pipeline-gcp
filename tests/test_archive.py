"""Streaming test for the GCS archive sink (dataflow.pipeline.archive_to_files)."""
import glob
import json
import os
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows, TimestampedValue

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataflow.pipeline import archive_to_files  # noqa: E402


def test_archive_writes_jsonl_per_window_on_unbounded_input(tmp_path):
    events = [{"event_id": f"e{i}", "event_type": "page_view"} for i in range(4)]
    stream = (TestStream()
              .advance_watermark_to(0)
              .add_elements([TimestampedValue(e, 10) for e in events[:2]])     # window [0, 60)
              .add_elements([TimestampedValue(e, 70) for e in events[2:]])     # window [60, 120)
              .advance_watermark_to_infinity())

    opts = PipelineOptions()
    opts.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=opts) as p:
        windowed = p | stream | beam.WindowInto(FixedWindows(60))
        archive_to_files(windowed, str(tmp_path))

    files = sorted(glob.glob(str(tmp_path / "events*.jsonl")))
    assert len(files) == 2                                   # one file per 60s window
    lines = [json.loads(line) for f in files for line in open(f) if line.strip()]
    assert sorted(r["event_id"] for r in lines) == ["e0", "e1", "e2", "e3"]
