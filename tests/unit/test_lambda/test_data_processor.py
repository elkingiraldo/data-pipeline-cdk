from lambdas.data_extractor.data_processor import DataProcessor

def test_process_and_metadata():
    p = DataProcessor()
    raw = [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Luis"}]
    out = p.process(raw)
    assert len(out) == 2
    assert "processed_at" in out[0]

    md = p.add_metadata(out, {"source": "test"})
    assert md["record_count"] == 2
    assert md["source"] == "test"
