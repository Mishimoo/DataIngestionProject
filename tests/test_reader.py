from ingestion.Reader import read_data

def test_all_data_loaded(data_path):
    assert len(read_data(data_path)) == 10404

if __name__ == "__main__":
    data_path: str = "data/Raw Data/Mental_Health_DB.csv"
    test_all_data_loaded()
