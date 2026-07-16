from lakehouse_01_bronze import load_bronze_data
from lakehouse_02_silver import load_silver_data
from lakehouse_03_gold import load_gold_data

if __name__ == "__main__":
    load_bronze_data()
    load_silver_data()
    load_gold_data()