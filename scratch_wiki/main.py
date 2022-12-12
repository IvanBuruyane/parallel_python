from time import time
from yaml_reader import YamlPipelineReader


def main():

    yaml_pipline_executor = YamlPipelineReader("pipelines/wiki_yahoo_scraper_pipeline.yaml")
    scraper_start_time = time()
    yaml_pipline_executor.start()
    yaml_pipline_executor.join()
    print(f"Extracting time: {round(time() - scraper_start_time, 1)}")


if __name__ == "__main__":
    main()
