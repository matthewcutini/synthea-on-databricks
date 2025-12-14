import dlt
from pyspark.sql.functions import col

@dlt.table
def clinical_notes_st():
    """
    Reads the raw clinical notes TXT data as a streaming source.
    """
    path = "/Volumes/mcutini/nebraska_health_demo/synthea/output/notes/"

    schema = """
        value STRING,
        note_id STRING
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholeText", True)
            .load(path)
            .withColumn("note_id", col("_metadata.file_name"))
            )