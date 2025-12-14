import dlt
from pyspark.sql import functions as sf

@dlt.table
def clinical_notes_mv():
    """
    Strips the patient name and the patient ID out of the note ID field and creates two new fields for them.
    """
    return (
    spark.read.table("clinical_notes_st")
    .withColumnRenamed("value", "note_text")
    .withColumn("patient_name", sf.regexp_replace(sf.regexp_substr('note_id', sf.lit(r'^([A-Za-záéíóúÁÉÍÓÚ]*_){1,4}')), '_', ' '))
    .withColumn("patient_id", sf.regexp_substr('note_id', sf.lit(r'[a-z0-9]*-[a-z0-9]*-[a-z0-9]*-[a-z0-9]*-[a-z0-9]*')))
)