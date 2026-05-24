import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.window import Window

from src.file_utils import detect_jsonl_type, list_files

logger = logging.getLogger("prepare_llm_chgnn_specs_v2")


VN_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("specifications", ArrayType(StringType()), True),
    StructField("description", StringType(), True),
    StructField("breadcrumb", StringType(), True),
])

AMZ_ITEM_SCHEMA = StructType([
    StructField("parent_asin", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("title", StringType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("main_category", StringType(), True),
    StructField("details", MapType(StringType(), StringType()), True),
])


def _standardize(c):
    c = F.coalesce(F.concat_ws(" ", c), c.cast("string"), F.lit(""))
    c = F.regexp_replace(c, r"\s+", " ")
    return F.lower(F.trim(c))


def _array_to_map(prefix: str, arr_col):
    return F.when(
        arr_col.isNotNull() & (F.size(arr_col) > 0),
        F.map_from_arrays(
            F.transform(
                F.sequence(F.lit(1), F.size(arr_col)),
                lambda x: F.concat(F.lit(prefix), x.cast("string"))
            ),
            F.transform(arr_col, lambda x: F.lower(F.trim(x.cast("string"))))
        )
    ).otherwise(F.create_map())


def _base_specs_map(domain_col, category_col):
    return F.map_from_arrays(
        F.array(F.lit("domain"), F.lit("category")),
        F.array(
            F.coalesce(domain_col, F.lit("unknown")),
            F.coalesce(category_col, F.lit("unknown"))
        )
    )


def run_prepare_llm_chgnn_specs(spark, raw_data_dir: str, output_path: str, file_groups: dict = None):
    logger.info("Building LLM-CHGNN item specs from raw metadata...")

    if file_groups:
        vn_files = file_groups.get("vn_item", [])
        amz_files = file_groups.get("amz_item", [])
    else:
        all_files = list_files(raw_data_dir)
        vn_files = [
            f for f in all_files
            if f.endswith(".jsonl") and detect_jsonl_type(f) == "vn_item"
        ]
        amz_files = [
            f for f in all_files
            if f.endswith(".jsonl") and detect_jsonl_type(f) == "amz_item"
        ]

    df_final = None

    if vn_files:
        logger.info(f"Reading VN metadata files: {len(vn_files)}")
        df_vn = spark.read.option("mode", "PERMISSIVE").schema(VN_ITEM_SCHEMA).json(vn_files)

        df_vn_specs = (
            df_vn
            .withColumn("product_id_std", _standardize(F.col("product_id")))
            .withColumn("asin_std", _standardize(F.coalesce(F.col("asin"), F.col("product_id"))))
            .withColumn("domain", F.lit("vn"))
            .withColumn("category", _standardize(F.col("breadcrumb")))
            .withColumn("embedding_id", F.concat(F.lit("vn_"), F.col("product_id_std")))
            .withColumn(
                "parsed_specs",
                F.map_concat(
                    _base_specs_map(F.col("domain"), F.col("category")),
                    _array_to_map("spec_", F.col("specifications"))
                )
            )
            .select("embedding_id", "parsed_specs")
            .filter((F.col("embedding_id").isNotNull()) & (~F.col("embedding_id").isin("vn_", "")))
        )

        df_final = df_vn_specs

    if amz_files:
        logger.info(f"Reading Amazon metadata files: {len(amz_files)}")
        df_amz = spark.read.option("mode", "PERMISSIVE").schema(AMZ_ITEM_SCHEMA).json(amz_files)

        df_amz_specs = (
            df_amz
            .withColumn("details_asin", F.coalesce(F.col("details")["ASIN"], F.col("details")["asin"]))
            .withColumn("asin_std", _standardize(F.coalesce(F.col("asin"), F.col("parent_asin"), F.col("details_asin"))))
            .withColumn("domain", F.lit("amazon"))
            .withColumn("category", _standardize(F.col("main_category")))
            .withColumn("embedding_id", F.concat(F.lit("amz_"), F.col("asin_std")))
            .withColumn(
                "parsed_specs",
                F.map_concat(
                    _base_specs_map(F.col("domain"), F.col("category")),
                    F.coalesce(F.col("details"), F.create_map()),
                    _array_to_map("feature_", F.col("features"))
                )
            )
            .select("embedding_id", "parsed_specs")
            .filter((F.col("embedding_id").isNotNull()) & (~F.col("embedding_id").isin("amz_", "")))
        )

        df_final = df_amz_specs if df_final is None else df_final.unionByName(df_amz_specs)

    if df_final is None:
        logger.warning("No metadata files found for LLM-CHGNN specs.")
        return 0

    df_final = df_final.withColumn("spec_size", F.size(F.col("parsed_specs")))

    w = Window.partitionBy("embedding_id").orderBy(F.desc("spec_size"))

    df_final = (
        df_final
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "spec_size")
    )

    logger.info(f"Writing LLM-CHGNN item specs -> {output_path}")
    df_final.coalesce(16).write.mode("overwrite").parquet(output_path)

    final_count = spark.read.parquet(output_path).count()
    logger.info(f"LLM-CHGNN item specs done: {final_count:,}")

    return final_count