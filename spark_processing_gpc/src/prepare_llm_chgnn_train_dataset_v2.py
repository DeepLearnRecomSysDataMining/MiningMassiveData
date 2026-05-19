import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger("prepare_llm_chgnn_train_dataset")


def run_prepare_llm_chgnn_train_dataset( spark, interactions_path, item_nodes_path, output_path, negatives_per_query=20 ):
    """
    Tạo dataset train cho LLM-CHGNN:

    query Amazon item
    +
    positive VN item
    +
    negative VN candidates
    +
    text/spec/category

    Output:
        parquet train dataset
    """

    logger.info("Loading interactions...")
    df_inter = (spark.read.parquet(interactions_path)
                        .select( "asin", "product_id", )
                        .dropna()
                        .dropDuplicates())

    logger.info("Loading item_nodes...")

    df_items = (spark.read.parquet(item_nodes_path)
                        .select( "product_id", "asin", "full_text", "parsed_specs", "category", "domain", )
                        .dropna(subset=["full_text"]))

    # =========================
    # AMAZON QUERY ITEMS
    # =========================

    df_amz = ( df_items.filter(F.col("domain") == "amazon")
                        .select(
                            F.col("asin").alias("query_asin"),
                            F.col("full_text").alias("query_text"),
                            F.col("parsed_specs").alias("query_specs"),
                            F.col("category").alias("query_category")
                        )
              )
    
    # =========================
    # VN POSITIVE ITEMS
    # =========================

    df_vn = (
        df_items
        .filter(F.col("domain") == "vn")
        .select(
            F.col("product_id").alias("vn_product_id"),
            F.col("full_text").alias("candidate_text"),
            F.col("parsed_specs").alias("candidate_specs"),
            F.col("category").alias("candidate_category"),
        )
    )

    # =========================
    # POSITIVE PAIRS
    # =========================

    logger.info("Building positive pairs...")

    df_pos = ( df_inter.join( df_amz, df_inter["asin"] == df_amz["query_asin"], "inner" )
                .join( df_vn, df_inter["product_id"] == df_vn["vn_product_id"], "inner" )
                .select( "query_asin", "query_text", "query_specs", "query_category",
                            F.col("vn_product_id").alias("positive_product_id"),
                            F.col("candidate_text").alias("positive_text"),
                            F.col("candidate_specs").alias("positive_specs"),
                            F.col("candidate_category").alias("positive_category")
                        )
            )

    # =========================
    # NEGATIVE POOL
    # =========================

    logger.info("Preparing negative pool...")

    df_vn_neg = df_vn.repartition(64)

    # =========================
    # RANDOM NEGATIVE SAMPLING
    # =========================

    logger.info("Generating negatives...")

    df_neg = df_pos.withColumn( "rand_seed", F.rand()).join( df_vn_neg, ( df_vn_neg["vn_product_id"] != df_pos["positive_product_id"] ), "inner" )

    # RANDOM TOP-K NEGATIVES
    window_neg = Window.partitionBy( "query_asin", "positive_product_id" ).orderBy(F.rand())

    df_neg = df_neg.withColumn( "neg_rank", F.row_number().over(window_neg) ).filter(F.col("neg_rank") <= negatives_per_query)

    # =========================
    # GROUP NEGATIVES
    # =========================

    logger.info("Aggregating negatives...")

    df_final = (
        df_neg
        .groupBy( "query_asin", "query_text", "query_specs", "query_category", "positive_product_id", "positive_text", "positive_specs", "positive_category", )
        .agg(
            F.collect_list("vn_product_id").alias("negative_ids"),
            F.collect_list("candidate_text").alias("negative_texts"),
            F.collect_list("candidate_specs").alias("negative_specs"),
            F.collect_list("candidate_category").alias("negative_categories"),
        )
    )

    # =========================
    # FINAL FORMAT
    # =========================

    logger.info("Formatting final dataset...")

    df_final = (
        df_final
        .withColumn( "candidate_ids", F.concat( F.array(F.col("positive_product_id")), F.col("negative_ids")))
        .withColumn( "candidate_texts", F.concat( F.array(F.col("positive_text")), F.col("negative_texts") ))
        .withColumn( "candidate_specs", F.concat( F.array(F.col("positive_specs")), F.col("negative_specs")))
        .withColumn( "candidate_categories", F.concat( F.array(F.col("positive_category")), F.col("negative_categories")))
        .withColumn("true_vn_id", F.col("positive_product_id"))
        .select(
            "query_asin", "query_text", "query_specs", "query_category",
            "candidate_ids", "candidate_texts", "candidate_specs", "candidate_categories",
            "true_vn_id",
        )
    )

    logger.info("Writing parquet output...")

    df_final.coalesce(32).write.mode("overwrite").parquet(output_path)
    final_count = spark.read.parquet(output_path).count()

    logger.info(f"LLM-CHGNN train dataset done: {final_count:,}")

    return final_count